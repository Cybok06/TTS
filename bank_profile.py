from flask import Blueprint, render_template, request, jsonify
from db import db
from bson import ObjectId
from datetime import datetime
import calendar
import re

bank_profile_bp = Blueprint("bank_profile", __name__, template_folder="templates")

accounts_col = db["bank_accounts"]
payments_col = db["payments"]          # existing bank receipt/confirmations
orders_col   = db["orders"]            # to compute S-Tax due per order
tax_col      = db["tax_records"]       # S-Tax payments live here

# ---- shared helpers ----
def _f(v, default=0.0):
    try:
        if v is None or v == "": return default
        return float(v)
    except Exception:
        return default

def _fmt2(n):  # string with 2dp
    try: return f"{float(n):,.2f}"
    except Exception: return "0.00"

def _stax_per_l(order):
    for k in ("s_tax", "s-tax"):
        if k in order and order.get(k) is not None:
            try:
                return float(order.get(k))
            except Exception:
                pass
    return 0.0

def _order_due(order):
    return round(_stax_per_l(order) * _f(order.get("quantity"), 0.0), 2)

def _paid_sum_for_order(oid: ObjectId) -> float:
    try:
        pipe = [
            {"$match": {"order_oid": oid, "type": {"$regex": r"^s[\s_-]*tax$", "$options": "i"}}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(tax_col.aggregate(pipe), None)
        return float(row.get("total", 0.0)) if row else 0.0
    except Exception:
        return 0.0

# ---- page ----
@bank_profile_bp.route("/bank-profile/<bank_id>")
def bank_profile(bank_id):
    bank = accounts_col.find_one({"_id": ObjectId(bank_id)})
    if not bank: return "Bank not found", 404

    bank_name = bank.get("bank_name")
    last4 = (bank.get("account_number") or "")[-4:]

    start_str = request.args.get("start_date")
    end_str   = request.args.get("end_date")

    query = {"bank_name": bank_name, "account_last4": last4, "status": "confirmed"}
    if start_str and end_str:
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d")
            end_date   = datetime.strptime(end_str, "%Y-%m-%d")
            query["date"] = {"$gte": start_date, "$lte": end_date}
        except ValueError:
            pass

    payments = list(payments_col.find(query).sort("date", -1))
    total_received = sum(_f(p.get("amount")) for p in payments)

    # history of S-Tax payments sourced from this bank
    bank_tax_rows = []
    for r in tax_col.find(
        {"source_bank_id": ObjectId(bank_id), "type": {"$regex": r"^s[\s_-]*tax$", "$options": "i"}},
        {"amount":1, "payment_date":1, "reference":1, "paid_by":1, "omc":1, "order_id":1}
    ).sort("payment_date", -1):
        pd = r.get("payment_date")
        if isinstance(pd, datetime):
            pd_str = pd.strftime("%Y-%m-%d")
        else:
            pd_str = str(pd or "—")
        bank_tax_rows.append({
            "amount": _f(r.get("amount")),
            "payment_date_str": pd_str,
            "reference": r.get("reference"),
            "paid_by": r.get("paid_by"),
            "omc": r.get("omc"),
            "order_id": r.get("order_id"),
        })

    return render_template(
        "partials/bank_profile.html",
        bank=bank,
        payments=payments,
        total_received=total_received,
        start_date=start_str,
        end_date=end_str,
        bank_tax_rows=bank_tax_rows
    )

# ---- API: OMC debts for this tenant (global across orders) ----
@bank_profile_bp.route("/bank-profile/<bank_id>/omc-debts", methods=["GET"])
def omc_debts(bank_id):
    try:
        # Find S-Tax eligible orders and compute remaining per OMC
        eligible = list(orders_col.find({
            "$or": [
                {"order_type": "s_tax"},
                {"order_type": "combo"},
                {"s_tax": {"$gt": 0}},
                {"s-tax": {"$gt": 0}},
            ]
        }, {"_id":1, "omc":1, "quantity":1, "s_tax":1, "s-tax":1, "date":1}))

        omc_map = {}
        for o in eligible:
            due = _order_due(o)
            paid = _paid_sum_for_order(o["_id"])
            rem  = max(0.0, round(due - paid, 2))
            if rem <= 0: continue
            omc = o.get("omc") or "—"
            slot = omc_map.setdefault(omc, {"outstanding":0.0, "unpaid_orders":0})
            slot["outstanding"] += rem
            slot["unpaid_orders"] += 1

        debts = [{"omc": k, "outstanding": round(v["outstanding"],2), "unpaid_orders": v["unpaid_orders"]}
                 for k,v in omc_map.items()]
        # sort by outstanding desc
        debts.sort(key=lambda x: x["outstanding"], reverse=True)
        return jsonify({"status":"success", "debts": debts})
    except Exception as e:
        return jsonify({"status":"error", "message": str(e)}), 500

# ---- API: apply bank payment to an OMC (allocates oldest-first across orders) ----
@bank_profile_bp.route("/bank-profile/pay-omc", methods=["POST"])
def pay_omc_from_bank():
    try:
        data = request.get_json(force=True)
        bank_id = data.get("bank_id")
        omc     = (data.get("omc") or "").strip()
        amount  = _f(data.get("amount"))
        ref     = (data.get("reference") or "").strip()
        paid_by = (data.get("paid_by") or "").strip()
        date_s  = (data.get("payment_date") or "").strip()

        if not bank_id or not ObjectId.is_valid(bank_id):
            return jsonify({"status":"error", "message":"Invalid bank id"}), 400
        if not omc:
            return jsonify({"status":"error", "message":"OMC is required"}), 400
        if amount <= 0:
            return jsonify({"status":"error", "message":"Amount must be greater than 0"}), 400

        pay_dt = datetime.utcnow()
        if date_s:
            try: pay_dt = datetime.strptime(date_s, "%Y-%m-%d")
            except ValueError: return jsonify({"status":"error", "message":"Invalid payment date"}), 400

        # Gather unpaid orders for this OMC, oldest first
        orders = list(orders_col.find({
            "omc": omc,
            "$or": [
                {"order_type": "s_tax"},
                {"order_type": "combo"},
                {"s_tax": {"$gt": 0}},
                {"s-tax": {"$gt": 0}},
            ]
        }, {"_id":1, "order_id":1, "quantity":1, "s_tax":1, "s-tax":1, "date":1}).sort("date", 1))

        # Compute remaining per order; keep only those with outstanding
        alloc_list = []
        total_outstanding = 0.0
        for o in orders:
            due = _order_due(o)
            paid = _paid_sum_for_order(o["_id"])
            rem  = max(0.0, round(due - paid, 2))
            if rem > 0:
                alloc_list.append({"order": o, "remaining": rem})
                total_outstanding += rem

        if total_outstanding <= 0:
            return jsonify({"status":"error", "message":"No outstanding S-Tax for this OMC"}), 400
        if amount > total_outstanding:
            return jsonify({"status":"error", "message": f"Amount exceeds OMC outstanding (GHS {_fmt2(total_outstanding)})"}), 400

        # Allocate oldest-first
        left = amount
        created = []
        for a in alloc_list:
            if left <= 0: break
            portion = min(left, a["remaining"])
            o = a["order"]
            tax_col.insert_one({
                "type": "S-Tax",
                "amount": round(portion, 2),
                "payment_date": pay_dt,
                "reference": ref or None,
                "paid_by": paid_by or None,
                "omc": omc,
                "order_id": o.get("order_id"),
                "order_oid": o["_id"],
                "source_bank_id": ObjectId(bank_id),   # <— tag for bank view
                "submitted_at": datetime.utcnow()
            })
            # recompute post-insert
            new_paid = _paid_sum_for_order(o["_id"])
            due      = _order_due(o)
            remaining= max(0.0, round(due - new_paid, 2))
            update_doc = {
                "s_tax_paid_amount": round(new_paid, 2),
                "s_tax_paid_at": pay_dt,
                "s_tax_reference": ref or o.get("s_tax_reference"),
                "s_tax_paid_by": paid_by or o.get("s_tax_paid_by"),
            }
            if remaining <= 0:
                update_doc.update({"s_tax_payment":"paid", "s-tax-payment":"paid"})
            else:
                update_doc.update({"s_tax_payment":"partial", "s-tax-payment":"partial"})
            orders_col.update_one({"_id": o["_id"]}, {"$set": update_doc})

            created.append({"order_id": o.get("order_id"), "applied": round(portion,2), "remaining_after": remaining})
            left = round(left - portion, 2)

        return jsonify({"status":"success", "allocated": created, "omc": omc, "amount": round(amount,2)})
    except Exception as e:
        return jsonify({"status":"error", "message": str(e)}), 500
