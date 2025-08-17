from flask import Blueprint, render_template, request, jsonify, session
from db import db
from datetime import datetime
from bson import ObjectId

# 📦 Collections
bdc_col = db["bdc"]
bdc_txn_col = db["bdc_transactions"]

# 🔹 Blueprint Declaration
bdc_bp = Blueprint('bdc', __name__)

# ---------- Helpers ----------
def _to_f(x):
    try:
        if isinstance(x, str):
            x = x.replace("GHS", "").replace(",", "").strip()
        return float(x)
    except Exception:
        return 0.0

def _compute_current_balance(bdc_id: ObjectId):
    """
    Balance = SUM(bdc_transactions.amount where type == 'deposit')
              - (SUM 'from account' + SUM 'credit' in bdc.payment_details)
    Returns a dict with component totals.
    """
    oid = ObjectId(bdc_id)

    # 1) Total deposits from transactions (type == 'deposit')
    deposits_total = 0.0
    for r in bdc_txn_col.aggregate([
        {"$match": {"bdc_id": oid, "type": "deposit"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]):
        deposits_total = _to_f(r.get("total"))

    # 2) Totals from BDC payment_details (manual/legacy)
    bdc_doc = bdc_col.find_one({"_id": oid}, {"payment_details": 1}) or {}
    from_account_total = 0.0
    credit_total = 0.0

    for p in bdc_doc.get("payment_details", []):
        ptype = (p.get("payment_type") or "").strip().lower()
        amt = _to_f(p.get("amount"))
        if ptype == "from account":
            from_account_total += amt
        elif ptype == "credit":
            credit_total += amt
        # 'cash' is ignored

    balance = round(deposits_total - (from_account_total + credit_total), 2)

    return {
        "deposits_total": round(deposits_total, 2),
        "from_account_total": round(from_account_total, 2),
        "credit_total": round(credit_total, 2),
        "balance": balance
    }

# 📄 View All BDCs
@bdc_bp.route('/bdc')
def bdc_list():
    bdcs = list(bdc_col.find().sort("name", 1))
    return render_template("partials/bdc.html", bdcs=bdcs)

# ➕ Add New BDC (no stored balance; we compute on read)
@bdc_bp.route('/bdc/add', methods=['POST'])
def add_bdc():
    data = request.json
    name = (data.get('name', '') or '').strip()
    phone = (data.get('phone', '') or '').strip()
    location = (data.get('location', '') or '').strip()
    rep_name = (data.get('rep_name', '') or '').strip()
    rep_phone = (data.get('rep_phone', '') or '').strip()

    if not all([name, phone, location, rep_name, rep_phone]):
        return jsonify({"status": "error", "message": "All fields are required."}), 400

    if bdc_col.find_one({"name": name}):
        return jsonify({"status": "error", "message": "BDC already exists"}), 400

    bdc_col.insert_one({
        "name": name,
        "phone": phone,
        "location": location,
        "rep_name": rep_name,
        "rep_phone": rep_phone,
        # 🔴 no 'balance' field; not used anymore
        "payment_details": [],
        "date_created": datetime.utcnow()
    })

    return jsonify({"status": "success"})

# 💰 Manual Deposit
@bdc_bp.route('/bdc/txn/<bdc_id>', methods=['POST'])
def add_transaction(bdc_id):
    try:
        data = request.json
        amount = _to_f(data.get('amount'))
        note = (data.get('note') or '').strip()
        txn_type = (data.get('type') or '').strip().lower()

        if amount <= 0 or txn_type != 'add':
            return jsonify({"status": "error", "message": "Invalid transaction type or amount."}), 400

        # Ensure BDC exists
        if not bdc_col.find_one({"_id": ObjectId(bdc_id)}):
            return jsonify({"status": "error", "message": "BDC not found"}), 404

        # Save transaction AS your schema: type='deposit'
        bdc_txn_col.insert_one({
            "bdc_id": ObjectId(bdc_id),
            "amount": amount,
            "type": "deposit",
            "note": note,
            "timestamp": datetime.utcnow()
        })

        # Return freshly computed balance (not stored)
        comp = _compute_current_balance(ObjectId(bdc_id))
        return jsonify({"status": "success", "new_balance": comp["balance"]})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 🧾 Record BDC Payment (cash / from account / credit) — no balance math/storage here
@bdc_bp.route('/bdc/payment/<bdc_id>', methods=['POST'])
def record_bdc_payment(bdc_id):
    try:
        data = request.json
        payment_type = (data.get("payment_type") or "").strip().lower()
        amount = _to_f(data.get("amount"))
        client_name = (data.get("client_name") or "").strip()

        # ➕ Optional Order/Delivery details
        product = (data.get("product") or "").strip()
        vehicle_number = (data.get("vehicle_number") or "").strip()
        driver_name = (data.get("driver_name") or "").strip()
        driver_phone = (data.get("driver_phone") or "").strip()
        quantity = data.get("quantity", "")
        region = (data.get("region") or "").strip()

        if payment_type not in ["cash", "from account", "credit"] or amount <= 0:
            return jsonify({"status": "error", "message": "Invalid payment type or amount"}), 400

        if not bdc_col.find_one({"_id": ObjectId(bdc_id)}):
            return jsonify({"status": "error", "message": "BDC not found"}), 404

        payment_entry = {
            "payment_type": payment_type,
            "amount": amount,
            "client_name": client_name or "—",
            "product": product,
            "vehicle_number": vehicle_number,
            "driver_name": driver_name,
            "driver_phone": driver_phone,
            "quantity": quantity,
            "region": region,
            "delivery_status": "pending",
            "date": datetime.utcnow()
        }

        bdc_col.update_one({"_id": ObjectId(bdc_id)}, {"$push": {"payment_details": payment_entry}})

        # Return computed balance (not stored)
        comp = _compute_current_balance(ObjectId(bdc_id))
        return jsonify({"status": "success", "new_balance": comp["balance"]})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 👤 BDC Profile Page (always uses computed balance)
@bdc_bp.route('/bdc/profile/<bdc_id>')
def bdc_profile(bdc_id):
    bdc = bdc_col.find_one({"_id": ObjectId(bdc_id)})
    if not bdc:
        return "BDC not found", 404

    role = session.get("role", "assistant")
    dashboard_url = "/admin/dashboard" if role == "admin" else "/assistant/dashboard"

    # Filter window for deposits table
    start = request.args.get("start")
    end = request.args.get("end")
    query = {"bdc_id": ObjectId(bdc_id)}
    try:
        if start:
            query["timestamp"] = {"$gte": datetime.strptime(start, "%Y-%m-%d")}
        if end:
            end_date = datetime.strptime(end, "%Y-%m-%d")
            query["timestamp"] = query.get("timestamp", {})
            query["timestamp"]["$lte"] = end_date
    except ValueError:
        pass

    transactions = list(bdc_txn_col.find(query).sort("timestamp", -1))

    payment_details = bdc.get("payment_details", [])
    payment_details.sort(key=lambda x: x.get("date", datetime.min), reverse=True)

    # Compute authoritative balance/components for the header
    comp = _compute_current_balance(ObjectId(bdc_id))

    return render_template(
        "partials/bdc_profile.html",
        bdc=bdc,
        transactions=transactions,
        payments=payment_details,
        credit_balance=comp["balance"],          # use this in the template
        deposits_total=comp["deposits_total"],
        from_account_total=comp["from_account_total"],
        credit_total=comp["credit_total"],
        dashboard_url=dashboard_url
    )

# ✅ Update Delivery Progress for a Specific Payment
@bdc_bp.route('/bdc/update_delivery/<bdc_id>', methods=['POST'])
def update_delivery_status(bdc_id):
    try:
        data = request.json
        index = data.get("index")
        status = (data.get("status") or "").strip()

        if index is None or not status:
            return jsonify({"status": "error", "message": "Missing index or status"}), 400

        bdc = bdc_col.find_one({"_id": ObjectId(bdc_id)})
        if not bdc:
            return jsonify({"status": "error", "message": "BDC not found"}), 404

        payments = bdc.get("payment_details", [])
        if index >= len(payments):
            return jsonify({"status": "error", "message": "Invalid payment index"}), 400

        # Update delivery status in the BDC document
        bdc_col.update_one(
            {"_id": ObjectId(bdc_id)},
            {"$set": {f"payment_details.{index}.delivery_status": status}}
        )

        # Mirror to order if an order_id exists
        entry = payments[index]
        order_id = entry.get("order_id")
        if order_id:
            db["orders"].update_one(
                {"_id": ObjectId(order_id)},
                {"$set": {"delivery_status": status}}
            )

        return jsonify({"status": "success"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
