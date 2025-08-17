from flask import Blueprint, render_template, session, redirect, url_for
from bson import ObjectId
from datetime import datetime
from db import clients_collection, orders_collection, payments_collection

client_profile_bp = Blueprint("client_profile", __name__, template_folder="templates")

def _f(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

def _parse_dt(v):
    """Handle Mongo extended JSON and naive datetimes."""
    if isinstance(v, datetime):
        return v
    if isinstance(v, (int, float)):
        # value already in ms/seconds? Heuristic: treat > 10^12 as ms
        if v > 10**12:  # ms
            return datetime.fromtimestamp(v / 1000.0)
        return datetime.fromtimestamp(v)
    if isinstance(v, dict) and "$date" in v:
        d = v["$date"]
        # could be {"$numberLong":"..."} or int ms
        if isinstance(d, dict) and "$numberLong" in d:
            try:
                return datetime.fromtimestamp(int(d["$numberLong"]) / 1000.0)
            except Exception:
                return None
        try:
            # sometimes "$date" can be millis directly
            return datetime.fromtimestamp(int(d) / 1000.0)
        except Exception:
            # or ISO string
            try:
                return datetime.fromisoformat(d.replace("Z", "+00:00"))
            except Exception:
                return None
    return None

@client_profile_bp.route('/client/<client_id>')
def client_profile(client_id):
    try:
        # ✅ Validate ObjectId
        if not ObjectId.is_valid(client_id):
            return "Invalid client ID", 400

        oid = ObjectId(client_id)

        # ✅ Fetch client
        client = clients_collection.find_one({"_id": oid})
        if not client:
            return "Client not found", 404

        # ✅ Fetch all orders for this client (support both ObjectId and string storage)
        orders = list(
            orders_collection.find({"client_id": {"$in": [oid, str(oid)]}})
                             .sort("date", -1)
        )

        # ---- Aggregate confirmed payments for ALL orders in one pass ----
        order_ids_obj = [o["_id"] for o in orders]
        order_ids_str = [str(x) for x in order_ids_obj]

        payments_match = {
            "status": {"$regex": "^confirmed$", "$options": "i"},
            "order_id": {"$in": order_ids_obj + order_ids_str},
            # If your payments store client_id, narrow by both forms:
            "client_id": {"$in": [oid, str(oid)]}
        }

        pipeline = [
            {"$match": payments_match},
            {"$group": {"_id": "$order_id", "total_paid": {"$sum": "$amount"}}}
        ]

        paid_map = {}
        for row in payments_collection.aggregate(pipeline):
            key = row["_id"]  # could be ObjectId or string
            total_paid = _f(row.get("total_paid"))
            paid_map[key] = total_paid
            try:
                paid_map[str(key)] = total_paid
            except Exception:
                pass

        # ✅ Decorate each order (dates, margin/returns fallback, amount_paid/left)
        for o in orders:
            # Dates
            o["date"] = _parse_dt(o.get("date"))
            o["due_date"] = _parse_dt(o.get("due_date"))

            # Margin & returns (use existing if present, else compute basic)
            if "margin" not in o or o.get("margin") is None:
                p = _f(o.get("p_bdc_omc"))
                s = _f(o.get("s_bdc_omc"))
                o["margin"] = round(s - p, 2)
            if "returns" not in o or o.get("returns") is None:
                o["returns"] = round(_f(o.get("margin")) * _f(o.get("quantity")), 2)

            # Tax & total_debt defaults
            o["tax"] = _f(o.get("tax"))
            o["total_debt"] = _f(o.get("total_debt"))

            # Payments: external (payments collection) + embedded (legacy)
            paid_external = _f(paid_map.get(o["_id"])) or _f(paid_map.get(str(o["_id"])))
            paid_embedded = sum(_f(p.get("amount")) for p in (o.get("payment_details") or []))
            o["amount_paid"] = round(paid_external + paid_embedded, 2)
            o["amount_left"] = round(o["total_debt"] - o["amount_paid"], 2)

        # ✅ Latest approved order (if any) and summary box values
        latest_approved = next((x for x in orders if (x.get("status") or "").lower() == "approved"), None)
        if latest_approved:
            total_paid = _f(latest_approved.get("amount_paid"))
            amount_left = max(_f(latest_approved.get("total_debt")) - total_paid, 0.0)
        else:
            total_paid = 0.0
            amount_left = 0.0

        return render_template(
            "partials/client_profile.html",
            client=client,
            orders=orders,
            latest_approved=latest_approved,
            total_paid=round(total_paid, 2),
            amount_left=round(amount_left, 2)
        )

    except Exception as e:
        # In production you might log e and show a friendlier page
        return f"An error occurred: {str(e)}", 500
