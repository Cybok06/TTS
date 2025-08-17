from flask import Blueprint, render_template, session, redirect, url_for, flash
from bson import ObjectId
from db import db
from datetime import datetime

client_dashboard_bp = Blueprint('client_dashboard', __name__, template_folder='templates')

clients_collection = db.clients
orders_collection = db.orders
payments_collection = db.payments  # ✅ add this

def _f(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

def _sum_embedded_payment_details(order):
    """Support legacy embedded payments on the order document."""
    pds = (order.get("payment_details") or [])
    return sum(_f(p.get("amount")) for p in pds)

@client_dashboard_bp.route('/dashboard')
def dashboard():
    if 'client_id' not in session or 'client_name' not in session:
        flash("Please log in first", "warning")
        return redirect(url_for('login.login'))

    client_id = session['client_id']
    if not ObjectId.is_valid(client_id):
        flash("Invalid session. Please log in again.", "danger")
        return redirect(url_for('login.login'))

    oid = ObjectId(client_id)

    client = clients_collection.find_one({"_id": oid})
    if not client:
        flash("Client not found. Please contact support.", "danger")
        return redirect(url_for('login.login'))

    # ✅ Fetch all orders for this client (support both ObjectId and string storage)
    orders = list(
        orders_collection.find({"client_id": {"$in": [oid, client_id]}}).sort("date", -1)
    )

    # --- Build lookup of payments from payments collection (confirmed only) ---
    order_ids_obj = [o["_id"] for o in orders]
    order_ids_str = [str(x) for x in order_ids_obj]

    # Match both ObjectId and string forms of order_id; status case-insensitive "confirmed"
    payments_pipe = [
        {
            "$match": {
                "status": {"$regex": "^confirmed$", "$options": "i"},
                "order_id": {"$in": order_ids_obj + order_ids_str}
            }
        },
        {
            "$group": {
                "_id": "$order_id",          # NOTE: could be ObjectId or string
                "total_paid": {"$sum": "$amount"}
            }
        }
    ]

    paid_map = {}  # key by both str(id) and ObjectId for easy lookup
    for row in payments_collection.aggregate(payments_pipe):
        key = row["_id"]
        total_paid = _f(row.get("total_paid"))
        # Save under both forms so downstream lookup always hits
        paid_map[key] = total_paid
        try:
            paid_map[str(key)] = total_paid
        except Exception:
            pass

    # --- Compute per-order amounts & totals ---
    total_orders = len(orders)
    total_debt = 0.0
    total_paid = 0.0

    for o in orders:
        # Coerce some fields
        o["total_debt"] = _f(o.get("total_debt"))
        total_debt += o["total_debt"]

        # Payments from collection
        paid_external = _f(paid_map.get(o["_id"])) or _f(paid_map.get(str(o["_id"])))
        # Legacy embedded payments (if any)
        paid_embedded = _sum_embedded_payment_details(o)

        o["amount_paid"] = round(paid_external + paid_embedded, 2)
        o["amount_left"] = round(o["total_debt"] - o["amount_paid"], 2)
        total_paid += o["amount_paid"]

        # Normalize datetimes if they came in as raw mongo dicts
        for field in ("date", "due_date", "delivered_date"):
            v = o.get(field)
            if isinstance(v, dict) and "$date" in v:
                try:
                    ms = int(v["$date"].get("$numberLong", 0))
                    o[field] = datetime.fromtimestamp(ms / 1000.0)
                except Exception:
                    pass

    amount_left = round(total_debt - total_paid, 2)
    latest_order = orders[0] if orders else None

    return render_template(
        'client/client_dashboard.html',
        client=client,
        total_orders=total_orders,
        total_debt=round(total_debt, 2),
        total_paid=round(total_paid, 2),     # ✅ now from payments collection (+ legacy)
        amount_left=amount_left,
        latest_order=latest_order,
        recent_orders=orders[:5]             # each has .amount_paid filled
    )
