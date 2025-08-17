from flask import Blueprint, render_template, session, redirect, url_for
from bson import ObjectId
from datetime import datetime
from db import db

client_order_history_bp = Blueprint('client_order_history', __name__)
orders_col = db["orders"]
clients_col = db["clients"]
payments_col = db["payments"]

def _f(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

def _sum_embedded(order):
    """Legacy support: sum amounts embedded on order.payment_details."""
    pds = (order.get("payment_details") or [])
    return sum(_f(p.get("amount")) for p in pds)

@client_order_history_bp.route("/order_history")
def client_order_history():
    client_id = session.get("client_id")

    # ✅ Validate session and ObjectId
    if not client_id or not ObjectId.is_valid(client_id):
        return redirect(url_for("login.client_login"))

    oid = ObjectId(client_id)

    # ✅ Fetch client
    client = clients_col.find_one({"_id": oid})
    if not client:
        return redirect(url_for("login.client_login"))

    # ✅ Fetch orders (support client_id stored as ObjectId or string)
    orders = list(
        orders_col.find({"client_id": {"$in": [oid, client_id]}})
                  .sort("date", -1)
    )

    # ---- Aggregate confirmed payments across ALL these orders ----
    order_ids_obj = [o["_id"] for o in orders]
    order_ids_str = [str(x) for x in order_ids_obj]

    payments_match = {
        "status": {"$regex": "^confirmed$", "$options": "i"},
        "order_id": {"$in": order_ids_obj + order_ids_str},
        # If your payments also store client_id, this narrows further safely:
        "client_id": {"$in": [oid, client_id]}
    }

    pipeline = [
        {"$match": payments_match},
        {"$group": {"_id": "$order_id", "total_paid": {"$sum": "$amount"}}}
    ]

    paid_map = {}
    for row in payments_col.aggregate(pipeline):
        key = row["_id"]                      # could be ObjectId or string
        total_paid = _f(row.get("total_paid"))
        paid_map[key] = total_paid
        try:
            paid_map[str(key)] = total_paid   # store both forms
        except Exception:
            pass

    # ---- Decorate each order with amount_paid / amount_left ----
    for o in orders:
        total_debt = _f(o.get("total_debt"))
        paid_external = _f(paid_map.get(o["_id"])) or _f(paid_map.get(str(o["_id"])))
        paid_embedded = _sum_embedded(o)
        o["amount_paid"] = round(paid_external + paid_embedded, 2)
        o["amount_left"] = round(total_debt - o["amount_paid"], 2)

        # (Optional) normalize weird mongo date dicts if any
        for field in ("date", "due_date"):
            v = o.get(field)
            if isinstance(v, dict) and "$date" in v:
                try:
                    ms = int(v["$date"].get("$numberLong", 0))
                    o[field] = datetime.fromtimestamp(ms / 1000.0)
                except Exception:
                    pass

    # ✅ Latest approved (if any), and compute its summary from decorated values
    latest_approved = next(
        (o for o in orders if (o.get("status") or "").lower() == "approved"),
        None
    )

    if latest_approved:
        total_paid = _f(latest_approved.get("amount_paid"))
        amount_left = max(_f(latest_approved.get("total_debt")) - total_paid, 0.0)
    else:
        total_paid = 0.0
        amount_left = 0.0

    return render_template(
        "client/client_order_history.html",
        orders=orders,                       # each has amount_paid & amount_left
        client=client,
        latest_approved=latest_approved,
        total_paid=round(total_paid, 2),
        amount_left=round(amount_left, 2)
    )
