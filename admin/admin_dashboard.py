from flask import Blueprint, render_template, session
from db import db

admin_dashboard_bp = Blueprint('admin_dashboard', __name__, template_folder='templates')

# Collections
clients_collection = db["clients"]
orders_collection = db["orders"]
payments_collection = db["payments"]
truck_payments_collection = db["truck_payments"]

@admin_dashboard_bp.route('/dashboard')  # ✅ Make sure the @ is present
def dashboard():
    # Count unapproved client orders
    unapproved_orders_count = orders_collection.count_documents({"status": "pending"})

    # Count overdue clients
    overdue_clients_count = clients_collection.count_documents({"status": "overdue"})

    # Count unconfirmed normal payments
    unconfirmed_payments_count = payments_collection.count_documents({"status": "pending"})

    # Count unconfirmed truck payments
    unconfirmed_truck_payments_count = truck_payments_collection.count_documents({"status": "pending"})

    # ✅ Count truck debtors
    pipeline = [
        {
            "$group": {
                "_id": "$client_id",
                "total_debt": {"$sum": "$total_debt"},
                "total_paid": {"$sum": "$paid"},
            }
        },
        {
            "$project": {
                "amount_left": {"$subtract": ["$total_debt", "$total_paid"]}
            }
        },
        {
            "$match": {
                "amount_left": {"$gt": 0}
            }
        },
        {
            "$count": "truck_debtors_count"
        }
    ]
    agg_result = list(db["orders"].aggregate(pipeline))
    truck_debtors_count = agg_result[0]["truck_debtors_count"] if agg_result else 0

    return render_template(
        'admin/admin_dashboard.html',
        unapproved_orders_count=unapproved_orders_count,
        overdue_clients_count=overdue_clients_count,
        unconfirmed_payments_count=unconfirmed_payments_count,
        unconfirmed_truck_payments_count=unconfirmed_truck_payments_count,
        truck_debtors_count=truck_debtors_count
    )
