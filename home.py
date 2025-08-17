from flask import Blueprint, render_template, session, redirect, url_for, flash, jsonify
from db import db
from bson import ObjectId
from datetime import datetime, timedelta

home_bp = Blueprint('home', __name__, template_folder='templates')

orders_collection = db['orders']
clients_collection = db['clients']
payments_collection = db['payments']
settings_collection = db['settings']

@home_bp.route('/home')
def dashboard_home():
    if 'role' not in session or session['role'] not in ['admin', 'assistant']:
        flash("Access denied.", "danger")
        return redirect(url_for('login.login'))

    settings_doc = settings_collection.find_one() or {}
    if not settings_doc.get('view_dashboard', False):
        return render_template('partials/home.html', dashboard_disabled=True)

    total_clients = clients_collection.estimated_document_count()
    total_orders = orders_collection.estimated_document_count()
    total_approved_orders = orders_collection.count_documents({'status': 'approved'})
    approval_rate = round((total_approved_orders / total_orders) * 100, 1) if total_orders else 0

    total_paid_cursor = payments_collection.aggregate([
        {"$match": {"status": "confirmed"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ])
    total_paid = next(total_paid_cursor, {}).get("total", 0)

    # ✅ Add total orders for today and yesterday
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)

    orders_today = orders_collection.count_documents({"date": {"$gte": today}})
    orders_yesterday = orders_collection.count_documents({
        "date": {"$gte": yesterday, "$lt": today}
    })

    return render_template('partials/home.html',
                           dashboard_disabled=False,
                           total_clients=total_clients,
                           total_orders=total_orders,
                           total_approved_orders=total_approved_orders,
                           approval_rate=approval_rate,
                           total_paid=round(total_paid, 2),
                           total_collected=round(total_paid, 2),
                           orders_today=orders_today,
                           orders_yesterday=orders_yesterday)

@home_bp.route('/home/details')
def dashboard_details():
    if 'role' not in session or session['role'] not in ['admin', 'assistant']:
        return jsonify({'error': 'Unauthorized'}), 403

    try:
        now = datetime.now()

        # Total Debt
        total_debt_cursor = orders_collection.aggregate([
            {"$match": {"status": "approved"}},
            {"$group": {"_id": None, "total_debt": {"$sum": "$total_debt"}}}
        ])
        total_debt = next(total_debt_cursor, {}).get("total_debt", 0)

        # Total Paid
        total_paid_cursor = payments_collection.aggregate([
            {"$match": {"status": "confirmed"}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ])
        total_paid = next(total_paid_cursor, {}).get("total", 0)

        # Monthly Orders
        months, order_counts = [], []
        for i in range(1, 13):
            start = datetime(now.year, i, 1)
            end = datetime(now.year + 1, 1, 1) if i == 12 else datetime(now.year, i + 1, 1)
            month_name = start.strftime('%B')
            months.append(month_name)
            order_counts.append(orders_collection.count_documents({'date': {'$gte': start, '$lt': end}}))

        # Top Clients
        top_clients_agg = list(orders_collection.aggregate([
            {"$group": {"_id": "$client_id", "order_count": {"$sum": 1}}},
            {"$sort": {"order_count": -1}},
            {"$limit": 5}
        ]))

        client_ids = [entry['_id'] for entry in top_clients_agg if entry['_id']]
        valid_object_ids = [cid if isinstance(cid, ObjectId) else ObjectId(cid) for cid in client_ids]
        client_map = {
            str(c['_id']): c.get('name', 'Unknown')
            for c in clients_collection.find({"_id": {"$in": valid_object_ids}})
        }

        top_clients_names = [client_map.get(str(entry['_id']), 'Unknown') for entry in top_clients_agg]
        top_clients_orders = [entry['order_count'] for entry in top_clients_agg]

        # Recent Activities
        three_days_ago = now - timedelta(days=3)
        orders = list(orders_collection.find({'status': 'approved', 'date': {'$gte': three_days_ago}})
                      .sort('date', -1).limit(5))
        payments = list(payments_collection.find({'status': 'confirmed', 'date': {'$gte': three_days_ago}})
                        .sort('date', -1).limit(5))
        overdues = list(orders_collection.find({'status': {'$ne': 'completed'}, 'due_date': {'$lt': now}})
                        .sort('due_date', -1).limit(5))

        recent_client_ids = list({o.get('client_id') for o in orders + payments + overdues})
        safe_client_ids = [cid if isinstance(cid, ObjectId) else ObjectId(cid) for cid in recent_client_ids if cid]
        clients = clients_collection.find({"_id": {"$in": safe_client_ids}})
        client_lookup = {str(c['_id']): c.get('name', 'Unknown') for c in clients}

        def format_time(dt):
            return dt.isoformat() if dt else now.isoformat()

        recent_activities = []

        for order in orders:
            name = client_lookup.get(str(order.get('client_id')), 'Unknown')
            recent_activities.append({
                "icon": "<i class='bi bi-check-circle-fill'></i>",
                "text": f"Order approved for {name} — {order.get('product', 'N/A')} (GHS {round(order.get('total_debt', 0), 2)})",
                "time": format_time(order.get('date')),
                "color": "text-success"
            })

        for payment in payments:
            name = client_lookup.get(str(payment.get('client_id')), 'Unknown')
            recent_activities.append({
                "icon": "<i class='bi bi-cash-stack'></i>",
                "text": f"Payment of GHS {round(payment.get('amount', 0), 2)} confirmed from {name} via {payment.get('method', 'N/A')}",
                "time": format_time(payment.get('date')),
                "color": "text-primary"
            })

        for order in overdues:
            name = client_lookup.get(str(order.get('client_id')), 'Unknown')
            recent_activities.append({
                "icon": "<i class='bi bi-exclamation-circle'></i>",
                "text": f"{name} missed due for {order.get('product', 'N/A')} — GHS {round(order.get('total_debt', 0), 2)}",
                "time": format_time(order.get('due_date')),
                "color": "text-danger"
            })

        recent_activities = sorted(recent_activities, key=lambda x: x['time'], reverse=True)[:8]

        return jsonify({
            "total_debt": round(total_debt, 2),
            "total_paid": round(total_paid, 2),
            "months": months,
            "order_counts": order_counts,
            "top_clients_names": top_clients_names,
            "top_clients_orders": top_clients_orders,
            "recent_activities": recent_activities
        })

    except Exception as e:
        print("Error in /home/details:", str(e))
        return jsonify({"error": "Failed to load dashboard details.", "details": str(e)}), 500
