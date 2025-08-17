from flask import Blueprint, render_template, request, jsonify, session
from bson import ObjectId
from db import db
from datetime import datetime

clientlist_bp = Blueprint('clientlist', __name__, template_folder='templates')

clients_collection = db["clients"]
deleted_collection = db["deleted"]

# ✅ Render shared client list partial
@clientlist_bp.route('/client_list_partial')
def client_list_partial():
    role = session.get('role', 'admin')
    clients = list(clients_collection.find().sort("date_registered", -1))
    return render_template('partials/client_list.html', clients=clients, role=role)

# ✅ Load clients with pagination, search, filter
@clientlist_bp.route('/clients/load', methods=['GET'])
def load_clients():
    page = int(request.args.get('page', 1))
    per_page = 20
    skip = (page - 1) * per_page

    search = request.args.get('search', '').strip().lower()
    status = request.args.get('status', '').strip().lower()
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

    query = {}

    if search:
        query["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"phone": {"$regex": search, "$options": "i"}},
            {"client_id": {"$regex": search, "$options": "i"}},
        ]

    if status:
        query["status"] = status

    if start_date and end_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            query["date_registered"] = {"$gte": start, "$lte": end}
        except ValueError:
            pass

    clients = list(clients_collection.find(query).skip(skip).limit(per_page).sort("date_registered", -1))
    for client in clients:
        client["_id"] = str(client["_id"])
        if client.get("date_registered"):
            client["date_registered"] = client["date_registered"].strftime('%Y-%m-%d')

    return jsonify(clients)

# ✅ Update client info from modal
@clientlist_bp.route('/clients/update', methods=['POST'])
def update_client():
    client_id = request.form.get('id')
    name = request.form.get('name')
    phone = request.form.get('phone')
    status = request.form.get('status')

    if not ObjectId.is_valid(client_id):
        return jsonify(success=False, error="Invalid client ID"), 400

    result = clients_collection.update_one(
        {"_id": ObjectId(client_id)},
        {"$set": {
            "name": name,
            "phone": phone,
            "status": status
        }}
    )

    if result.modified_count == 0:
        return jsonify(success=False, error="No changes made or client not found")

    return jsonify(success=True)

# ✅ Delete (archive) client
@clientlist_bp.route('/clients/delete/<client_id>', methods=['POST'])
def delete_client(client_id):
    if not ObjectId.is_valid(client_id):
        return jsonify(success=False, error="Invalid client ID"), 400

    client = clients_collection.find_one({"_id": ObjectId(client_id)})
    if not client:
        return jsonify(success=False, error="Client not found"), 404

    client["deleted_by"] = {
        "username": session.get("username", "unknown"),
        "role": session.get("role", "unknown"),
        "timestamp": datetime.utcnow()
    }

    deleted_collection.insert_one(client)
    clients_collection.delete_one({"_id": ObjectId(client_id)})

    return jsonify(success=True)
