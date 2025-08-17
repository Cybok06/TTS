from flask import Blueprint, render_template, jsonify, request
from bson import ObjectId
from datetime import datetime
from db import db

# Collections
payments_col = db["payments"]
clients_col = db["clients"]

# Blueprint
payments_bp = Blueprint("payments_bp", __name__)

# GET: Load the payments page
@payments_bp.route("/payments")
def view_payments():
    # Step 1: Fetch all payments sorted by date DESC
    payment_docs = list(payments_col.find({}, {
        "client_id": 1,
        "amount": 1,
        "bank_name": 1,
        "status": 1,
        "account_last4": 1,  # ✅ Include last 4 digits of account number
        "proof_url": 1,
        "date": 1
    }).sort("date", -1))

    # Step 2: Extract unique client ObjectIds
    client_ids = list({
        ObjectId(p["client_id"]) for p in payment_docs if p.get("client_id")
    })

    # Step 3: Batch-fetch all clients
    clients = clients_col.find({"_id": {"$in": client_ids}})
    client_map = {str(c["_id"]): c for c in clients}

    # Step 4: Build the payments list with mapped client info
    payments = []
    for p in payment_docs:
        raw_client_id = p.get("client_id")
        str_client_id = str(raw_client_id) if raw_client_id else None
        client = client_map.get(str_client_id)

        payments.append({
            "_id": str(p["_id"]),
            "client_name": client.get("name", "Unknown") if client else "Unknown",
            "client_id_str": client.get("client_id", "Unknown") if client else "Unknown",
            "phone": client.get("phone", "Unknown") if client else "Unknown",
            "amount": float(p.get("amount", 0)),
            "bank_name": p.get("bank_name", "-"),
            "account_last4": p.get("account_last4", ""),  # ✅ Added here
            "status": p.get("status", "pending"),
            "proof_url": p.get("proof_url", "#"),
            "date": p.get("date", "N/A")
        })

    return render_template("partials/payments.html", payments=payments)

# POST: Confirm payment
@payments_bp.route("/confirm_payment/<payment_id>", methods=["POST"])
def confirm_payment(payment_id):
    try:
        feedback = request.form.get("feedback", "").strip()
        update_fields = {
            "status": "confirmed",
            "confirmed_at": datetime.now()
        }
        if feedback:
            update_fields["feedback"] = feedback

        result = payments_col.update_one(
            {"_id": ObjectId(payment_id)},
            {"$set": update_fields}
        )

        if result.modified_count == 1:
            return jsonify({"success": True})
        else:
            return jsonify({"success": False, "error": "No matching payment found."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
from flask import Blueprint, render_template, jsonify, request
from bson import ObjectId
from datetime import datetime
from db import db

# Collections
payments_col = db["payments"]
clients_col = db["clients"]

# Blueprint
payments_bp = Blueprint("payments_bp", __name__)

# GET: Load the payments page
@payments_bp.route("/payments")
def view_payments():
    # Step 1: Fetch all payments sorted by date DESC
    payment_docs = list(payments_col.find({}, {
        "client_id": 1,
        "amount": 1,
        "bank_name": 1,
        "status": 1,
        "account_last4": 1,  # ✅ Include last 4 digits of account number
        "proof_url": 1,
        "date": 1
    }).sort("date", -1))

    # Step 2: Extract unique client ObjectIds
    client_ids = list({
        ObjectId(p["client_id"]) for p in payment_docs if p.get("client_id")
    })

    # Step 3: Batch-fetch all clients
    clients = clients_col.find({"_id": {"$in": client_ids}})
    client_map = {str(c["_id"]): c for c in clients}

    # Step 4: Build the payments list with mapped client info
    payments = []
    for p in payment_docs:
        raw_client_id = p.get("client_id")
        str_client_id = str(raw_client_id) if raw_client_id else None
        client = client_map.get(str_client_id)

        payments.append({
            "_id": str(p["_id"]),
            "client_name": client.get("name", "Unknown") if client else "Unknown",
            "client_id_str": client.get("client_id", "Unknown") if client else "Unknown",
            "phone": client.get("phone", "Unknown") if client else "Unknown",
            "amount": float(p.get("amount", 0)),
            "bank_name": p.get("bank_name", "-"),
            "account_last4": p.get("account_last4", ""),  # ✅ Added here
            "status": p.get("status", "pending"),
            "proof_url": p.get("proof_url", "#"),
            "date": p.get("date", "N/A")
        })

    return render_template("partials/payments.html", payments=payments)

# POST: Confirm payment
@payments_bp.route("/confirm_payment/<payment_id>", methods=["POST"])
def confirm_payment(payment_id):
    try:
        feedback = request.form.get("feedback", "").strip()
        update_fields = {
            "status": "confirmed",
            "confirmed_at": datetime.now()
        }
        if feedback:
            update_fields["feedback"] = feedback

        result = payments_col.update_one(
            {"_id": ObjectId(payment_id)},
            {"$set": update_fields}
        )

        if result.modified_count == 1:
            return jsonify({"success": True})
        else:
            return jsonify({"success": False, "error": "No matching payment found."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
