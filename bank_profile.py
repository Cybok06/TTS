from flask import Blueprint, render_template, request
from db import db
from bson import ObjectId
from datetime import datetime

bank_profile_bp = Blueprint("bank_profile", __name__, template_folder="templates")

accounts_col = db["bank_accounts"]
payments_col = db["payments"]

@bank_profile_bp.route("/bank-profile/<bank_id>")
def bank_profile(bank_id):
    bank = accounts_col.find_one({"_id": ObjectId(bank_id)})
    if not bank:
        return "Bank not found", 404

    bank_name = bank.get("bank_name")
    last4 = bank.get("account_number")[-4:]

    # Time filtering
    start_str = request.args.get("start_date")
    end_str = request.args.get("end_date")

    query = {
        "bank_name": bank_name,
        "account_last4": last4,
        "status": "confirmed"
    }

    if start_str and end_str:
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_str, "%Y-%m-%d")
            query["date"] = {"$gte": start_date, "$lte": end_date}
        except ValueError:
            pass  # Ignore if invalid format

    payments = list(payments_col.find(query).sort("date", -1))

    total_received = sum(float(p.get("amount", 0)) for p in payments)

    return render_template("partials/bank_profile.html", bank=bank, payments=payments, total_received=total_received, start_date=start_str, end_date=end_str)
