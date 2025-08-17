# share_links.py
from flask import Blueprint, request, render_template, redirect, url_for, session, jsonify
from bson import ObjectId
from db import db
from datetime import datetime, timedelta
import secrets, re
from werkzeug.security import generate_password_hash, check_password_hash

shared_bp = Blueprint("shared_links", __name__, template_folder="templates")

orders = db["orders"]
clients = db["clients"]
bdc = db["bdc"]
shared_links = db["shared_links"]  # NEW collection

STATUS_OPTIONS = [
    "Ordered", "Approved", "GoodStanding", "Depot Manager",
    "BRV check pass", "BRV check unpass", "Loading", "Loaded",
    "Moved", "Released"
]

# ---------- helpers ----------
def _now():
    return datetime.utcnow()

def _clean_bdc_name(name: str) -> str:
    return (name or "").strip()

def _require_5_digit(s: str) -> bool:
    return bool(re.fullmatch(r"\d{5}", s or ""))

def _token():
    # url‑safe token
    return secrets.token_urlsafe(24)

def _is_link_valid(link_doc):
    if not link_doc:
        return False
    if link_doc.get("revoked_at"):
        return False
    exp = link_doc.get("expires_at")
    if exp and exp < _now():
        return False
    return True

def _safe_oid(val):
    try:
        return ObjectId(val)
    except Exception:
        return None

# ---------- Admin UI: create link (browser page) ----------
@shared_bp.route("/deliveries/share/new", methods=["GET", "POST"])
def new_share_link_form():
    """
    GET  -> show form to create a share link
    POST -> validate inputs, create link, render result page with final URL
    """
    if request.method == "GET":
        # Optional: prefill BDC names from your collection to help selection
        bdc_names = sorted({d.get("name", "") for d in bdc.find({}, {"name": 1}) if d.get("name")})
        return render_template("shared/new_share_link_form.html", bdc_names=bdc_names, error=None)

    # POST -> Create link via same validation as API
    bdc_name = _clean_bdc_name(request.form.get("bdc_name"))
    passcode = (request.form.get("passcode") or "").strip()
    try:
        expires_in_days = int(request.form.get("expires_in_days") or 7)
    except Exception:
        expires_in_days = 7

    error = None
    if not bdc_name:
        error = "BDC is required."
    elif not _require_5_digit(passcode):
        error = "Passcode must be exactly 5 digits."
    elif not (1 <= expires_in_days <= 90):
        error = "Expiry must be between 1 and 90 days."

    if error:
        bdc_names = sorted({d.get("name", "") for d in bdc.find({}, {"name": 1}) if d.get("name")})
        return render_template("shared/new_share_link_form.html", bdc_names=bdc_names, error=error)

    token = _token()
    doc = {
        "token": token,
        "bdc_name": bdc_name,
        "pass_hash": generate_password_hash(passcode),
        "created_at": _now(),
        "expires_at": _now() + timedelta(days=expires_in_days),
        "revoked_at": None,
        "created_by": session.get("user_id"),  # optional
        "audit": [{"type": "create", "at": _now(), "by": session.get("user_id")}]
    }
    shared_links.insert_one(doc)

    shared_url = url_for("shared_links.shared_landing", token=token, _external=True)
    return render_template(
        "shared/new_share_link_result.html",
        bdc_name=bdc_name,
        shared_url=shared_url,
        passcode=passcode,  # show once to admin
        expires_at=doc["expires_at"]
    )

# ---------- Programmatic API: create a share link ----------
# POST /deliveries/share/create
@shared_bp.route("/deliveries/share/create", methods=["POST"])
def create_share_link():
    bdc_name = _clean_bdc_name(request.form.get("bdc_name"))
    passcode = (request.form.get("passcode") or "").strip()
    expires_in_days = request.form.get("expires_in_days", "").strip()

    if not bdc_name:
        return jsonify({"success": False, "message": "BDC is required."}), 400
    if not _require_5_digit(passcode):
        return jsonify({"success": False, "message": "Passcode must be exactly 5 digits."}), 400

    try:
        days = int(expires_in_days) if expires_in_days else 7  # default 7 days
        if days < 1 or days > 90:
            raise ValueError()
    except:
        return jsonify({"success": False, "message": "expires_in_days must be 1–90."}), 400

    token = _token()
    doc = {
        "token": token,
        "bdc_name": bdc_name,
        "pass_hash": generate_password_hash(passcode),
        "created_at": _now(),
        "expires_at": _now() + timedelta(days=days),
        "revoked_at": None,
        "created_by": session.get("user_id"),
        "audit": [{"type": "create", "at": _now(), "by": session.get("user_id")}]
    }
    shared_links.insert_one(doc)

    return jsonify({
        "success": True,
        "url": url_for("shared_links.shared_landing", token=token, _external=True),
        "expires_at": doc["expires_at"].isoformat() + "Z"
    })

# ---------- Admin: revoke link ----------
@shared_bp.route("/deliveries/share/<token>/revoke", methods=["POST"])
def revoke_share_link(token):
    link = shared_links.find_one({"token": token})
    if not link:
        return jsonify({"success": False, "message": "Link not found."}), 404
    if link.get("revoked_at"):
        return jsonify({"success": False, "message": "Already revoked."}), 400
    shared_links.update_one({"_id": link["_id"]}, {"$set": {"revoked_at": _now()}})
    return jsonify({"success": True})

# ---------- Partner: landing (asks for passcode) ----------
@shared_bp.route("/deliveries/shared/<token>", methods=["GET", "POST"])
def shared_landing(token):
    link = shared_links.find_one({"token": token})
    if not _is_link_valid(link):
        return render_template("shared/invalid_link.html"), 410  # Gone/invalid

    # Minimal in-session rate limiting for pass attempts
    key_attempts = f"pass_attempts:{token}"
    attempts = session.get(key_attempts, 0)

    if request.method == "POST":
        if attempts >= 10:
            return render_template("shared/passcode.html", token=token, bdc_name=link["bdc_name"],
                                   error="Too many attempts. Try again later."), 429

        passcode = (request.form.get("passcode") or "").strip()
        session[key_attempts] = attempts + 1

        if not _require_5_digit(passcode):
            return render_template("shared/passcode.html", token=token, bdc_name=link["bdc_name"],
                                   error="Enter exactly 5 digits.")

        if not check_password_hash(link["pass_hash"], passcode):
            return render_template("shared/passcode.html", token=token, bdc_name=link["bdc_name"],
                                   error="Incorrect passcode.")

        # Success: mark unlocked and set session
        session[f"shared_unlocked:{token}"] = True
        shared_links.update_one(
            {"_id": link["_id"]},
            {"$push": {"audit": {"type": "unlock", "at": _now(), "ip": request.remote_addr}}}
        )
        return redirect(url_for("shared_links.shared_manage", token=token))

    # GET -> show passcode form
    return render_template("shared/passcode.html", token=token, bdc_name=link["bdc_name"], error=None)

# ---------- Partner: manage deliveries (restricted) ----------
@shared_bp.route("/deliveries/shared/<token>/manage", methods=["GET"])
def shared_manage(token):
    link = shared_links.find_one({"token": token})
    if not _is_link_valid(link):
        return render_template("shared/invalid_link.html"), 410
    if not session.get(f"shared_unlocked:{token}"):
        return redirect(url_for("shared_links.shared_landing", token=token))

    bdc_name = link["bdc_name"]

    # Only approved orders visible, filtered by BDC
    filters = {"status": "approved", "bdc_name": bdc_name}
    projection = {
        "_id": 1, "client_id": 1, "bdc_name": 1, "product": 1,
        "vehicle_number": 1, "driver_name": 1, "driver_phone": 1,
        "quantity": 1, "region": 1,
        "delivery_status": 1, "tts_status": 1, "npa_status": 1,
        "date": 1, "delivered_date": 1
    }

    items = list(orders.find(filters, projection).sort("date", -1))

    # get client names
    client_ids = []
    for o in items:
        cid = o.get("client_id")
        if isinstance(cid, ObjectId):
            client_ids.append(cid)
        else:
            try:
                client_ids.append(ObjectId(str(cid)))
            except Exception:
                pass

    cmap = {str(c["_id"]): c.get("name", "Unknown")
            for c in clients.find({"_id": {"$in": list(set(client_ids))}}, {"name": 1})}

    deliveries = []
    for o in items:
        cid = str(o.get("client_id")) if o.get("client_id") else ""
        deliveries.append({
            "order_id": str(o["_id"]),
            "bdc_name": o.get("bdc_name", ""),
            "client_name": cmap.get(cid, "Unknown"),
            "product": o.get("product", ""),
            "vehicle_number": o.get("vehicle_number", ""),
            "driver_name": o.get("driver_name", ""),
            "driver_phone": o.get("driver_phone", ""),
            "quantity": o.get("quantity", 0),
            "region": o.get("region", ""),
            "delivery_status": o.get("delivery_status", "pending"),
            "tts_status": o.get("tts_status"),
            "npa_status": o.get("npa_status"),
            "date": o.get("date"),
            "delivered_date": o.get("delivered_date")
        })

    return render_template(
        "shared/manage_deliveries_shared.html",
        bdc_name=bdc_name,
        deliveries=deliveries,
        status_options=STATUS_OPTIONS,
        token=token
    )

# ---------- Partner: update (restricted + server-side BDC check) ----------
@shared_bp.route("/deliveries/shared/<token>/update_status/<order_id>", methods=["POST"])
def shared_update_status(token, order_id):
    link = shared_links.find_one({"token": token})
    if not _is_link_valid(link):
        return jsonify({"success": False, "message": "Invalid or expired link."}), 410
    if not session.get(f"shared_unlocked:{token}"):
        return jsonify({"success": False, "message": "Locked."}), 403

    oid = _safe_oid(order_id)
    if not oid:
        return jsonify({"success": False, "message": "Invalid order id."}), 400

    tts = (request.form.get("tts_status") or "").strip()
    npa = (request.form.get("npa_status") or "").strip()

    if not tts and not npa:
        return jsonify({"success": False, "message": "Provide TTS and/or NPA status."}), 400

    # Ensure the order belongs to the linked BDC
    order_doc = orders.find_one({"_id": oid}, {"bdc_name": 1})
    if not order_doc:
        return jsonify({"success": False, "message": "Order not found."}), 404
    if order_doc.get("bdc_name") != link["bdc_name"]:
        return jsonify({"success": False, "message": "Order not allowed for this link."}), 403

    update_fields = {}
    if tts:
        update_fields["tts_status"] = tts
    if npa:
        update_fields["npa_status"] = npa

    history_entry = {
        "tts_status": tts if tts else None,
        "npa_status": npa if npa else None,
        "by_shared_token": token,
        "timestamp": _now()
    }

    res = orders.update_one(
        {"_id": oid},
        {"$set": update_fields, "$push": {"delivery_history": history_entry}}
    )

    # audit the share link doc
    shared_links.update_one(
        {"_id": link["_id"]},
        {"$push": {"audit": {"type": "update", "at": _now(), "order_id": str(oid), "ip": request.remote_addr,
                             "tts": tts or None, "npa": npa or None}}}
    )

    return jsonify({"success": res.modified_count == 1, "message": "Updated" if res.modified_count == 1 else "No change"})
