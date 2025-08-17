from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from db import users_collection, clients_collection
from werkzeug.security import check_password_hash

login_bp = Blueprint('login', __name__, template_folder='templates')


@login_bp.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()

        # Clear previous session
        session.clear()

        # === Admin / Assistant Login ===
        user = users_collection.find_one({"username": username})
        if user and check_password_hash(user.get("password", ""), password):

            # Check assistant lock
            if user.get("role") == "assistant" and user.get("status") != "active":
                flash("Account is locked. Contact admin.", "danger")
                return redirect(url_for('login.login'))

            session['username'] = username
            session['role'] = user['role']
            session['name'] = user.get("name", "Admin")

            if user['role'] == 'admin':
                return redirect(url_for('admin_dashboard.dashboard'))
            elif user['role'] == 'assistant':
                return redirect(url_for('assistant_dashboard.dashboard'))
            else:
                flash("Unauthorized role.", "warning")
                return redirect(url_for('login.login'))

        # === Registered Client Login (client_id + phone) ===
        client = clients_collection.find_one({"client_id": username})
        if client and client.get("phone") == password:
            session['role'] = 'client'
            session['client_id'] = str(client['_id'])
            session['client_code'] = client['client_id']
            session['client_name'] = client['name']
            return redirect(url_for('client_dashboard.dashboard'))

        # === External Client Login (name + phone) ===
        external = clients_collection.find_one({
            "name": {"$regex": f"^{username}$", "$options": "i"},
            "phone": password,
            "status": "external"
        })
        if external:
            session['role'] = 'external'
            session['external_id'] = str(external['_id'])
            session['external_name'] = external['name']
            session['external_phone'] = external['phone']
        return redirect(url_for('external.external_dashboard'))

        # If no match
        flash("Invalid credentials", "danger")
        return redirect(url_for('login.login'))

    return render_template('login.html')
