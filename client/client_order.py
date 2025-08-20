from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from datetime import datetime
from bson import ObjectId, Regex
from db import db
import random, string
from pymongo.errors import DuplicateKeyError

client_order_bp = Blueprint('client_order', __name__, template_folder='templates')

orders_collection = db["orders"]
products_collection = db["products"]
trucks_collection = db["trucks"]
truck_orders_collection = db["truck_orders"]

# Ensure unique human-friendly order_id
orders_collection.create_index("order_id", unique=True, sparse=True)

def _to_int_qty(q):
    if not q:
        return None
    return int(str(q).replace(",", "").strip())

def _maybe_oid(val):
    try:
        return ObjectId(val)
    except Exception:
        return val  # fall back to raw string if not a valid ObjectId

def _generate_order_id():
    """Return a random 5-char uppercase alphanumeric code."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))

@client_order_bp.route('/submit_order', methods=['GET', 'POST'])
def submit_order():
    if 'client_id' not in session:
        flash("Please log in to place an order", "danger")
        return redirect(url_for('client_login'))

    if request.method == 'POST':
        product = request.form.get('product')
        quantity = _to_int_qty(request.form.get('quantity'))
        region = request.form.get('region')
        vehicle_number = request.form.get('vehicle_number')
        driver_name = request.form.get('driver_name')
        driver_phone = request.form.get('driver_phone')
        selected_truck_number = request.form.get('vehicle_number')  # used for matching below

        if not all([product, quantity, region, vehicle_number, driver_name, driver_phone]):
            flash("All fields are required.", "danger")
            return redirect(url_for('client_order.submit_order'))

        # Try to find a truck match by truck_number (used in dropdown)
        truck = trucks_collection.find_one({"truck_number": selected_truck_number})

        # Snapshot current product prices + taxes into the order
        prod_doc = products_collection.find_one(
            {"name": Regex(f"^{product}$", "i")},
            {"s_price": 1, "p_price": 1, "s_tax": 1, "p_tax": 1, "name": 1}
        )
        snapshot_s_price = (prod_doc or {}).get("s_price")
        snapshot_p_price = (prod_doc or {}).get("p_price")
        snapshot_s_tax   = (prod_doc or {}).get("s_tax")
        snapshot_p_tax   = (prod_doc or {}).get("p_tax")

        # Build the base order doc
        base_order = {
            "client_id": _maybe_oid(session['client_id']),
            "product": product,
            "vehicle_number": vehicle_number,
            "driver_name": driver_name,
            "driver_phone": driver_phone,
            "quantity": quantity,
            "region": region,
            "status": "pending",
            "date": datetime.utcnow(),
            # store current prices & taxes at order time (what the client saw)
            "product_s_price": snapshot_s_price,
            "product_p_price": snapshot_p_price,
            "product_s_tax":   snapshot_s_tax,
            "product_p_tax":   snapshot_p_tax,
        }
        if truck:
            base_order["truck_id"] = truck["_id"]

        # Insert with a unique 5-char order_id; retry on rare collisions
        while True:
            code = _generate_order_id()
            doc = dict(base_order)
            doc["order_id"] = code
            try:
                result = orders_collection.insert_one(doc)
                order_mongo_id = result.inserted_id
                break
            except DuplicateKeyError:
                continue  # try another code

        # If truck was selected, create entry in truck_orders for admin approval
        if truck:
            truck_orders_collection.insert_one({
                "order_ref": str(order_mongo_id),
                "order_id": code,
                "client_id": session['client_id'],
                "truck_id": str(truck["_id"]),
                "truck_number": truck.get("truck_number"),
                "driver_name": truck.get("driver_name") or driver_name,
                "driver_phone": truck.get("driver_phone") or driver_phone,
                "quantity": quantity,
                "region": region,
                "status": "pending",
                "created_at": datetime.utcnow()
            })

        flash(f"Order submitted successfully! Your Order ID is {code}", "success")
        return redirect(url_for('client_order.submit_order'))

    # GET request: fetch product (with prices + taxes) and truck options
    products = list(products_collection.find(
        {},
        {"name": 1, "description": 1, "s_price": 1, "p_price": 1, "s_tax": 1, "p_tax": 1}
    ))
    trucks = list(trucks_collection.find(
        {},
        {"truck_number": 1, "capacity": 1, "driver_name": 1, "driver_phone": 1}
    ))
    return render_template('client/client_order.html', products=products, trucks=trucks)

@client_order_bp.route('/client/product_price', methods=['GET'])
def client_product_price():
    """
    AJAX: Return s_price, p_price, s_tax, p_tax, and a convenience s_total (per L)
    for a given product name.
    Query: /client/product_price?name=Ago%20cell%20Site
    Response: {
      success: True,
      s_price: 9.0, p_price: 8.9,
      s_tax: 0.5, p_tax: 0.3,
      s_total: 9.5
    }
    """
    name = (request.args.get('name') or '').strip()
    if not name:
        return jsonify({"success": False, "error": "Missing product name"}), 400

    product = products_collection.find_one(
        {"name": Regex(f"^{name}$", "i")},
        {"s_price": 1, "p_price": 1, "s_tax": 1, "p_tax": 1}
    )
    if not product:
        return jsonify({"success": False, "error": "Product not found"}), 404

    def _num(v): 
        try: return float(v or 0)
        except Exception: return 0.0

    s_price = _num(product.get("s_price"))
    p_price = _num(product.get("p_price"))
    s_tax   = _num(product.get("s_tax"))
    p_tax   = _num(product.get("p_tax"))

    return jsonify({
        "success": True,
        "s_price": s_price,
        "p_price": p_price,
        "s_tax":   s_tax,
        "p_tax":   p_tax,
        "s_total": s_price + s_tax
    })
