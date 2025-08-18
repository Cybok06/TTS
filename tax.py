from flask import Blueprint, render_template, request, jsonify, send_file
from datetime import datetime, timedelta
from bson import ObjectId, errors
from io import BytesIO
from db import db
import calendar
import re
import requests

tax_bp = Blueprint("tax", __name__, template_folder="templates")

orders_col = db["orders"]
tax_col   = db["tax_records"]

# ---------- helpers ----------
def _f(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default

def _fmt(n):
    try:
        return f"{float(n):,.2f}"
    except Exception:
        return "0.00"

def _str_oid(v):
    try:
        return str(v) if isinstance(v, ObjectId) else str(ObjectId(v))
    except Exception:
        return None

def _month_buckets():
    return {m: 0.0 for m in list(calendar.month_name)[1:]}

def _stax_per_l(order: dict) -> float:
    """Get S-Tax per litre from the order (supports 's_tax' and 's-tax')."""
    for k in ("s_tax", "s-tax"):
        if k in order and order.get(k) is not None:
            val = _f(order.get(k), None)
            if val is not None:
                return float(val)
    return 0.0

def _order_stax_due(order: dict) -> float:
    """Due = S-Tax per L × quantity."""
    q = _f(order.get("quantity"), 0.0)
    stax = _stax_per_l(order)
    return round(q * stax, 2)

def _is_paid(order: dict) -> bool:
    v1 = str(order.get("s_tax_payment", "")).lower()
    v2 = str(order.get("s-tax-payment", "")).lower()
    return v1 == "paid" or v2 == "paid"

def _parse_date_start(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None

def _parse_date_end(s):
    """Parse and set to end-of-day so filters include the full 'to' date."""
    dt = _parse_date_start(s)
    if not dt:
        return None
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)

def _paid_type_query():
    # robust & index-friendly: match s-tax, s_tax, s tax (any case)
    return {"type": {"$regex": r"^s[\s_-]*tax$", "$options": "i"}}

# ---------- views ----------
@tax_bp.route("/tax", methods=["GET"])
def tax_dashboard():
    # ===== UNPAID =====
    # S-Tax eligible if: explicit type 's_tax' OR 'combo' OR has s_tax/s-tax > 0
    unpaid_query = {
        "$and": [
            {"$or": [
                {"order_type": "s_tax"},
                {"order_type": "combo"},
                {"s_tax": {"$gt": 0}},
                {"s-tax": {"$gt": 0}},
            ]},
            {"$or": [{"s_tax_payment": {"$exists": False}}, {"s_tax_payment": {"$ne": "paid"}}]},
            {"$or": [{"s-tax-payment": {"$exists": False}}, {"s-tax-payment": {"$ne": "paid"}}]},
        ],
    }
    projection = {
        "_id": 1, "order_id": 1, "omc": 1, "quantity": 1,
        "s_tax": 1, "s-tax": 1,
        "due_date": 1, "date": 1,
    }
    unpaid_orders = list(orders_col.find(unpaid_query, projection).sort("date", -1))

    unpaid_rows, total_unpaid_sum = [], 0.0
    for o in unpaid_orders:
        due = _order_stax_due(o)  # s_tax × qty
        total_unpaid_sum += due
        unpaid_rows.append({
            "_id": _str_oid(o.get("_id")),
            "order_id": o.get("order_id", "—"),
            "omc": o.get("omc", "—"),
            "due_amount": due,
            "due_amount_fmt": _fmt(due),
            "payment_status": "Pending",
            "payment_badge": "warning",
            "due_date": o.get("due_date"),
            "date": o.get("date"),
            "quantity_fmt": _fmt(_f(o.get("quantity"), 0.0)),
            # Keep the key name the template expects; now it shows S-Tax per L
            "s_price_fmt": _fmt(_stax_per_l(o)),
        })

    # ===== FILTERS for PAID table =====
    omc_f      = (request.args.get("omc") or "").strip()
    paid_by_f  = (request.args.get("paid_by") or "").strip()
    date_from_s= (request.args.get("date_from") or "").strip()
    date_to_s  = (request.args.get("date_to") or "").strip()
    amt_min_s  = (request.args.get("amount_min") or "").strip()
    amt_max_s  = (request.args.get("amount_max") or "").strip()

    paid_query = {"$and": [_paid_type_query()]}
    if omc_f:
        paid_query["$and"].append({"omc": omc_f})
    if paid_by_f:
        paid_query["$and"].append({"paid_by": {"$regex": re.escape(paid_by_f), "$options": "i"}})

    df = _parse_date_start(date_from_s)
    dt = _parse_date_end(date_to_s)
    if df and dt:
        paid_query["$and"].append({"payment_date": {"$gte": df, "$lte": dt}})
    elif df:
        paid_query["$and"].append({"payment_date": {"$gte": df}})
    elif dt:
        paid_query["$and"].append({"payment_date": {"$lte": dt}})

    try:
        if amt_min_s:
            paid_query["$and"].append({"amount": {"$gte": float(amt_min_s)}})
    except ValueError:
        pass
    try:
        if amt_max_s:
            paid_query["$and"].append({"amount": {"$lte": float(amt_max_s)}})
    except ValueError:
        pass

    # ===== PAID rows (filtered) =====
    paid_rows, total_paid_sum = [], 0.0
    for t in tax_col.find(paid_query, {
        "_id": 0, "type": 1, "amount": 1, "payment_date": 1, "reference": 1,
        "paid_by": 1, "omc": 1, "order_id": 1, "order_oid": 1
    }).sort("payment_date", -1):
        amt = _f(t.get("amount"), 0.0)
        total_paid_sum += amt
        pd = t.get("payment_date")
        if isinstance(pd, str):
            try:
                pd_dt = datetime.strptime(pd, "%Y-%m-%d")
            except Exception:
                pd_dt = None
        else:
            pd_dt = pd if isinstance(pd, datetime) else None

        paid_rows.append({
            "omc": t.get("omc", "—"),
            "order_id": t.get("order_id", "—"),
            "amount": amt,
            "amount_fmt": _fmt(amt),
            "payment_date": pd,
            "payment_date_str": pd_dt.strftime("%Y-%m-%d") if pd_dt else str(pd or "—"),
            "reference": t.get("reference", "—"),
            "paid_by": t.get("paid_by", "—"),
        })

    # ===== CARDS: totals per OMC (ALL S-Tax, not filtered) =====
    pipeline_cards = [
        {"$match": _paid_type_query()},
        {"$group": {"_id": "$omc", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}},
    ]
    omc_cards = []
    for d in tax_col.aggregate(pipeline_cards):
        name = d.get("_id") or "—"
        total = float(d.get("total") or 0.0)
        if total > 0:
            omc_cards.append({"omc": name, "total": total, "total_fmt": _fmt(total)})

    # ===== Trend (all S-Tax) =====
    trend = _month_buckets()
    for row in tax_col.find(_paid_type_query(), {"amount": 1, "payment_date": 1}):
        dtp = row.get("payment_date")
        try:
            if isinstance(dtp, str):
                try:
                    dtp = datetime.strptime(dtp, "%Y-%m-%d")
                except Exception:
                    continue
            if not isinstance(dtp, datetime):
                continue
            trend[calendar.month_name[dtp.month]] += _f(row.get("amount"), 0.0)
        except Exception:
            continue

    return render_template(
        "partials/tax_dashboard.html",
        unpaid_rows=unpaid_rows,
        total_unpaid_sum=_fmt(total_unpaid_sum),
        paid_rows=paid_rows,
        total_paid_sum=_fmt(total_paid_sum),
        omc_cards=omc_cards,
        filters={
            "omc": omc_f, "paid_by": paid_by_f,
            "date_from": date_from_s, "date_to": date_to_s,
            "amount_min": amt_min_s, "amount_max": amt_max_s,
        },
        trend_data=trend
    )

@tax_bp.route("/tax/pay", methods=["POST"])
def pay_stax():
    try:
        order_oid = (request.form.get("order_oid") or "").strip()
        amount = _f(request.form.get("amount"))
        reference = (request.form.get("reference") or "").strip()
        paid_by = (request.form.get("paid_by") or "").strip()
        payment_date_str = (request.form.get("payment_date") or "").strip()

        if not order_oid:
            return jsonify({"status": "error", "message": "Missing order id"}), 400
        try:
            oid = ObjectId(order_oid)
        except (errors.InvalidId, Exception):
            return jsonify({"status": "error", "message": "Invalid order id"}), 400

        order = orders_col.find_one({"_id": oid})
        if not order:
            return jsonify({"status": "error", "message": "Order not found"}), 404

        # S-Tax eligible if explicit type s_tax/combo OR has s_tax value
        is_stax_type    = str(order.get("order_type", "")).lower() in {"s_tax", "combo"}
        has_stax_value  = _stax_per_l(order) > 0
        if not (is_stax_type or has_stax_value):
            return jsonify({"status": "error", "message": "Order has no S-Tax to pay"}), 400

        if _is_paid(order):
            return jsonify({"status": "error", "message": "S-Tax already recorded as paid"}), 400

        due = _order_stax_due(order)  # s_tax × qty
        if amount <= 0:
            return jsonify({"status": "error", "message": "Amount must be greater than 0"}), 400
        if amount < due:
            return jsonify({"status": "error", "message": f"Amount must cover S-Tax due (GH₵ {_fmt(due)})"}), 400

        pay_dt = datetime.utcnow()
        if payment_date_str:
            try:
                pay_dt = datetime.strptime(payment_date_str, "%Y-%m-%d")
            except ValueError:
                return jsonify({"status": "error", "message": "Invalid payment date"}), 400

        tax_col.insert_one({
            "type": "S-Tax",
            "amount": round(float(amount), 2),
            "payment_date": pay_dt,
            "reference": reference or None,
            "paid_by": paid_by or None,
            "omc": order.get("omc"),
            "order_id": order.get("order_id"),
            "order_oid": oid,
            "submitted_at": datetime.utcnow()
        })

        orders_col.update_one({"_id": oid}, {"$set": {
            "s_tax_payment": "paid",
            "s-tax-payment": "paid",
            "s_tax_paid_amount": round(float(amount), 2),
            "s_tax_paid_at": pay_dt,
            "s_tax_reference": reference or None,
            "s_tax_paid_by": paid_by or None
        }})
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@tax_bp.route("/tax/add", methods=["POST"])
def add_tax():
    try:
        tax_type     = (request.form.get("type") or "").strip()
        amount       = _f(request.form.get("amount"))
        payment_date = (request.form.get("payment_date") or "").strip()
        reference    = (request.form.get("reference") or "").strip() or None
        paid_by      = (request.form.get("paid_by") or "").strip() or None

        if not tax_type:
            return jsonify({"status": "error", "message": "Type is required"}), 400
        if amount <= 0:
            return jsonify({"status": "error", "message": "Amount must be greater than 0"}), 400

        pay_dt = datetime.utcnow()
        if payment_date:
            try:
                pay_dt = datetime.strptime(payment_date, "%Y-%m-%d")
            except ValueError:
                return jsonify({"status": "error", "message": "Invalid payment date"}), 400

        new_tax = {
            "type": tax_type,
            "amount": round(amount, 2),
            "payment_date": pay_dt,
            "reference": reference,
            "paid_by": paid_by,
            "submitted_at": datetime.utcnow()
        }
        tax_col.insert_one(new_tax)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ===== Export filtered PAID transactions to PDF (server-side) =====
@tax_bp.route("/tax/export.pdf", methods=["GET"])
def export_tax_pdf():
    omc_f      = (request.args.get("omc") or "").strip()
    paid_by_f  = (request.args.get("paid_by") or "").strip()
    date_from_s= (request.args.get("date_from") or "").strip()
    date_to_s  = (request.args.get("date_to") or "").strip()
    amt_min_s  = (request.args.get("amount_min") or "").strip()
    amt_max_s  = (request.args.get("amount_max") or "").strip()

    q = {"$and": [_paid_type_query()]}
    if omc_f:     q["$and"].append({"omc": omc_f})
    if paid_by_f: q["$and"].append({"paid_by": {"$regex": re.escape(paid_by_f), "$options": "i"}})

    df = _parse_date_start(date_from_s)
    dt = _parse_date_end(date_to_s)
    if df and dt:   q["$and"].append({"payment_date": {"$gte": df, "$lte": dt}})
    elif df:        q["$and"].append({"payment_date": {"$gte": df}})
    elif dt:        q["$and"].append({"payment_date": {"$lte": dt}})

    try:
        if amt_min_s: q["$and"].append({"amount": {"$gte": float(amt_min_s)}})
    except ValueError:
        pass
    try:
        if amt_max_s: q["$and"].append({"amount": {"$lte": float(amt_max_s)}})
    except ValueError:
        pass

    rows = list(tax_col.find(q).sort("payment_date", -1))

    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.pdfgen import canvas
        from reportlab.lib.units import mm
        from reportlab.lib.utils import ImageReader
        from reportlab.pdfbase.pdfmetrics import stringWidth

        # Try to fetch logo (AVIF may fail; ignore if so)
        logo_bytes = None
        try:
            resp = requests.get("https://res.cloudinary.com/dl2ipzxyk/image/upload/v1751107241/logo_ijmteg.avif", timeout=6)
            if resp.ok:
                logo_bytes = resp.content
        except Exception:
            logo_bytes = None

        buf = BytesIO()
        W, H = landscape(A4)
        left_margin, right_margin = 15*mm, 15*mm
        top_margin, bottom_margin = 12*mm, 12*mm
        table_width = W - left_margin - right_margin

        col_defs = [
            ("Date", 28*mm),
            ("OMC", 70*mm),
            ("Order ID", 28*mm),
            ("Paid By", 38*mm),
            ("Reference", 75*mm),
            ("Amount (GH₵)", 30*mm),
        ]
        total_widths = sum(w for _, w in col_defs)
        scale = table_width / total_widths
        col_defs = [(h, w*scale) for h, w in col_defs]

        def draw_header(c, page_num):
            y = H - top_margin
            x = left_margin
            if logo_bytes:
                try:
                    img = ImageReader(BytesIO(logo_bytes))
                    logo_h = 10*mm
                    iw, ih = img.getSize()
                    ratio = logo_h / ih
                    logo_w = iw * ratio
                    c.drawImage(img, x, y - logo_h, width=logo_w, height=logo_h, mask='auto')
                    x += logo_w + 6
                except Exception:
                    pass
            c.setFont("Helvetica-Bold", 13)
            c.drawString(x, y - 3*mm, "TrueType Services")
            c.setFont("Helvetica", 10)
            title = "S-Tax Payments Report"
            if omc_f:
                title += f" — {omc_f}"
            c.drawString(left_margin, y - 12*mm, title)
            c.setFont("Helvetica", 8)
            c.drawRightString(W - right_margin, y - 12*mm, f"Generated: {datetime.utcnow():%Y-%m-%d %H:%M UTC}  |  Page {page_num}")

            c.setLineWidth(0.6)
            header_y = y - 18*mm
            c.line(left_margin, header_y, left_margin + table_width, header_y)
            c.setFont("Helvetica-Bold", 9)
            xh = left_margin
            for head, w in col_defs:
                c.drawString(xh + 2, header_y - 9, head)
                xh += w
            c.line(left_margin, header_y - 12, left_margin + table_width, header_y - 12)
            return header_y - 14

        def draw_table(c):
            y = draw_header(c, draw_table.page)
            c.setFont("Helvetica", 9)
            line_height = 10
            x_cols = [left_margin]
            for _, w in col_defs:
                x_cols.append(x_cols[-1] + w)

            total_amt = 0.0
            for r in rows:
                if y < bottom_margin + 20*mm:
                    c.showPage()
                    draw_table.page += 1
                    y = draw_header(c, draw_table.page)
                    c.setFont("Helvetica", 9)

                pd = r.get("payment_date")
                date_str = pd.strftime("%Y-%m-%d") if isinstance(pd, datetime) else str(pd or "—")
                vals = [
                    date_str,
                    r.get("omc", "—"),
                    r.get("order_id", "—"),
                    r.get("paid_by", "—"),
                    r.get("reference", "—"),
                    _fmt(_f(r.get("amount"), 0.0)),
                ]
                for i, val in enumerate(vals):
                    txt = str(val)
                    maxw = col_defs[i][1] - 4
                    while stringWidth(txt, "Helvetica", 9) > maxw and len(txt) > 3:
                        txt = txt[:-4] + "…"
                    x = x_cols[i] + 2
                    c.drawString(x, y, txt)
                c.setLineWidth(0.3)
                c.line(left_margin, y - 2, left_margin + table_width, y - 2)
                y -= line_height
                total_amt += _f(r.get("amount"), 0.0)

            c.setFont("Helvetica-Bold", 9)
            c.line(left_margin, y - 2, left_margin + table_width, y - 2)
            c.drawRightString(x_cols[-1], y - 10, f"TOTAL: GH₵ {_fmt(total_amt)}")

        c = canvas.Canvas(buf, pagesize=landscape(A4))
        draw_table.page = 1
        draw_table(c)
        c.showPage()
        c.save()
        buf.seek(0)

        safe_omc = re.sub(r'\W+', '_', omc_f.lower()) if omc_f else ""
        filename = f"s_tax_payments_{safe_omc}.pdf" if safe_omc else "s_tax_payments.pdf"
        return send_file(buf, mimetype="application/pdf", as_attachment=True, download_name=filename)
    except Exception as e:
        return jsonify({"status": "error", "message": f"PDF generation failed: {e}. Install 'reportlab'."}), 500
