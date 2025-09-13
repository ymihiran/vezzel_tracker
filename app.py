from flask import Flask, jsonify, request
import requests
import io
import pdfplumber
from datetime import datetime, timedelta
from flask_cors import CORS
from pymongo import MongoClient
import os

app = Flask(__name__)
CORS(app)

PDF_URL = "http://ezport.hipg.lk/Localfolder/Berthing/CQYB.pdf"
VALID_PORTS = {"Mundra", "Deendayal", "Mumbai", "Pipavav"}

# MongoDB connection
client = MongoClient("mongodb+srv://yasantha:Yasantha%40123@fronxc.mhfwocx.mongodb.net/?retryWrites=true&w=majority&appName=fronxC")
db = client["shipdb"]
orders_col = db["orders"]

def download_pdf(url):
    response = requests.get(url)
    return io.BytesIO(response.content)

def extract_data_from_pdf(pdf_file):
    result = []
    today = datetime.today()
    min_eta = today - timedelta(days=20)
    max_eta = today + timedelta(days=20)

    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            count = 0
            for table in tables:
                for row in table:
                    # skip first 2 rows if 1st page
                    if count < 2 and page.page_number == 1:
                        count += 1
                        continue

                    vessel_type = row[3].strip().lower()
                    if "roro" not in vessel_type:
                        continue

                    last_port = row[4].strip().capitalize()
                    if last_port not in VALID_PORTS:
                        continue

                    result.append({
                        "vessel_name": row[6].strip(),
                        "eta": row[0],
                        "last_port": last_port,
                        "next_port": row[5].strip(),
                        "discharge": row[13].strip(),
                        "loading": row[14].strip(),
                        "remarks": row[18].strip()
                    })

    return result

@app.route("/ships")
def ships():
    pdf = download_pdf(PDF_URL)
    data = extract_data_from_pdf(pdf)
    return jsonify(data)

@app.route("/save-order", methods=["POST"])
def save_order():
    payload = request.json
    required = ["whatsapp_number", "order_date", "called_date", "colour"]

    if not all(k in payload for k in required):
        return jsonify({"error": "Missing required fields"}), 400

    try:
        order = {
            "whatsapp_number": payload["whatsapp_number"],
            "order_date": datetime.strptime(payload["order_date"], "%Y-%m-%d"),
            "called_date": datetime.strptime(payload["called_date"], "%Y-%m-%d"),
            "colour": payload["colour"],
            "timestamp": datetime.utcnow()
        }
        orders_col.insert_one(order)
        return jsonify({"message": "Order saved successfully"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/latest-orders", methods=["GET"])
def latest_orders():
    pipeline = [
        {"$sort": {"order_date": -1}},
        {"$group": {
            "_id": "$colour",
            "latest_order_date": {"$first": "$order_date"}
        }}
    ]
    results = list(orders_col.aggregate(pipeline))
    data = {r["_id"]: r["latest_order_date"].strftime("%Y-%m-%d") for r in results}
    return jsonify(data)

# Cloud Run entrypoint
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))  # 👈 FIX: use Cloud Run's PORT
    app.run(host="0.0.0.0", port=port, debug=True)

# For Gunicorn / WSGI
application = app
