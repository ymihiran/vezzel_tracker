from flask import Flask, jsonify
import requests
import io
import pdfplumber
from datetime import datetime, timedelta
from flask_cors import CORS
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)

PDF_URL = "http://ezport.hipg.lk/Localfolder/Berthing/CQYB.pdf"
VALID_PORTS = {"Mundra", "Deendayal", "Mumbai","Pipavav"}

# MongoDB connection
client = MongoClient("mongodb+srv://yasantha:Yasantha%40123@fronxc.mhfwocx.mongodb.net/?retryWrites=true&w=majority&appName=fronxC")  # change if remote
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
            count =0
            for table in tables:
                print("Row count in table:", len(table))
                for row in table:
                    # skip first 2 rows if 1st page
                    if count < 2 and page.page_number == 1:
                        count += 1
                        print(f"Skipping row {count} in table")
                        continue
                    
                    # try:
                    #     eta_str = row[0].split("-")[0].strip()  # Get date part before '-'
                    #     eta = datetime.strptime(eta_str, "%d/%m/%Y")
                    # except Exception:
                    #     print(f"Skipping row due to date parsing error: {row}")
                    #     continue

                    # if not (min_eta <= eta <= max_eta):
                    #     continue
                    
                    vessel_type = row[3].strip().lower()
                    print(f"Processing vessel type: {vessel_type} ")
                    if "roro" not in vessel_type:
                        continue
                    
                    last_port = row[4].strip().capitalize()
                    # print(f"Processing vessel: {row[6].strip()} with ETA: {eta_str} and Last Port: {last_port}")
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
    print(f"Extracted {len(data)} ships data.")
    return jsonify(data)

# 1️⃣ Save order
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

# 2️⃣ Get latest order_date per colour
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

if __name__ == "__main__":

   app.run(host='0.0.0.0' ,debug=True)
application = app


