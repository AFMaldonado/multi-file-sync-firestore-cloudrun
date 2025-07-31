import re
import os
import functions_framework
from google.cloud import firestore
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests

# Environment variables
PROJECT_ID = os.environ.get("PROJECT_ID")
CLOUD_RUN_URL = os.environ.get("CLOUD_RUN_URL")  # Cloud Run service URL

@functions_framework.cloud_event
def handle_new_file(event):
    data = event.data
    file_name = data["name"]
    bucket_name = data["bucket"]

    # Validate file name
    match = re.match(r"(sales|customers)_(\d{8})\.csv", file_name)
    if not match:
        print(f"Ignored file: {file_name}")
        return

    file_type, date = match.groups()
    print(f"📥 Received file: {file_name} - type: {file_type}, date: {date}")

    # Connect to Firestore
    db = firestore.Client()
    doc_ref = db.collection("files").document(date)
    doc = doc_ref.get()

    status = {"sales": False, "customers": False}
    if doc.exists:
        status.update(doc.to_dict())

    status[file_type] = True
    doc_ref.set(status)

    # Check if both files are already available
    if status["sales"] and status["customers"]:
        print(f"✅ Both files available for {date}. Triggering processing...")

        try:
            credentials, _ = google.auth.default()
            auth_request = Request()
            target_audience = CLOUD_RUN_URL

            # Get the signed ID token
            token = id_token.fetch_id_token(auth_request, target_audience)

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

            response = requests.post(
                CLOUD_RUN_URL,
                json={"date": date},
                headers=headers,
                timeout=30
            )

            print(f"🔁 Cloud Run responded with: {response.status_code}")
            print(response.text)

        except Exception as e:
            print("❌ Error while calling Cloud Run:", str(e))
    else:
        print(f"⏳ Still waiting for one file for {date}. Current status: {status}")
