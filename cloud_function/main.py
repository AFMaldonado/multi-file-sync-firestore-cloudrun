import re
import os
import functions_framework
from google.cloud import firestore
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests

# Variables de entorno
PROJECT_ID = os.environ.get("PROJECT_ID")
CLOUD_RUN_URL = os.environ.get("CLOUD_RUN_URL")  # URL del servicio Cloud Run

@functions_framework.cloud_event
def handle_new_file(event):
    data = event.data
    file_name = data["name"]
    bucket_name = data["bucket"]

    # Validar nombre del archivo
    match = re.match(r"(ventas|clientes)_(\d{8})\.csv", file_name)
    if not match:
        print(f"Archivo ignorado: {file_name}")
        return

    tipo, fecha = match.groups()
    print(f"📥 Recibido archivo: {file_name} - tipo: {tipo}, fecha: {fecha}")

    # Conexión a Firestore
    db = firestore.Client()
    doc_ref = db.collection("archivos").document(fecha)
    doc = doc_ref.get()

    estado = {"ventas": False, "clientes": False}
    if doc.exists:
        estado.update(doc.to_dict())

    estado[tipo] = True
    doc_ref.set(estado)

    # Verificar si ya están ambos archivos
    if estado["ventas"] and estado["clientes"]:
        print(f"✅ Ambos archivos disponibles para {fecha}. Lanzando procesamiento...")

        try:
            credentials, _ = google.auth.default()
            auth_request = Request()
            target_audience = CLOUD_RUN_URL

            # Obtener el ID token firmado
            token = id_token.fetch_id_token(auth_request, target_audience)

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

            response = requests.post(
                CLOUD_RUN_URL,
                json={"fecha": fecha},
                headers=headers,
                timeout=30
            )

            print(f"🔁 Cloud Run respondió: {response.status_code}")
            print(response.text)

        except Exception as e:
            print("❌ Error al llamar a Cloud Run:", str(e))
    else:
        print(f"⏳ Aún falta un archivo para {fecha}. Estado actual: {estado}")
