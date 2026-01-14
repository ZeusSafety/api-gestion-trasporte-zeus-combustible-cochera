import functions_framework
import pymysql
import json
import requests
import logging
from datetime import datetime
from google.cloud import storage

# 🔧 Configuración
API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"
BUCKET_NAME = "archivos_sistema"
BASE_STORAGE_URL = f"https://storage.googleapis.com/{BUCKET_NAME}/"

def get_connection():
    return pymysql.connect(
        user="zeussafety-2024",
        password="ZeusSafety2025",
        db="Zeus_Safety_Data_Integration",
        unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
        cursorclass=pymysql.cursors.DictCursor
    )

def subir_a_gcs(archivo, folder="archivos_generales_zeus"):
    """ Sube directamente a Google Cloud Storage sin pasar por APIs intermedias """
    if not archivo or archivo.filename == '':
        return None
    try:
        # 1. Preparar el cliente de Storage
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        
        # 2. Crear un nombre único para evitar colisiones
        timestamp = int(datetime.now().timestamp())
        filename = f"{timestamp}_{archivo.filename.replace(' ', '_')}"
        blob_path = f"{folder}/{filename}"
        
        # 3. Subir el archivo
        blob = bucket.blob(blob_path)
        # Aseguramos que el puntero esté al inicio
        archivo.seek(0)
        blob.upload_from_string(
            archivo.read(),
            content_type=archivo.content_type
        )
        
        logging.info(f"Archivo subido con éxito: {blob_path}")
        return blob_path # Retornamos la ruta relativa (la base se añade en el GET)
    except Exception as e:
        logging.error(f"Error subiendo a GCS: {str(e)}")
        return None

def extraer(request, headers):
    conn = get_connection()
    metodo = request.args.get("method")
    with conn:
        with conn.cursor() as cursor:
            if metodo == "listar_registros_combustible":
                sql = """
                    SELECT V.*, 
                           C.tipo_combustible, C.precio_total AS gasto_combustible, C.precio_unitario, C.url_comprobante AS foto_combustible,
                           P.monto_pagado AS gasto_cochera, P.url_comprobante AS foto_cochera
                    FROM REGISTRO_VIAJE V
                    LEFT JOIN DETALLE_COMBUSTIBLE C ON V.id_viaje = C.id_viaje
                    LEFT JOIN DETALLE_COCHERA P ON V.id_viaje = P.id_viaje
                    ORDER BY V.fecha_registro DESC
                """
                cursor.execute(sql)
                registros = cursor.fetchall()
                for reg in registros:
                    for campo in ['foto_combustible', 'foto_cochera']:
                        if reg[campo] and not reg[campo].startswith("http"):
                            reg[campo] = f"{BASE_STORAGE_URL}{reg[campo]}"
                return (json.dumps(registros, default=str), 200, headers)
    return (json.dumps({"error": "Método no válido"}), 405, headers)

def insert(request, headers):
    conn = get_connection()
    metodo = request.args.get("method")
    data = request.form 

    if metodo == "registrar_combustible_completo":
        try:
            # --- SUBIDA DIRECTA DE ARCHIVOS ---
            url_gas = None
            if "file_combustible" in request.files:
                url_gas = subir_a_gcs(request.files["file_combustible"])

            url_cochera = None
            if "file_cochera" in request.files:
                url_cochera = subir_a_gcs(request.files["file_cochera"])

            with conn.cursor() as cursor:
                # 1. Viaje
                sql_viaje = """
                    INSERT INTO REGISTRO_VIAJE (fecha, vehiculo, conductor, km_inicial, km_final, miembros_vehiculo, esta_limpio, en_buen_estado, descripcion_estado)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql_viaje, (
                    data.get("fecha"), data.get("vehiculo"), data.get("conductor"),
                    data.get("km_inicial"), data.get("km_final"), data.get("miembros_vehiculo"),
                    1 if data.get("esta_limpio") == "Si" else 0,
                    1 if data.get("en_buen_estado") == "Si" else 0,
                    data.get("descripcion_estado")
                ))
                id_viaje = cursor.lastrowid

                # 2. Combustible (Solo si marcó "Si")
                if data.get("lleno_combustible") == "Si":
                    sql_fuel = "INSERT INTO DETALLE_COMBUSTIBLE (id_viaje, tipo_combustible, precio_total, precio_unitario, url_comprobante) VALUES (%s, %s, %s, %s, %s)"
                    cursor.execute(sql_fuel, (id_viaje, data.get("tipo_combustible"), data.get("precio_total"), data.get("precio_unitario"), url_gas))

                # 3. Cochera (Solo si marcó "Si")
                if data.get("pago_cochera") == "Si":
                    sql_cochera = "INSERT INTO DETALLE_COCHERA (id_viaje, monto_pagado, url_comprobante) VALUES (%s, %s, %s)"
                    cursor.execute(sql_cochera, (id_viaje, data.get("monto_cochera"), url_cochera))

                conn.commit()
            
            return (json.dumps({"success": True, "id": id_viaje, "url_gas": url_gas}), 200, headers)
        except Exception as e:
            conn.rollback()
            return (json.dumps({"error": str(e)}), 500, headers)
        finally:
            conn.close()

@functions_framework.http
def hello_http(request):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,Authorization"
    }

    if request.method == "OPTIONS":
        return ("", 204, headers)

    # Validación de Token
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "No token"}), 401, headers)

    try:
        val_resp = requests.post(API_TOKEN, headers={"Authorization": auth_header}, timeout=10)
        if val_resp.status_code != 200:
            return (json.dumps({"error": "Token inválido"}), 401, headers)
    except:
        return (json.dumps({"error": "Error auth service"}), 503, headers)

    if request.method == "GET":
        return extraer(request, headers)
    if request.method == "POST":
        return insert(request, headers)
    
    return (json.dumps({"error": "Método inválido"}), 405, headers)