import functions_framework
import pymysql
import json
import requests
import logging
import smtplib
from email.message import EmailMessage
from datetime import datetime
from google.cloud import storage

# ==========================================================
# CONFIGURACIÓN GLOBAL
# ==========================================================
API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"
BUCKET_NAME = "archivos_sistema"
BASE_URL_STORAGE = f"https://storage.googleapis.com/{BUCKET_NAME}/"

# CREDENCIALES DE CORREO (Configuradas con tu imagen)
SMTP_USER = "zeusadmi24@gmail.com" 
SMTP_PASS = "ufcfyqzgmxahbrfa"  # Tu contraseña de aplicación generada
DESTINATARIO_ALERTA = "zeusadmi24@gmail.com"

def get_connection():
    """Establece conexión con Cloud SQL"""
    return pymysql.connect(
        user="zeussafety-2024",
        password="ZeusSafety2025",
        db="Zeus_Safety_Data_Integration",
        unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
        cursorclass=pymysql.cursors.DictCursor
    )

def enviar_correo_alerta(kilometraje, vehiculo):
    """Envía la notificación de alerta basada en la lógica de Apps Script"""
    try:
        msg = EmailMessage()
        msg['Subject'] = "Alerta de Kilometraje-Cambio de Aceite"
        msg['From'] = SMTP_USER
        msg['To'] = DESTINATARIO_ALERTA
        
        # Cuerpo del mensaje idéntico al sistema antiguo
        contenido = (f"El kilometraje ha alcanzado o se acerca al umbral. "
                     f"El kilometraje actual es: {kilometraje} para el vehículo {vehiculo}")
        msg.set_content(contenido)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(msg)
        logging.info(f"Correo de alerta enviado con éxito para el vehículo {vehiculo}")
    except Exception as e:
        logging.error(f"Error al enviar correo: {e}")

def subir_a_gcs(archivo, folder="archivos_generales_zeus"):
    """Sube archivos a Google Cloud Storage"""
    if not archivo or archivo.filename == '':
        return None
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        timestamp = int(datetime.now().timestamp())
        nombre_limpio = archivo.filename.replace(" ", "_").replace("(", "").replace(")", "")
        blob_path = f"{folder}/{timestamp}_{nombre_limpio}"
        blob = bucket.blob(blob_path)
        archivo.seek(0)
        blob.upload_from_string(archivo.read(), content_type=archivo.content_type)
        return f"{BASE_URL_STORAGE}{blob_path}"
    except Exception as e:
        logging.error(f"Error subiendo archivo a GCS: {str(e)}")
        return None

# ==========================================================
# HANDLER PARA GET (LISTAR)
# ==========================================================
def extraer(request, headers):
    conn = get_connection()
    metodo = request.args.get("method")
    
    with conn:
        with conn.cursor() as cursor:
            if metodo == "listar_registros_combustible":
                sql = """
                    SELECT V.*, C.tipo_combustible, C.precio_total AS gasto_combustible, 
                           C.precio_unitario, C.url_comprobante AS foto_combustible,
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
                            reg[campo] = f"{BASE_URL_STORAGE}{reg[campo]}"
                return (json.dumps(registros, default=str), 200, headers)
    return (json.dumps({"error": "Método GET no reconocido"}), 404, headers)

# ==========================================================
# HANDLER PARA POST (INSERTAR)
# ==========================================================
def insert(request, headers):
    conn = get_connection()
    metodo = request.args.get("method")
    data = request.form 

    if metodo == "registrar_combustible_completo":
        try:
            url_combustible = None
            if "file_combustible" in request.files:
                url_combustible = subir_a_gcs(request.files["file_combustible"])

            url_cochera = None
            if "file_cochera" in request.files:
                url_cochera = subir_a_gcs(request.files["file_cochera"])

            with conn.cursor() as cursor:
                # A. Insertar en REGISTRO_VIAJE
                sql_viaje = """
                    INSERT INTO REGISTRO_VIAJE (
                        fecha, vehiculo, conductor, km_inicial, km_final, 
                        miembros_vehiculo, esta_limpio, en_buen_estado, descripcion_estado
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql_viaje, (
                    data.get("fecha"), data.get("vehiculo"), data.get("conductor"),
                    data.get("km_inicial"), data.get("km_final"), data.get("miembros_vehiculo"),
                    1 if data.get("esta_limpio") == "Si" else 0,
                    1 if data.get("en_buen_estado") == "Si" else 0,
                    data.get("descripcion_estado")
                ))
                id_viaje = cursor.lastrowid

                # B. Insertar en DETALLE_COMBUSTIBLE
                if data.get("lleno_combustible") == "Si":
                    sql_fuel = """
                        INSERT INTO DETALLE_COMBUSTIBLE (id_viaje, tipo_combustible, precio_total, precio_unitario, url_comprobante)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_fuel, (id_viaje, data.get("tipo_combustible"), data.get("precio_total"), data.get("precio_unitario"), url_combustible))

                # C. Insertar en DETALLE_COCHERA
                if data.get("pago_cochera") == "Si":
                    sql_cochera = """
                        INSERT INTO DETALLE_COCHERA (id_viaje, monto_pagado, url_comprobante)
                        VALUES (%s, %s, %s)
                    """
                    cursor.execute(sql_cochera, (id_viaje, data.get("monto_cochera"), url_cochera))

                conn.commit()

            # --- LÓGICA DE ALERTA PREVENTIVA (REPLICA DE APPS SCRIPT) ---
            try:
                # Convertimos a float para poder comparar matemáticamente
                mileage = float(data.get("km_final", 0))
                # Ajusta estos valores según la imagen 1 o 2 (ej. 17000 o 90)
                threshold = 90  
                alert_threshold = 20 
                
                # Condición: si está dentro del rango preventivo
                if mileage >= (threshold - alert_threshold):
                    enviar_correo_alerta(mileage, data.get("vehiculo"))
            except Exception as e:
                logging.error(f"Error evaluando alerta de kilometraje: {e}")

            return (json.dumps({
                "success": True,
                "id_viaje": id_viaje,
                "url_foto_combustible": url_combustible,
                "url_foto_cochera": url_cochera
            }), 200, headers)

        except Exception as e:
            conn.rollback()
            logging.error(f"Error en insert: {str(e)}")
            return (json.dumps({"error": str(e)}), 500, headers)
        finally:
            conn.close()
    
    return (json.dumps({"error": "Método POST no reconocido"}), 404, headers)

# ==========================================================
# FUNCIÓN PRINCIPAL (ENTRY POINT)
# ==========================================================
@functions_framework.http
def hello_http(request):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }

    if request.method == "OPTIONS":
        return ("", 204, headers)

    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "No token"}), 401, headers)

    try:
        val_resp = requests.post(API_TOKEN, headers={"Authorization": auth_header}, timeout=10)
        if val_resp.status_code != 200:
            return (json.dumps({"error": "Token inválido"}), 401, headers)
    except Exception as e:
        return (json.dumps({"error": f"Error de autenticación: {str(e)}"}), 503, headers)

    if request.method == "GET":
        return extraer(request, headers)
    elif request.method == "POST":
        return insert(request, headers)
    
    return (json.dumps({"error": "Method Not Allowed"}), 405, headers)