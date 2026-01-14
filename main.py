import functions_framework
import pymysql
import json
import requests
import logging
from datetime import datetime
from google.cloud import storage

# ==========================================================
# CONFIGURACIÓN GLOBAL
# ==========================================================
API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"
BUCKET_NAME = "archivos_sistema"
# Base para construir el link cliqueable
BASE_URL_STORAGE = f"https://storage.googleapis.com/{BUCKET_NAME}/"

def get_connection():
    """Establece conexión con Cloud SQL"""
    return pymysql.connect(
        user="zeussafety-2024",
        password="ZeusSafety2025",
        db="Zeus_Safety_Data_Integration",
        unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
        cursorclass=pymysql.cursors.DictCursor
    )

def subir_a_gcs(archivo, folder="archivos_generales_zeus"):
    """
    Sube el archivo directamente a Google Cloud Storage 
    y retorna la URL PÚBLICA COMPLETA.
    """
    if not archivo or archivo.filename == '':
        return None
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        
        # Generar nombre único: timestamp + nombre sin espacios ni paréntesis
        timestamp = int(datetime.now().timestamp())
        nombre_limpio = archivo.filename.replace(" ", "_").replace("(", "").replace(")", "")
        blob_path = f"{folder}/{timestamp}_{nombre_limpio}"
        
        blob = bucket.blob(blob_path)
        
        # Asegurar que el puntero esté al inicio antes de leer
        archivo.seek(0)
        blob.upload_from_string(
            archivo.read(),
            content_type=archivo.content_type
        )
        
        # Retorna el link que necesitas: https://storage.googleapis.com/...
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
                    SELECT 
                        V.*, 
                        C.tipo_combustible, C.precio_total AS gasto_combustible, 
                        C.precio_unitario, C.url_comprobante AS foto_combustible,
                        P.monto_pagado AS gasto_cochera, P.url_comprobante AS foto_cochera
                    FROM REGISTRO_VIAJE V
                    LEFT JOIN DETALLE_COMBUSTIBLE C ON V.id_viaje = C.id_viaje
                    LEFT JOIN DETALLE_COCHERA P ON V.id_viaje = P.id_viaje
                    ORDER BY V.fecha_registro DESC
                """
                cursor.execute(sql)
                registros = cursor.fetchall()
                
                # Asegurar que las URLs en el listado también sean completas
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
    data = request.form # Datos de texto de form-data

    if metodo == "registrar_combustible_completo":
        try:
            # 1. Procesar archivos y obtener URLs COMPLETAS
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

                # B. Insertar en DETALLE_COMBUSTIBLE (si aplica)
                if data.get("lleno_combustible") == "Si":
                    sql_fuel = """
                        INSERT INTO DETALLE_COMBUSTIBLE (id_viaje, tipo_combustible, precio_total, precio_unitario, url_comprobante)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_fuel, (id_viaje, data.get("tipo_combustible"), data.get("precio_total"), data.get("precio_unitario"), url_combustible))

                # C. Insertar en DETALLE_COCHERA (si aplica)
                if data.get("pago_cochera") == "Si":
                    sql_cochera = """
                        INSERT INTO DETALLE_COCHERA (id_viaje, monto_pagado, url_comprobante)
                        VALUES (%s, %s, %s)
                    """
                    cursor.execute(sql_cochera, (id_viaje, data.get("monto_cochera"), url_cochera))

                conn.commit()
            
            # Respuesta con las llaves que necesitas y links completos
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
    # Configuración de CORS
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }

    if request.method == "OPTIONS":
        return ("", 204, headers)

    # Verificación de Token
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "No token"}), 401, headers)

    try:
        val_resp = requests.post(API_TOKEN, headers={"Authorization": auth_header}, timeout=10)
        if val_resp.status_code != 200:
            return (json.dumps({"error": "Token inválido"}), 401, headers)
    except Exception as e:
        return (json.dumps({"error": f"Error de autenticación: {str(e)}"}), 503, headers)

    # Router por método
    if request.method == "GET":
        return extraer(request, headers)
    elif request.method == "POST":
        return insert(request, headers)
    
    return (json.dumps({"error": "Method Not Allowed"}), 405, headers)