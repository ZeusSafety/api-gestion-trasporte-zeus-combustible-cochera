import functions_framework
import pymysql
import json
import requests
import logging

# 🔧 Configuración de APIs y Storage
API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"
API_SUBIDA_ARCHIVOS = "https://api-subida-archivos-2946605267.us-central1.run.app"
# Base URL para que los links en el GET sean clickeables
BASE_STORAGE_URL = "https://storage.googleapis.com/archivos_sistema/"

def get_connection():
    return pymysql.connect(
        user="zeussafety-2024",
        password="ZeusSafety2025",
        db="Zeus_Safety_Data_Integration",
        unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
        cursorclass=pymysql.cursors.DictCursor
    )

def subir_comprobante(archivo):
    """ Sube el archivo y retorna la URL completa """
    try:
        params = {
            "bucket_name": "archivos_sistema",
            "folder_bucket": "archivos_generales_zeus",
            "method": "no_encriptar"
        }
        files = {"archivo": (archivo.filename, archivo.read(), archivo.content_type)}
        response = requests.post(API_SUBIDA_ARCHIVOS, params=params, files=files, timeout=15)
        
        if response.status_code == 200:
            url_recibida = response.json().get("url")
            # Si la API devuelve solo la ruta, le agregamos el prefijo de Google
            if url_recibida and not url_recibida.startswith("http"):
                return f"{BASE_STORAGE_URL}{url_recibida}"
            return url_recibida
        return None
    except Exception as e:
        logging.error(f"Error en subida: {str(e)}")
        return None

def extraer(request, headers):
    conn = get_connection()
    metodo = request.args.get("method")

    with conn:
        with conn.cursor() as cursor:
            if metodo == "listar_registros_combustible":
                sql = """
                    SELECT 
                        V.*, 
                        C.tipo_combustible, C.precio_total AS gasto_combustible, C.precio_unitario, C.url_comprobante AS foto_combustible,
                        P.monto_pagado AS gasto_cochera, P.url_comprobante AS foto_cochera
                    FROM REGISTRO_VIAJE V
                    LEFT JOIN DETALLE_COMBUSTIBLE C ON V.id_viaje = C.id_viaje
                    LEFT JOIN DETALLE_COCHERA P ON V.id_viaje = P.id_viaje
                    ORDER BY V.fecha_registro DESC
                """
                cursor.execute(sql)
                registros = cursor.fetchall()

                # Asegurar links completos para registros antiguos en la DB
                for reg in registros:
                    for campo in ['foto_combustible', 'foto_cochera']:
                        if reg[campo] and not reg[campo].startswith("http"):
                            reg[campo] = f"{BASE_STORAGE_URL}{reg[campo]}"
                
                return (json.dumps(registros, default=str), 200, headers)
            
            return (json.dumps({"error": "Método GET no reconocido"}), 405, headers)

def insert(request, headers):
    conn = get_connection()
    metodo = request.args.get("method")
    data = request.form 

    with conn:
        with conn.cursor() as cursor:
            if metodo == "registrar_combustible_completo":
                try:
                    # 1. Insertar Viaje Principal
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

                    # 2. Lógica de Combustible
                    if data.get("lleno_combustible") == "Si":
                        url_gas = None
                        if "file_combustible" in request.files:
                            url_gas = subir_comprobante(request.files["file_combustible"])
                        
                        sql_fuel = """
                            INSERT INTO DETALLE_COMBUSTIBLE (id_viaje, tipo_combustible, precio_total, precio_unitario, url_comprobante)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        cursor.execute(sql_fuel, (id_viaje, data.get("tipo_combustible"), data.get("precio_total"), data.get("precio_unitario"), url_gas))

                    # 3. Lógica de Cochera
                    if data.get("pago_cochera") == "Si":
                        url_cochera = None
                        if "file_cochera" in request.files:
                            url_cochera = subir_comprobante(request.files["file_cochera"])

                        sql_cochera = """
                            INSERT INTO DETALLE_COCHERA (id_viaje, monto_pagado, url_comprobante)
                            VALUES (%s, %s, %s)
                        """
                        cursor.execute(sql_cochera, (id_viaje, data.get("monto_cochera"), url_cochera))

                    conn.commit()
                    return (json.dumps({"success": "Guardado correctamente", "id": id_viaje}), 200, headers)

                except Exception as e:
                    conn.rollback()
                    return (json.dumps({"error": str(e)}), 500, headers)
            
            return (json.dumps({"error": "Método POST no reconocido"}), 405, headers)

@functions_framework.http
def hello_http(request):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,Authorization"
    }

    if request.method == "OPTIONS":
        return ("", 200, headers)

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
    elif request.method == "POST":
        return insert(request, headers)
    
    return (json.dumps({"error": "Invalid Method"}), 405, headers)