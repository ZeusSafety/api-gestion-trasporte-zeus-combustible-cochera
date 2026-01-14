import functions_framework
import pymysql
import json
import requests
import logging

# 🔧 Configuración de Endpoints y Almacenamiento
API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"
API_SUBIDA_ARCHIVOS = "https://api-subida-archivos-2946605267.us-central1.run.app"
BASE_STORAGE_URL = "https://storage.googleapis.com/archivos_sistema/"

def get_connection():
    """Establece la conexión con Cloud SQL"""
    return pymysql.connect(
        user="zeussafety-2024",
        password="ZeusSafety2025",
        db="Zeus_Safety_Data_Integration",
        unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
        cursorclass=pymysql.cursors.DictCursor
    )

def subir_comprobante(archivo):
    """Sube el archivo a la API de archivos y retorna la URL completa"""
    try:
        params = {
            "bucket_name": "archivos_sistema",
            "folder_bucket": "archivos_generales_zeus",
            "method": "no_encriptar"
        }
        files = {"archivo": (archivo.filename, archivo.read(), archivo.content_type)}
        response = requests.post(API_SUBIDA_ARCHIVOS, params=params, files=files, timeout=20)
        
        if response.status_code == 200:
            url_recibida = response.json().get("url")
            # Si recibimos solo la ruta, completamos con el dominio de Google
            if url_recibida and not url_recibida.startswith("http"):
                return f"{BASE_STORAGE_URL}{url_recibida}"
            return url_recibida
        return None
    except Exception as e:
        logging.error(f"Error en subida de archivo: {str(e)}")
        return None

def extraer(request, headers):
    """Consulta las tablas con JOIN y devuelve datos unificados"""
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

                # Limpieza de URLs para que sean clickeables en el Frontend/Postman
                for reg in registros:
                    for campo in ['foto_combustible', 'foto_cochera']:
                        if reg[campo] and not reg[campo].startswith("http"):
                            reg[campo] = f"{BASE_STORAGE_URL}{reg[campo]}"
                
                return (json.dumps(registros, default=str), 200, headers)
            
            return (json.dumps({"error": "Método GET no válido"}), 405, headers)

def insert(request, headers):
    """Procesa el formulario y distribuye los datos en las 3 tablas"""
    conn = get_connection()
    metodo = request.args.get("method")
    data = request.form 

    # Función interna para validar respuestas afirmativas
    def es_si(valor):
        return str(valor).strip().lower() == "si"

    with conn:
        with conn.cursor() as cursor:
            if metodo == "registrar_combustible_completo":
                try:
                    # 1. Tabla Principal: REGISTRO_VIAJE
                    sql_viaje = """
                        INSERT INTO REGISTRO_VIAJE (
                            fecha, vehiculo, conductor, km_inicial, km_final, 
                            miembros_vehiculo, esta_limpio, en_buen_estado, descripcion_estado
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_viaje, (
                        data.get("fecha"), data.get("vehiculo"), data.get("conductor"),
                        data.get("km_inicial"), data.get("km_final"), data.get("miembros_vehiculo"),
                        1 if es_si(data.get("esta_limpio")) else 0,
                        1 if es_si(data.get("en_buen_estado")) else 0,
                        data.get("descripcion_estado")
                    ))
                    id_viaje = cursor.lastrowid

                    # 2. Tabla Secundaria: DETALLE_COMBUSTIBLE
                    if es_si(data.get("lleno_combustible")):
                        foto_gas = None
                        if "file_combustible" in request.files:
                            foto_gas = subir_comprobante(request.files["file_combustible"])
                        
                        sql_fuel = """
                            INSERT INTO DETALLE_COMBUSTIBLE (id_viaje, tipo_combustible, precio_total, precio_unitario, url_comprobante)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        cursor.execute(sql_fuel, (id_viaje, data.get("tipo_combustible"), data.get("precio_total"), data.get("precio_unitario"), foto_gas))

                    # 3. Tabla Secundaria: DETALLE_COCHERA
                    if es_si(data.get("pago_cochera")):
                        foto_cochera = None
                        if "file_cochera" in request.files:
                            foto_cochera = subir_comprobante(request.files["file_cochera"])

                        sql_cochera = """
                            INSERT INTO DETALLE_COCHERA (id_viaje, monto_pagado, url_comprobante)
                            VALUES (%s, %s, %s)
                        """
                        cursor.execute(sql_cochera, (id_viaje, data.get("monto_cochera"), foto_cochera))

                    conn.commit()
                    return (json.dumps({"success": "Registro guardado correctamente", "id": id_viaje}), 200, headers)

                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error en transacción: {str(e)}")
                    return (json.dumps({"error": str(e)}), 500, headers)
            
            return (json.dumps({"error": "Método POST no válido"}), 405, headers)

@functions_framework.http
def hello_http(request):
    """Punto de entrada principal con CORS y Auth"""
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,Authorization"
    }

    if request.method == "OPTIONS":
        return ("", 200, headers)

    # Verificación de Seguridad (Token)
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "Falta cabecera de autorización"}), 401, headers)

    try:
        val_resp = requests.post(API_TOKEN, headers={"Authorization": auth_header}, timeout=10)
        if val_resp.status_code != 200:
            return (json.dumps({"error": "Token no autorizado"}), 401, headers)
    except Exception:
        return (json.dumps({"error": "Servicio de autenticación no disponible"}), 503, headers)

    # Enrutamiento según el método HTTP
    if request.method == "GET":
        return extraer(request, headers)
    elif request.method == "POST":
        return insert(request, headers)
    
    return (json.dumps({"error": "Método no permitido"}), 405, headers)