"""
API de procesamiento de Prioridades de Pago - Venezuela
Fase 2: Paso 1 (limpiar) y Paso 2 (montar en template + BigQuery)
"""
import os
from pathlib import Path
from datetime import datetime

import pytz
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Google Cloud imports
from google.cloud import bigquery
from google.cloud import storage
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account

# Procesamiento local
from venezuela import procesar_paso1, procesar_paso2
from connection import upload_to_bigquery

# Cargar variables de entorno
load_dotenv()

# Configuración desde environment
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_TEMPLATE_PATH = os.getenv('GCS_TEMPLATE_PATH', 'template/vzla/Plantilla-VZLA-CAPEX-2526.xlsx')
BQ_DATASET = os.getenv('BQ_DATASET')
BQ_TABLE = os.getenv('BQ_TABLE')

# Timezone de Caracas, Venezuela
TZ_CARACAS = pytz.timezone('America/Caracas')

# Path del archivo de credenciales
CREDENTIALS_PATH = Path(__file__).parent.parent / 'credentials.json'

app = Flask(__name__)


def get_credentials():
    """
    Obtiene credenciales usando ADC (Application Default Credentials).
    Si no está disponible, usa el archivo credentials.json.
    
    Returns:
        Credenciales de Google Cloud
    """
    try:
        # Intentar ADC primero
        credentials, project = default()
        print("[INFO] Usando Application Default Credentials (ADC)")
        return credentials, project or GCP_PROJECT_ID
    except DefaultCredentialsError:
        print("[INFO] ADC no disponible, buscando credentials.json...")
        
        if CREDENTIALS_PATH.exists():
            credentials = service_account.Credentials.from_service_account_file(
                str(CREDENTIALS_PATH)
            )
            print(f"[INFO] Usando credenciales desde: {CREDENTIALS_PATH}")
            return credentials, GCP_PROJECT_ID
        else:
            raise Exception(
                "No se encontraron credenciales. "
                "Configure ADC o proporcione credentials.json"
            )


def get_bigquery_client() -> bigquery.Client:
    """
    Crea y retorna un cliente de BigQuery.
    """
    credentials, project = get_credentials()
    return bigquery.Client(credentials=credentials, project=project)


def get_storage_client() -> storage.Client:
    """
    Crea y retorna un cliente de Google Cloud Storage.
    """
    credentials, project = get_credentials()
    return storage.Client(credentials=credentials, project=project)


# ============================================================================
# FUNCIONES HELPER DE GCS
# ============================================================================

def upload_to_gcs(content_bytes: bytes, gcs_path: str, content_type: str = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') -> dict:
    """
    Sube un archivo (bytes) a Google Cloud Storage.
    
    Args:
        content_bytes: Contenido del archivo en bytes
        gcs_path: Ruta dentro del bucket (ej: 'tmp/archivo.xlsx')
        content_type: Tipo MIME del archivo
        
    Returns:
        Dict con resultado de la operación
    """
    try:
        client = get_storage_client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        blob.upload_from_string(content_bytes, content_type=content_type)
        
        print(f"[GCS] Archivo subido: gs://{GCS_BUCKET_NAME}/{gcs_path} ({len(content_bytes)} bytes)")
        
        return {
            'success': True,
            'gcs_path': f"gs://{GCS_BUCKET_NAME}/{gcs_path}",
            'size_bytes': len(content_bytes)
        }
    except Exception as e:
        print(f"[GCS] ERROR al subir archivo: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }


def download_from_gcs(gcs_path: str) -> bytes:
    """
    Descarga un archivo desde Google Cloud Storage y retorna sus bytes.
    
    Args:
        gcs_path: Ruta dentro del bucket (ej: 'template/vzla/Plantilla.xlsx')
        
    Returns:
        bytes del archivo descargado
        
    Raises:
        Exception: Si no se puede descargar el archivo
    """
    client = get_storage_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    
    if not blob.exists():
        raise FileNotFoundError(
            f"Archivo no encontrado en GCS: gs://{GCS_BUCKET_NAME}/{gcs_path}"
        )
    
    content = blob.download_as_bytes()
    print(f"[GCS] Archivo descargado: gs://{GCS_BUCKET_NAME}/{gcs_path} ({len(content)} bytes)")
    
    return content


def clear_gcs_tmp() -> dict:
    """
    Borra todos los archivos dentro de la carpeta tmp/ del bucket en GCS.
    
    Returns:
        Dict con resultado de la operación
    """
    try:
        client = get_storage_client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blobs = list(bucket.list_blobs(prefix='tmp/'))
        
        deleted = 0
        for blob in blobs:
            blob.delete()
            deleted += 1
        
        print(f"[GCS] Carpeta tmp/ limpiada: {deleted} archivos eliminados")
        return {'success': True, 'deleted': deleted}
    except Exception as e:
        print(f"[GCS] ERROR al limpiar tmp/: {str(e)}")
        return {'success': False, 'error': str(e)}


def get_public_url(gcs_path: str) -> str:
    """
    Genera la URL pública de un archivo en GCS.
    
    Args:
        gcs_path: Ruta dentro del bucket (ej: 'tmp/archivo.xlsx')
        
    Returns:
        URL pública del archivo
    """
    return f"https://storage.googleapis.com/{GCS_BUCKET_NAME}/{gcs_path}"


def get_fecha_caracas() -> str:
    """
    Obtiene la fecha actual en timezone de Caracas, Venezuela.
    
    Returns:
        Fecha en formato YYYY-MM-DD
    """
    ahora = datetime.now(TZ_CARACAS)
    return ahora.strftime('%Y-%m-%d')


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.route('/')
def root():
    """Endpoint raíz con información de la API."""
    return jsonify({
        "name": "PPTO Capex Venezuela API",
        "version": "2.0.0",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "test_bigquery": "/test/bigquery",
            "test_gcs": "/test/gcs",
            "test_all": "/test/connections",
            "paso1_limpiar": "POST /process/prioridades-pago",
            "paso2_upload": "POST /process/prioridades-pago/upload"
        }
    })


@app.route('/health')
def health_check():
    """
    Health check endpoint para verificar que la API está funcionando.
    """
    return jsonify({
        "status": "healthy",
        "service": "ppto-capex-venezuela",
        "config": {
            "project_id": GCP_PROJECT_ID or "not_configured",
            "bucket": GCS_BUCKET_NAME or "not_configured",
            "dataset": BQ_DATASET or "not_configured",
            "table": BQ_TABLE or "not_configured"
        }
    })


@app.route('/test/bigquery')
def test_bigquery_connection():
    """
    Prueba la conexión a BigQuery ejecutando una query simple.
    """
    print("[INFO] Probando conexión a BigQuery...")
    
    try:
        client = get_bigquery_client()
        
        # Query simple para probar conexión
        query = "SELECT 1 as test_value"
        query_job = client.query(query)
        results = list(query_job.result())
        
        # Obtener información del dataset si está configurado
        dataset_info = None
        if BQ_DATASET:
            try:
                dataset_ref = client.dataset(BQ_DATASET)
                dataset = client.get_dataset(dataset_ref)
                dataset_info = {
                    "dataset_id": dataset.dataset_id,
                    "location": dataset.location,
                    "created": str(dataset.created)
                }
            except Exception as e:
                dataset_info = {"error": str(e)}
        
        print("[INFO] Conexión a BigQuery exitosa")
        
        return jsonify({
            "status": "connected",
            "service": "BigQuery",
            "project": client.project,
            "test_query_result": results[0].test_value if results else None,
            "dataset_info": dataset_info
        })
        
    except Exception as e:
        print(f"[ERROR] Error conectando a BigQuery: {str(e)}")
        return jsonify({
            "status": "error",
            "service": "BigQuery",
            "error": str(e)
        }), 500


@app.route('/test/gcs')
def test_gcs_connection():
    """
    Prueba la conexión a Google Cloud Storage listando buckets.
    """
    print("[INFO] Probando conexión a GCS...")
    
    try:
        client = get_storage_client()
        
        # Listar buckets para probar conexión
        buckets = list(client.list_buckets(max_results=5))
        bucket_names = [b.name for b in buckets]
        
        # Verificar bucket específico si está configurado
        bucket_info = None
        if GCS_BUCKET_NAME:
            try:
                bucket = client.get_bucket(GCS_BUCKET_NAME)
                bucket_info = {
                    "name": bucket.name,
                    "location": bucket.location,
                    "storage_class": bucket.storage_class
                }
            except Exception as e:
                bucket_info = {"error": str(e)}
        
        print("[INFO] Conexión a GCS exitosa")
        
        return jsonify({
            "status": "connected",
            "service": "Google Cloud Storage",
            "project": client.project,
            "buckets_found": len(bucket_names),
            "sample_buckets": bucket_names[:3],
            "configured_bucket": bucket_info
        })
        
    except Exception as e:
        print(f"[ERROR] Error conectando a GCS: {str(e)}")
        return jsonify({
            "status": "error",
            "service": "Google Cloud Storage",
            "error": str(e)
        }), 500


@app.route('/test/connections')
def test_all_connections():
    """
    Prueba todas las conexiones a servicios de GCP.
    """
    print("[INFO] Probando todas las conexiones...")
    
    results = {
        "bigquery": {"status": "pending"},
        "gcs": {"status": "pending"}
    }
    
    # Test BigQuery
    try:
        client = get_bigquery_client()
        query_job = client.query("SELECT 1")
        list(query_job.result())
        results["bigquery"] = {
            "status": "connected",
            "project": client.project
        }
        print("[INFO] BigQuery: OK")
    except Exception as e:
        results["bigquery"] = {
            "status": "error",
            "error": str(e)
        }
        print(f"[ERROR] BigQuery: {str(e)}")
    
    # Test GCS
    try:
        client = get_storage_client()
        list(client.list_buckets(max_results=1))
        results["gcs"] = {
            "status": "connected",
            "project": client.project
        }
        print("[INFO] GCS: OK")
    except Exception as e:
        results["gcs"] = {
            "status": "error",
            "error": str(e)
        }
        print(f"[ERROR] GCS: {str(e)}")
    
    # Estado general
    all_connected = all(
        r.get("status") == "connected" 
        for r in results.values()
    )
    
    return jsonify({
        "overall_status": "healthy" if all_connected else "degraded",
        "services": results
    })


@app.route('/process/prioridades-pago', methods=['POST'])
def process_prioridades_pago_paso1():
    """
    PASO 1: Procesa/limpia un archivo Excel de Prioridades de Pago.
    - Limpia el archivo, calcula columnas adicionales
    - Guarda el archivo procesado en GCS /tmp/
    - Retorna el Excel procesado como descarga
    
    NO sube a BigQuery (eso es Paso 2).
    """
    # Verificar que se envió un archivo
    if 'file' not in request.files:
        return jsonify({
            "error": "No se envió ningún archivo",
            "detail": "Debe enviar un archivo con el key 'file'"
        }), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({
            "error": "Nombre de archivo vacío"
        }), 400
    
    print(f"[PASO1] Recibido archivo: {file.filename}")
    
    # Validar extensión
    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({
            "error": "El archivo debe ser un Excel (.xlsx o .xls)"
        }), 400
    
    # Limpiar carpeta tmp/ en GCS antes de procesar
    clear_gcs_tmp()
    
    try:
        # Leer contenido del archivo
        content = file.read()
        print(f"[PASO1] Tamaño del archivo: {len(content)} bytes")
        
        # Obtener parámetros opcionales
        sheet_name = request.form.get('sheet_name', None)
        
        # Procesar con el módulo de Venezuela (Paso 1: solo limpiar)
        resultado = procesar_paso1(content, sheet_name)
        
        if not resultado['success']:
            return jsonify({
                "error": resultado.get('error', 'Error procesando archivo'),
                "step": "paso1"
            }), 422
        
        # Generar nombre de archivo procesado
        timestamp = datetime.now(TZ_CARACAS).strftime('%Y%m%d_%H%M%S')
        output_filename = f'Prioridades_Pago_Procesado_{timestamp}.xlsx'
        
        # Guardar en GCS /tmp/
        gcs_tmp_path = f"tmp/{output_filename}"
        gcs_result = upload_to_gcs(resultado['excel_bytes'], gcs_tmp_path)
        
        if not gcs_result['success']:
            return jsonify({
                "error": "Error guardando archivo en GCS",
                "detail": gcs_result.get('error'),
                "step": "paso1"
            }), 500
        
        # Generar URL pública del archivo
        public_url = get_public_url(gcs_tmp_path)
        
        print(f"[PASO1] Archivo guardado en GCS: {public_url}")
        
        return jsonify({
            "success": True,
            "message": "Archivo procesado correctamente (Paso 1)",
            "file_url": public_url,
            "file_name": output_filename,
            "gcs_path": gcs_result['gcs_path'],
            "stats": resultado.get('stats'),
        }), 200
            
    except Exception as e:
        print(f"[ERROR] Error en Paso 1: {str(e)}")
        return jsonify({
            "error": "Error procesando archivo",
            "detail": str(e),
            "step": "paso1"
        }), 500


@app.route('/process/prioridades-pago/upload', methods=['POST'])
def process_prioridades_pago_paso2():
    """
    PASO 2: Recibe el archivo procesado, lo monta en el template y sube a BigQuery.
    - Descarga el template desde GCS
    - Monta la data en la hoja 'Detalle' del template
    - Sube los datos a BigQuery
    - Guarda el Excel final en GCS /logs/{fecha_caracas}/
    - Retorna el Excel final como descarga
    """
    # Verificar que se envió un archivo
    if 'file' not in request.files:
        return jsonify({
            "error": "No se envió ningún archivo",
            "detail": "Debe enviar el archivo procesado del Paso 1 con el key 'file'"
        }), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({
            "error": "Nombre de archivo vacío"
        }), 400
    
    print(f"[PASO2] Recibido archivo: {file.filename}")
    
    # Validar extensión
    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({
            "error": "El archivo debe ser un Excel (.xlsx o .xls)"
        }), 400
    
    try:
        # Leer contenido del archivo procesado
        content = file.read()
        print(f"[PASO2] Tamaño del archivo: {len(content)} bytes")
        
        # Obtener parámetros opcionales
        sheet_name = request.form.get('sheet_name', None)
        
        # 1. Descargar template desde GCS
        print(f"[PASO2] Descargando template desde GCS: {GCS_TEMPLATE_PATH}")
        try:
            template_bytes = download_from_gcs(GCS_TEMPLATE_PATH)
        except FileNotFoundError as e:
            return jsonify({
                "error": "Template no encontrado en GCS",
                "detail": str(e),
                "gcs_path": f"gs://{GCS_BUCKET_NAME}/{GCS_TEMPLATE_PATH}"
            }), 404
        except Exception as e:
            return jsonify({
                "error": "Error descargando template desde GCS",
                "detail": str(e)
            }), 500
        
        # 2. Procesar con el módulo de Venezuela (Paso 2: montar en template)
        resultado = procesar_paso2(content, template_bytes, sheet_name)
        
        if not resultado['success']:
            return jsonify({
                "error": resultado.get('error', 'Error montando datos en template'),
                "step": "paso2"
            }), 422
        
        # 3. Subir datos a BigQuery
        df_procesado = resultado['df']
        print("[PASO2] Subiendo datos a BigQuery...")
        
        # Obtener tasas del DataFrame si están disponibles
        tasa_ves_usd = df_procesado.attrs.get('tasa_ves_usd', 0)
        tasa_ves_usd_mas_5 = df_procesado.attrs.get('tasa_ves_usd_mas_5', 0)
        tasa_eur_usd = df_procesado.attrs.get('tasa_eur_usd', 0)
        tasa_cop_usd = df_procesado.attrs.get('tasa_cop_usd', 0)
        
        bq_result = upload_to_bigquery(
            df=df_procesado,
            tasa_ves_usd=tasa_ves_usd,
            tasa_ves_usd_mas_5=tasa_ves_usd_mas_5,
            tasa_eur_usd=tasa_eur_usd,
            tasa_cop_usd=tasa_cop_usd,
            write_disposition='WRITE_APPEND'
        )
        
        if bq_result['success']:
            print(f"[PASO2] BigQuery: {bq_result['rows_uploaded']} filas subidas")
        else:
            print(f"[PASO2] BigQuery ERROR: {bq_result.get('error', 'Unknown error')}")
        
        # 4. Guardar en GCS /logs/{fecha_caracas}/
        fecha_caracas = get_fecha_caracas()
        timestamp = datetime.now(TZ_CARACAS).strftime('%Y%m%d_%H%M%S')
        output_filename = f'Prioridades_Pago_Final_{timestamp}.xlsx'
        gcs_logs_path = f"logs/{fecha_caracas}/{output_filename}"
        
        gcs_result = upload_to_gcs(resultado['excel_bytes'], gcs_logs_path)
        
        if not gcs_result['success']:
            return jsonify({
                "error": "Error guardando archivo en GCS logs",
                "detail": gcs_result.get('error'),
                "step": "paso2"
            }), 500
        
        # 5. Generar URL pública del archivo
        public_url = get_public_url(gcs_logs_path)
        
        print(f"[PASO2] Archivo guardado en GCS logs: {public_url}")
        
        return jsonify({
            "success": True,
            "message": "Datos montados en template y subidos a BigQuery (Paso 2)",
            "file_url": public_url,
            "file_name": output_filename,
            "gcs_path": gcs_result['gcs_path'],
            "bigquery": bq_result,
            "stats": resultado.get('stats'),
        }), 200
            
    except Exception as e:
        print(f"[ERROR] Error en Paso 2: {str(e)}")
        return jsonify({
            "error": "Error procesando archivo",
            "detail": str(e),
            "step": "paso2"
        }), 500


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Iniciando PPTO Capex Venezuela API - Fase 2")
    print("=" * 60)
    print(f"Project ID: {GCP_PROJECT_ID}")
    print(f"Bucket: {GCS_BUCKET_NAME}")
    print(f"Template: {GCS_TEMPLATE_PATH}")
    print(f"Dataset: {BQ_DATASET}")
    print(f"Table: {BQ_TABLE}")
    print("=" * 60)
    
    app.run(
        host="0.0.0.0",
        port=9777,
        debug=True
    )
