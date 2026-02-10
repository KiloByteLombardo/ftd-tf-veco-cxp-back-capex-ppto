"""
Módulo de conexión a Google Sheets y BigQuery
- Lee datos de un Google Sheet y los retorna como DataFrame
- Sube datos procesados a BigQuery
"""
import os
import pandas as pd
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
import google.auth

# Cargar variables de entorno
load_dotenv()

# Scopes necesarios para acceder a Google Sheets y BigQuery
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/bigquery'
]

# Nombre de la hoja a leer (hardcodeado)
SHEET_NAME = "AREAS VZLA"

# Configuración de BigQuery desde variables de entorno
BQ_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BQ_DATASET = os.getenv('BIGQUERY_DATASET')
BQ_TABLE = os.getenv('BIGQUERY_TABLE')

# ============================================================================
# MAPEO DE COLUMNAS: DataFrame -> BigQuery
# ============================================================================

# Mapeo de columnas del DataFrame a columnas de BigQuery
COLUMN_MAPPING = {
    'Numero de Factura': 'vzla_capex_ppto_numero_factura',
    'Numero de OC': 'vzla_capex_ppto_numero_oc',
    'Tipo factura': 'vzla_capex_ppto_tipo_factura',
    'Nombre Lote': 'vzla_capex_ppto_nombre_lote',
    'Proveedor': 'vzla_capex_ppto_proveedor',
    'RIF': 'vzla_capex_ppto_rif',
    'Fecha Documento': 'vzla_capex_ppto_fecha_documento',
    'Tienda': 'vzla_capex_ppto_tienda',
    'Sucursal': 'vzla_capex_ppto_sucursal',
    'Monto': 'vzla_capex_ppto_monto',
    'Moneda': 'vzla_capex_ppto_moneda',
    'Fecha Vencimiento': 'vzla_capex_ppto_fecha_vencimiento',
    'Cuenta': 'vzla_capex_ppto_cuenta',
    'CODIGO CTA': 'vzla_capex_ppto_codigo_cuenta',
    'METODO DE PAGO': 'vzla_capex_ppto_metodo_pago',
    'Pago Independiente': 'vzla_capex_ppto_pago_independiente',
    'Prioridad origen': 'vzla_capex_ppto_prioridad_origen',
    'Monto CAPEX EXT': 'vzla_capex_ppto_monto_capex_ext',
    'Monto CAPEX ORD': 'vzla_capex_ppto_monto_capex_ord',
    'Monto CADM': 'vzla_capex_ppto_monto_cadm',
    'Fecha Creación': 'vzla_capex_ppto_fecha_creacion',
    'Solicitante': 'vzla_capex_ppto_solicitante',
    'MONTO CAPEX ORD2': 'vzla_capex_ppto_monto_capex_ord_2',
    'MONTO CAPEX EXT3': 'vzla_capex_ppto_monto_capex_ext_3',
    'MONTO CAPEX FINAL': 'vzla_capex_ppto_monto_capex_final',
    'Moneda Pago': 'vzla_capex_ppto_moneda_pago',
    'Monto Final': 'vzla_capex_ppto_monto_final',
    'Cuenta Bancaria': 'vzla_capex_ppto_cuenta_bancaria',
    'Día de pago': 'vzla_capex_ppto_dia_pago',
    'MONTO CAPEX ORD USD': 'vzla_capex_ppto_monto_capex_ord_usd',
    'MONTO CAPEX EXT USD': 'vzla_capex_ppto_monto_capex_ext_usd',
    'Monto CAPEX USD': 'vzla_capex_ppto_monto_capex_usd',
    'Monto OPEX USD': 'vzla_capex_ppto_monto_opex_usd',
    'MONTO OPEX FINAL': 'vzla_capex_ppto_monto_opex_final',
    'MONTO TOTAL USD': 'vzla_capex_ppto_monto_total_usd',
    'ÁREA': 'vzla_capex_ppto_area',
    'TIPO CAPEX': 'vzla_capex_ppto_tipo_capex',
    'CAPEX': 'vzla_capex_ppto_tipo_capex_2'
}

# Esquema de BigQuery
BQ_SCHEMA = [
    bigquery.SchemaField('vzla_capex_ppto_numero_factura', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_numero_oc', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_tipo_factura', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_nombre_lote', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_proveedor', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_rif', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_fecha_documento', 'DATE'),
    bigquery.SchemaField('vzla_capex_ppto_tienda', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_sucursal', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_monto', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_moneda', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_fecha_vencimiento', 'DATE'),
    bigquery.SchemaField('vzla_capex_ppto_cuenta', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_codigo_cuenta', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_metodo_pago', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_pago_independiente', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_prioridad_origen', 'INTEGER'),
    bigquery.SchemaField('vzla_capex_ppto_monto_capex_ext', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_monto_capex_ord', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_monto_cadm', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_fecha_creacion', 'DATE'),
    bigquery.SchemaField('vzla_capex_ppto_solicitante', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_monto_capex_ord_2', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_monto_capex_ext_3', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_monto_capex_final', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_moneda_pago', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_monto_final', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_cuenta_bancaria', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_dia_pago', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_monto_capex_ord_usd', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_monto_capex_ext_usd', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_monto_capex_usd', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_monto_opex_usd', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_monto_opex_final', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_monto_total_usd', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_area', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_tipo_capex', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_tipo_capex_2', 'STRING'),
    bigquery.SchemaField('vzla_capex_ppto_tasa_bolivares', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_tasa_bolivares_jueves', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_tasa_euro', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_tasa_cop', 'FLOAT'),
    bigquery.SchemaField('vzla_capex_ppto_timestamp', 'TIMESTAMP'),
]


# ============================================================================
# FUNCIONES DE GOOGLE SHEETS
# ============================================================================

def get_google_sheet_data() -> pd.DataFrame:
    """
    Conecta a un Google Sheet y retorna los datos como DataFrame.
    
    Lee la hoja especificada en SHEET_NAME.
    
    Returns:
        pd.DataFrame: DataFrame con los datos del Google Sheet.
    
    Raises:
        ValueError: Si no se encuentra el GOOGLE_SHEET_ID.
        gspread.exceptions.SpreadsheetNotFound: Si el spreadsheet no existe.
        gspread.exceptions.WorksheetNotFound: Si la hoja no existe.
    """
    print("[CONN] Conectando a Google Sheets...")
    
    # Obtener el ID del Google Sheet desde las variables de entorno
    sheet_id = os.getenv('GOOGLE_SHEET_ID')
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID no encontrado en las variables de entorno (.env)")
    
    # Obtener credenciales de Google
    credentials = _get_credentials()
    
    # Autorizar cliente de gspread
    client = gspread.authorize(credentials)
    
    # Abrir el spreadsheet por ID
    spreadsheet = client.open_by_key(sheet_id)
    
    # Obtener la hoja por nombre
    worksheet = spreadsheet.worksheet(SHEET_NAME)
    
    # Obtener todos los datos
    data = worksheet.get_all_records()
    
    # Convertir a DataFrame
    df = pd.DataFrame(data)
    
    print(f"[CONN] Google Sheets: {len(df)} registros obtenidos de '{SHEET_NAME}'")
    
    return df


# ============================================================================
# FUNCIONES DE BIGQUERY
# ============================================================================

def get_bigquery_client() -> bigquery.Client:
    """
    Crea y retorna un cliente de BigQuery.
    
    Returns:
        bigquery.Client: Cliente autenticado de BigQuery.
    """
    credentials = _get_credentials_bigquery()
    return bigquery.Client(credentials=credentials, project=BQ_PROJECT_ID)


def _get_credentials_bigquery():
    """
    Obtiene credenciales para BigQuery.
    """
    # Opción 1: ADC
    try:
        credentials, project = google.auth.default(
            scopes=['https://www.googleapis.com/auth/bigquery']
        )
        return credentials
    except google.auth.exceptions.DefaultCredentialsError:
        pass
    
    # Opción 2: Archivo credentials.json
    project_root = os.path.dirname(os.path.dirname(__file__))
    creds_file = os.path.join(project_root, 'credentials.json')
    
    if os.path.exists(creds_file):
        return Credentials.from_service_account_file(
            creds_file, 
            scopes=['https://www.googleapis.com/auth/bigquery']
        )
    
    raise ValueError("No se encontraron credenciales para BigQuery")


def prepare_dataframe_for_bigquery(
    df: pd.DataFrame, 
    tasa_ves_usd: float = 0, 
    tasa_ves_usd_mas_5: float = 0,
    tasa_eur_usd: float = 0,
    tasa_cop_usd: float = 0
) -> pd.DataFrame:
    """
    Prepara el DataFrame para subir a BigQuery.
    - Renombra columnas según el mapeo
    - Convierte tipos de datos
    - Agrega columnas de tasas y timestamp
    
    Args:
        df: DataFrame procesado de venezuela.py
        tasa_ves_usd: Tasa VES/USD
        tasa_ves_usd_mas_5: Tasa VES/USD + 5
        tasa_eur_usd: Tasa EUR/USD
        tasa_cop_usd: Tasa COP/USD
        
    Returns:
        DataFrame listo para BigQuery
    """
    print("[CONN-BQ] Preparando DataFrame para BigQuery...")
    
    df_bq = df.copy()
    
    # Renombrar columnas según el mapeo
    columns_to_rename = {}
    for df_col, bq_col in COLUMN_MAPPING.items():
        if df_col in df_bq.columns:
            columns_to_rename[df_col] = bq_col
    
    df_bq = df_bq.rename(columns=columns_to_rename)
    
    # Convertir columnas de fecha a datetime
    date_columns = [
        'vzla_capex_ppto_fecha_documento',
        'vzla_capex_ppto_fecha_vencimiento',
        'vzla_capex_ppto_fecha_creacion'
    ]
    
    for col in date_columns:
        if col in df_bq.columns:
            df_bq[col] = pd.to_datetime(df_bq[col], errors='coerce').dt.date
    
    # Convertir columnas numéricas a float
    float_columns = [
        'vzla_capex_ppto_monto',
        'vzla_capex_ppto_monto_capex_ext',
        'vzla_capex_ppto_monto_capex_ord',
        'vzla_capex_ppto_monto_cadm',
        'vzla_capex_ppto_monto_capex_ord_2',
        'vzla_capex_ppto_monto_capex_ext_3',
        'vzla_capex_ppto_monto_capex_final',
        'vzla_capex_ppto_monto_final',
        'vzla_capex_ppto_monto_capex_ord_usd',
        'vzla_capex_ppto_monto_capex_ext_usd',
        'vzla_capex_ppto_monto_capex_usd',
        'vzla_capex_ppto_monto_opex_usd',
        'vzla_capex_ppto_monto_opex_final',
        'vzla_capex_ppto_monto_total_usd'
    ]
    
    for col in float_columns:
        if col in df_bq.columns:
            df_bq[col] = pd.to_numeric(df_bq[col], errors='coerce').fillna(0)
    
    # Convertir Prioridad a INTEGER
    if 'vzla_capex_ppto_prioridad_origen' in df_bq.columns:
        df_bq['vzla_capex_ppto_prioridad_origen'] = pd.to_numeric(
            df_bq['vzla_capex_ppto_prioridad_origen'], errors='coerce'
        ).fillna(0).astype(int)
    
    # Convertir columnas STRING
    string_columns = [
        'vzla_capex_ppto_numero_factura',
        'vzla_capex_ppto_numero_oc',
        'vzla_capex_ppto_tipo_factura',
        'vzla_capex_ppto_nombre_lote',
        'vzla_capex_ppto_proveedor',
        'vzla_capex_ppto_rif',
        'vzla_capex_ppto_tienda',
        'vzla_capex_ppto_sucursal',
        'vzla_capex_ppto_moneda',
        'vzla_capex_ppto_cuenta',
        'vzla_capex_ppto_codigo_cuenta',
        'vzla_capex_ppto_metodo_pago',
        'vzla_capex_ppto_pago_independiente',
        'vzla_capex_ppto_solicitante',
        'vzla_capex_ppto_moneda_pago',
        'vzla_capex_ppto_cuenta_bancaria',
        'vzla_capex_ppto_dia_pago',
        'vzla_capex_ppto_area',
        'vzla_capex_ppto_tipo_capex',
        'vzla_capex_ppto_tipo_capex_2'
    ]
    
    for col in string_columns:
        if col in df_bq.columns:
            df_bq[col] = df_bq[col].astype(str).replace('nan', '').replace('None', '')
    
    # Agregar columnas de tasas
    df_bq['vzla_capex_ppto_tasa_bolivares'] = tasa_ves_usd
    df_bq['vzla_capex_ppto_tasa_bolivares_jueves'] = tasa_ves_usd_mas_5
    df_bq['vzla_capex_ppto_tasa_euro'] = tasa_eur_usd
    df_bq['vzla_capex_ppto_tasa_cop'] = tasa_cop_usd
    
    # Agregar timestamp
    df_bq['vzla_capex_ppto_timestamp'] = datetime.now()
    
    # Seleccionar solo las columnas del esquema
    schema_columns = [field.name for field in BQ_SCHEMA]
    existing_columns = [col for col in schema_columns if col in df_bq.columns]
    
    # Agregar columnas faltantes con valores por defecto
    for col in schema_columns:
        if col not in df_bq.columns:
            df_bq[col] = None
    
    df_bq = df_bq[schema_columns]
    
    print(f"[CONN-BQ] DataFrame preparado: {len(df_bq)} filas, {len(df_bq.columns)} columnas")
    
    return df_bq


def upload_to_bigquery(
    df: pd.DataFrame,
    tasa_ves_usd: float = 0,
    tasa_ves_usd_mas_5: float = 0,
    tasa_eur_usd: float = 0,
    tasa_cop_usd: float = 0,
    write_disposition: str = 'WRITE_TRUNCATE'
) -> Dict:
    """
    Sube el DataFrame procesado a BigQuery.
    
    Args:
        df: DataFrame procesado de venezuela.py (THREAD-DF)
        tasa_ves_usd: Tasa VES/USD
        tasa_ves_usd_mas_5: Tasa VES/USD + 5
        tasa_eur_usd: Tasa EUR/USD
        tasa_cop_usd: Tasa COP/USD
        write_disposition: Modo de escritura:
            - 'WRITE_TRUNCATE': Reemplaza todos los datos
            - 'WRITE_APPEND': Agrega a los datos existentes
            - 'WRITE_EMPTY': Solo escribe si la tabla está vacía
    
    Returns:
        Dict con el resultado de la operación
    """
    print("[CONN-BQ] Iniciando upload a BigQuery...")
    
    if not BQ_PROJECT_ID or not BQ_DATASET or not BQ_TABLE:
        return {
            'success': False,
            'error': 'Faltan variables de entorno: GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE',
            'rows_uploaded': 0
        }
    
    try:
        # Preparar DataFrame
        df_bq = prepare_dataframe_for_bigquery(
            df, tasa_ves_usd, tasa_ves_usd_mas_5, tasa_eur_usd, tasa_cop_usd
        )
        
        # Obtener cliente de BigQuery
        client = get_bigquery_client()
        
        # Referencia a la tabla
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
        
        print(f"[CONN-BQ] Tabla destino: {table_id}")
        
        # Configurar el job de carga
        job_config = bigquery.LoadJobConfig(
            schema=BQ_SCHEMA,
            write_disposition=write_disposition,
        )
        
        # Subir datos
        print(f"[CONN-BQ] Subiendo {len(df_bq)} filas...")
        
        job = client.load_table_from_dataframe(
            df_bq, 
            table_id, 
            job_config=job_config
        )
        
        # Esperar a que termine
        job.result()
        
        # Obtener resultado
        table = client.get_table(table_id)
        
        print(f"[CONN-BQ] Upload completado: {table.num_rows} filas en la tabla")
        
        return {
            'success': True,
            'table_id': table_id,
            'rows_uploaded': len(df_bq),
            'total_rows_in_table': table.num_rows,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"[CONN-BQ] ERROR: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'rows_uploaded': 0
        }


def test_bigquery_connection() -> Dict:
    """
    Prueba la conexión a BigQuery.
    
    Returns:
        Dict con el resultado del test
    """
    print("[CONN-BQ] Probando conexión a BigQuery...")
    
    try:
        client = get_bigquery_client()
        
        # Query de prueba
        query = "SELECT 1 as test"
        job = client.query(query)
        result = list(job.result())
        
        # Verificar tabla si está configurada
        table_info = None
        if BQ_PROJECT_ID and BQ_DATASET and BQ_TABLE:
            try:
                table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
                table = client.get_table(table_id)
                table_info = {
                    'table_id': table_id,
                    'num_rows': table.num_rows,
                    'created': str(table.created)
                }
            except Exception as e:
                table_info = {'error': str(e)}
        
        print("[CONN-BQ] Conexión exitosa")
        
        return {
            'success': True,
            'project': client.project,
            'test_query': 'OK',
            'table_info': table_info
        }
        
    except Exception as e:
        print(f"[CONN-BQ] ERROR: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }


# ============================================================================
# FUNCIONES DE CREDENCIALES
# ============================================================================

def _get_credentials() -> Credentials:
    """
    Obtiene las credenciales de Google para Sheets.
    
    Busca credenciales en el siguiente orden:
    1. ADC (Application Default Credentials)
    2. Archivo 'credentials.json' en el directorio raíz del proyecto
    
    Returns:
        Credentials: Credenciales autorizadas para Google APIs.
    
    Raises:
        ValueError: Si no se encuentran credenciales válidas.
    """
    # Opción 1: Application Default Credentials (ADC)
    try:
        credentials, project = google.auth.default(scopes=SCOPES)
        return credentials
    except google.auth.exceptions.DefaultCredentialsError:
        pass
    
    # Opción 2: Archivo credentials.json en el directorio raíz del proyecto
    project_root = os.path.dirname(os.path.dirname(__file__))
    creds_file = os.path.join(project_root, 'credentials.json')
    
    if os.path.exists(creds_file):
        return Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    
    raise ValueError(
        "No se encontraron credenciales de Google.\n"
        "Configura ADC o coloca 'credentials.json' en el directorio raíz del proyecto."
    )


# ============================================================================
# MAIN (para pruebas)
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Probando conexiones")
    print("=" * 60)
    
    print("\n1. Probando BigQuery:")
    bq_result = test_bigquery_connection()
    print(f"   Resultado: {bq_result}")
    
    print("\n2. Probando Google Sheets:")
    try:
        df = get_google_sheet_data()
        print(f"   Resultado: {len(df)} filas obtenidas")
    except Exception as e:
        print(f"   Error: {str(e)}")
    
    print("\n" + "=" * 60)
