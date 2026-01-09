"""
Módulo de conexión a Google Sheets
Lee datos de un Google Sheet y los retorna como DataFrame
"""
import os
import pandas as pd
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
import google.auth

# Cargar variables de entorno
load_dotenv()

# Scopes necesarios para acceder a Google Sheets
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly'
]

# Nombre de la hoja a leer (hardcodeado)
SHEET_NAME = "AREAS VZLA"  # <-- Cambia aquí el nombre de la hoja


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
    
    return df


def _get_credentials() -> Credentials:
    """
    Obtiene las credenciales de Google.
    
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
