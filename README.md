# PPTO Capex Venezuela

API para procesamiento de archivos Excel de Prioridades de Pago (Venezuela). Limpia, transforma y carga datos hacia BigQuery y Google Cloud Storage.

## Arquitectura

El flujo de procesamiento se divide en **dos pasos**:

1. **Paso 1 - Limpiar**: Recibe el Excel crudo, detecta cabezales, limpia datos, calcula columnas adicionales (tasas de cambio, montos convertidos) y devuelve el archivo procesado en GCS `/tmp/`.
2. **Paso 2 - Upload**: Recibe el archivo procesado, lo monta en un template descargado desde GCS, sube los datos a BigQuery y guarda el Excel final en GCS `/logs/{fecha}/`.

## Características

- **Deteccion automatica de cabezales**: Itera por las filas del Excel para encontrar los cabezales esperados
- **Procesamiento con Pandas**: Limpieza, validacion y transformacion de datos usando DataFrames
- **Tasas de cambio en tiempo real**: Consulta tasas VES/USD (BCV), EUR/USD y COP/USD desde APIs externas
- **Integracion GCP**: Conexion a BigQuery, Google Cloud Storage y Google Sheets
- **Autenticacion flexible**: Soporta ADC (Application Default Credentials) o archivo `credentials.json`
- **Deploy en Cloud Run**: Script de despliegue automatizado con PowerShell

## Estructura del Proyecto

```
ppto_capex/
├── src/
│   ├── api.py             # Endpoints Flask, conexiones a GCP, helpers de GCS
│   ├── venezuela.py       # Logica de procesamiento del Excel (Paso 1 y Paso 2)
│   ├── connection.py      # Conexion a Google Sheets y subida a BigQuery
│   └── tasa.py            # Consulta de tasas de cambio (VES, EUR, COP -> USD)
├── resultados/            # Carpeta local para outputs
├── credentials.json       # Credenciales de GCP (opcional, no commitear)
├── .env                   # Variables de entorno
├── Dockerfile
├── docker-compose.yaml
├── deploy-ppto-capex-vzla.ps1  # Script de deploy a Cloud Run
└── requirements.txt
```

## Configuracion

### Variables de Entorno

Crea un archivo `.env` con las siguientes variables:

```env
GCP_PROJECT_ID=tu-proyecto-gcp
GCS_BUCKET_NAME=tu-bucket-gcs
BIGQUERY_DATASET=tu_dataset_bigquery
BIGQUERY_TABLE=tu_tabla_bigquery
GOOGLE_SHEET_ID=tu_google_sheet_id
GCS_TEMPLATE_PATH=template/vzla/Plantilla-VZLA-CAPEX-2526.xlsx
DEBUG=FALSE
```

| Variable | Descripcion |
|----------|-------------|
| `GCP_PROJECT_ID` | ID del proyecto en Google Cloud |
| `GCS_BUCKET_NAME` | Nombre del bucket de Cloud Storage |
| `BIGQUERY_DATASET` | Dataset de BigQuery donde se almacenan los datos |
| `BIGQUERY_TABLE` | Tabla de BigQuery destino |
| `GOOGLE_SHEET_ID` | ID del Google Sheet con datos de areas (AREAS VZLA) |
| `GCS_TEMPLATE_PATH` | Ruta del template Excel dentro del bucket |
| `DEBUG` | Modo debug (TRUE/FALSE) |

### Autenticacion con GCP

El proyecto usa el siguiente orden de prioridad para credenciales:

1. **ADC (Application Default Credentials)**: Si tienes `gcloud` configurado
   ```bash
   gcloud auth application-default login
   ```

2. **credentials.json**: Si ADC no esta disponible, busca el archivo en la raiz del proyecto

## Instalacion

### Local

```bash
# Crear entorno virtual
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar
cd src
python api.py
```

La API estara disponible en `http://localhost:9777`.

### Docker

```bash
# Construir y ejecutar con docker-compose
docker-compose up --build

# Solo construir
docker build -t ppto-capex-vzla .

# Ejecutar con variables de entorno
docker run -p 9777:9777 --env-file .env ppto-capex-vzla
```

### Deploy a Cloud Run

```powershell
# Ejecutar el script de despliegue (requiere gcloud CLI configurado)
.\deploy-ppto-capex-vzla.ps1
```

El script automatiza: configuracion del proyecto GCP, build con Cloud Build, deploy a Cloud Run y retorna la URL del servicio.

## Endpoints

| Metodo | Endpoint | Descripcion |
|--------|----------|-------------|
| GET | `/` | Informacion de la API y listado de endpoints |
| GET | `/health` | Health check con estado de configuracion |
| GET | `/test/bigquery` | Probar conexion a BigQuery |
| GET | `/test/gcs` | Probar conexion a Google Cloud Storage |
| GET | `/test/connections` | Probar todas las conexiones (BigQuery + GCS) |
| POST | `/process/prioridades-pago` | **Paso 1**: Limpiar archivo Excel |
| POST | `/process/prioridades-pago/upload` | **Paso 2**: Montar en template y subir a BigQuery |
| GET | `/logs` | Listar archivos procesados agrupados por fecha |

## Uso

### Probar conexiones

```bash
# Health check
curl http://localhost:9777/health

# Test BigQuery
curl http://localhost:9777/test/bigquery

# Test GCS
curl http://localhost:9777/test/gcs

# Test todas las conexiones
curl http://localhost:9777/test/connections
```

### Paso 1 - Limpiar archivo Excel

Envia el archivo Excel crudo. La API lo limpia, calcula columnas adicionales y devuelve la URL del archivo procesado en GCS.

```bash
curl -X POST http://localhost:9777/process/prioridades-pago \
  -F "file=@Prioridades de Pago.xlsx"
```

**Respuesta exitosa:**
```json
{
  "success": true,
  "message": "Archivo procesado correctamente (Paso 1)",
  "file_url": "https://storage.googleapis.com/bucket/tmp/archivo.xlsx",
  "file_name": "Prioridades_Pago_Procesado_20260210_120000.xlsx",
  "gcs_path": "gs://bucket/tmp/archivo.xlsx",
  "stats": { ... }
}
```

### Paso 2 - Montar en template y subir a BigQuery

Envia el archivo procesado del Paso 1. La API descarga el template desde GCS, monta los datos, sube a BigQuery y guarda el Excel final en logs.

```bash
curl -X POST http://localhost:9777/process/prioridades-pago/upload \
  -F "file=@Prioridades_Pago_Procesado.xlsx"
```

**Respuesta exitosa:**
```json
{
  "success": true,
  "message": "Datos montados en template y subidos a BigQuery (Paso 2)",
  "file_url": "https://storage.googleapis.com/bucket/logs/2026-02-10/archivo.xlsx",
  "file_name": "Prioridades_Pago_Final_20260210_120000.xlsx",
  "gcs_path": "gs://bucket/logs/2026-02-10/archivo.xlsx",
  "bigquery": { "success": true, "rows_uploaded": 150 },
  "stats": { ... }
}
```

### Listar logs

```bash
curl http://localhost:9777/logs
```

Retorna los archivos procesados agrupados por fecha, con URLs de descarga.

## Modulos

### `api.py`
Servidor Flask con todos los endpoints. Maneja conexiones a GCP (BigQuery, GCS), autenticacion, subida/descarga de archivos y orquestacion del flujo de procesamiento.

### `venezuela.py`
Logica de procesamiento del Excel de Prioridades de Pago:
- **`procesar_paso1()`**: Limpieza de datos, deteccion de cabezales, calculo de columnas adicionales (tasas de cambio, montos convertidos, moneda de pago, cuentas bancarias)
- **`procesar_paso2()`**: Montaje de datos procesados en el template Excel y preparacion para BigQuery

### `connection.py`
Conexion a servicios de Google:
- **`get_google_sheet_data()`**: Lee datos del Google Sheet (hoja "AREAS VZLA")
- **`upload_to_bigquery()`**: Sube DataFrame procesado a BigQuery con mapeo de columnas

### `tasa.py`
Consulta de tasas de cambio en tiempo real:
- **`obtener_tasa_bolivar_dolar()`**: Tasa VES/USD desde DolarAPI Venezuela (BCV y paralelo)
- **`obtener_tasa_euro_dolar()`**: Tasa EUR/USD desde Frankfurter API
- **`obtener_tasa_peso_colombiano_dolar()`**: Tasa COP/USD desde DolarAPI Colombia

## Stack Tecnologico

| Componente | Tecnologia |
|------------|-----------|
| Framework web | Flask 3.0 |
| Servidor WSGI | Gunicorn 21.2 |
| Procesamiento de datos | Pandas 2.1, NumPy |
| Manejo de Excel | openpyxl 3.1, XlsxWriter 3.1 |
| Google Cloud | BigQuery, Cloud Storage, Sheets |
| Tasas de cambio | DolarAPI, Frankfurter API |
| Contenedor | Docker (Python 3.11-slim) |
| Deploy | Google Cloud Run |
