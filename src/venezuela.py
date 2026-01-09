"""
Módulo de procesamiento de archivos Excel de Prioridades de Pago - Venezuela
Implementa procesamiento paralelo con Threads
"""
import pandas as pd
import numpy as np
import threading
from typing import Optional, Tuple, List, Dict, Any
from io import BytesIO
from pathlib import Path
from datetime import datetime
import os
import json

# Configuración de carpeta de resultados
RESULTADOS_PATH = Path(__file__).parent.parent / 'resultados'

# Cabezales esperados del archivo Prioridades de Pago
CABEZALES_ESPERADOS = [
    'Numero de Factura',
    'Numero de OC',
    'Tipo Factura',
    'Nombre Lote',
    'Proveedor',
    'RIF',
    'Fecha Documento',
    'Tienda',
    'Sucursal',
    'Monto',
    'Moneda',
    'Fecha Vencimiento',
    'Cuenta',
    'Banco',
    'Id Cta',
    'Método de Pago',
    'Pago Independiente',
    'Prioridad',
    'Monto CAPEX EXT',
    'Monto CAPEX ORD',
    'Monto CADM',
    'Fecha Creación',
    'Solicitante',
    'Proveedor Remito'
]


class ResultadoThread:
    """Clase para almacenar resultados de los threads."""
    def __init__(self):
        self.dataframe_result: Optional[Dict] = None
        self.excel_result: Optional[Dict] = None
        self.dataframe_error: Optional[str] = None
        self.excel_error: Optional[str] = None


def dataframe_a_json_serializable(df: pd.DataFrame) -> List[Dict]:
    """
    Convierte un DataFrame a una lista de diccionarios serializables a JSON.
    Maneja correctamente valores NaT, NaN, Timestamp, etc.
    
    Args:
        df: DataFrame a convertir
        
    Returns:
        Lista de diccionarios serializables
    """
    def convertir_valor(val):
        """Convierte un valor individual a formato serializable."""
        if pd.isna(val):
            return None
        elif isinstance(val, pd.Timestamp):
            return val.isoformat()
        elif isinstance(val, datetime):
            return val.isoformat()
        elif isinstance(val, np.integer):
            return int(val)
        elif isinstance(val, np.floating):
            return float(val) if not np.isnan(val) else None
        elif isinstance(val, np.ndarray):
            return val.tolist()
        else:
            return val
    
    registros = []
    for _, row in df.iterrows():
        registro = {}
        for col in df.columns:
            registro[col] = convertir_valor(row[col])
        registros.append(registro)
    
    return registros


# ============================================================================
# FUNCIONES DE PROCESAMIENTO DE DATAFRAME (Thread 1)
# ============================================================================

def encontrar_cabezales(df_raw: pd.DataFrame, max_filas_busqueda: int = 20) -> Tuple[int, List[str]]:
    """
    Encuentra automáticamente la fila de cabezales iterando por las filas del archivo.
    Busca coincidencias con los cabezales esperados.
    """
    print("[THREAD-DF] Buscando cabezales automáticamente...")
    
    for idx in range(min(max_filas_busqueda, len(df_raw))):
        fila = df_raw.iloc[idx]
        valores = [str(v).strip() if pd.notna(v) else '' for v in fila]
        
        # Buscar coincidencias con cabezales esperados
        coincidencias = sum(1 for v in valores if v in CABEZALES_ESPERADOS)
        
        if coincidencias >= 5:  # Al menos 5 cabezales coinciden
            print(f"[THREAD-DF] Cabezales encontrados en fila {idx} ({coincidencias} coincidencias)")
            return idx, valores
    
    # Fallback: buscar fila con más valores string no vacíos
    for idx in range(min(max_filas_busqueda, len(df_raw))):
        fila = df_raw.iloc[idx]
        valores_validos = [v for v in fila.dropna() if str(v).strip() != '']
        
        if len(valores_validos) >= 10:
            strings_count = sum(1 for v in valores_validos if isinstance(v, str))
            if strings_count >= len(valores_validos) * 0.5:
                cabezales = [str(v).strip() if pd.notna(v) else f'Columna_{i}' 
                            for i, v in enumerate(fila)]
                print(f"[THREAD-DF] Cabezales encontrados en fila {idx} (fallback)")
                return idx, cabezales
    
    print("[THREAD-DF] WARN: No se encontraron cabezales, usando fila 0")
    return 0, list(df_raw.columns)


def leer_excel_con_cabezales(file_content: bytes, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """
    Lee un archivo Excel y detecta automáticamente los cabezales.
    """
    print("[THREAD-DF] Leyendo archivo Excel...")
    
    df_raw = pd.read_excel(
        BytesIO(file_content),
        sheet_name=sheet_name or 0,
        header=None
    )
    
    print(f"[THREAD-DF] Archivo leído: {df_raw.shape[0]} filas x {df_raw.shape[1]} columnas")
    
    header_idx, cabezales = encontrar_cabezales(df_raw)
    
    df = pd.read_excel(
        BytesIO(file_content),
        sheet_name=sheet_name or 0,
        header=header_idx
    )
    
    print(f"[THREAD-DF] DataFrame con cabezales: {df.shape[0]} filas x {df.shape[1]} columnas")
    
    return df


def limpiar_datos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y normaliza los datos del DataFrame.
    """
    print("[THREAD-DF] Limpiando datos...")
    
    # Eliminar filas completamente vacías
    df_limpio = df.dropna(how='all')
    filas_eliminadas = len(df) - len(df_limpio)
    
    if filas_eliminadas > 0:
        print(f"[THREAD-DF] Eliminadas {filas_eliminadas} filas vacías")
    
    # Eliminar columnas completamente vacías
    df_limpio = df_limpio.dropna(axis=1, how='all')
    
    # Limpiar nombres de columnas
    df_limpio.columns = [
        str(col).strip().replace('\n', ' ').replace('\r', '')
        for col in df_limpio.columns
    ]
    
    # Eliminar columnas sin nombre útil
    cols_a_mantener = [col for col in df_limpio.columns if not col.startswith('Unnamed')]
    df_limpio = df_limpio[cols_a_mantener]
    
    print(f"[THREAD-DF] Datos limpios: {df_limpio.shape[0]} filas x {df_limpio.shape[1]} columnas")
    
    return df_limpio


def procesar_dataframe_thread(file_content: bytes, sheet_name: Optional[str], resultado: ResultadoThread):
    """
    Thread 1: Procesa la data en DataFrame.
    """
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Iniciando procesamiento de DataFrame...")
    
    try:
        # Leer y procesar
        df = leer_excel_con_cabezales(file_content, sheet_name)
        df_limpio = limpiar_datos(df)
        
        # Calcular estadísticas
        stats = {
            'total_filas': len(df_limpio),
            'total_columnas': len(df_limpio.columns),
            'columnas': list(df_limpio.columns),
            'montos': {}
        }
        
        # Calcular sumas de montos si existen las columnas
        columnas_monto = ['Monto', 'Monto CAPEX EXT', 'Monto CAPEX ORD', 'Monto CADM']
        for col in columnas_monto:
            if col in df_limpio.columns:
                try:
                    stats['montos'][col] = float(pd.to_numeric(df_limpio[col], errors='coerce').sum())
                except:
                    stats['montos'][col] = 0
        
        resultado.dataframe_result = {
            'success': True,
            'stats': stats,
            'data': dataframe_a_json_serializable(df_limpio),
            'df': df_limpio  # Guardar el DataFrame para el thread de Excel
        }
        
        print(f"[{thread_name}] Procesamiento de DataFrame completado: {stats['total_filas']} registros")
        
    except Exception as e:
        resultado.dataframe_error = str(e)
        print(f"[{thread_name}] ERROR: {str(e)}")


# ============================================================================
# FUNCIONES DE GENERACIÓN DE EXCEL (Thread 2)
# ============================================================================

def crear_excel_con_formulas(df: pd.DataFrame, output_path: Path) -> Dict[str, Any]:
    """
    Crea un archivo Excel con la hoja 'Detalle' y fórmulas calculadas.
    """
    print("[THREAD-EXCEL] Creando Excel con fórmulas...")
    
    # Crear el archivo Excel con xlsxwriter para poder agregar fórmulas
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        # Escribir datos en hoja 'Detalle'
        df.to_excel(writer, sheet_name='Detalle', index=False, startrow=0)
        
        workbook = writer.book
        worksheet = writer.sheets['Detalle']
        
        # Formatos
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#4472C4',
            'font_color': 'white',
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'
        })
        
        money_format = workbook.add_format({
            'num_format': '#,##0.00',
            'border': 1
        })
        
        date_format = workbook.add_format({
            'num_format': 'dd/mm/yyyy',
            'border': 1
        })
        
        text_format = workbook.add_format({
            'border': 1
        })
        
        formula_format = workbook.add_format({
            'bold': True,
            'bg_color': '#E2EFDA',
            'num_format': '#,##0.00',
            'border': 1
        })
        
        # Aplicar formato a cabezales
        for col_num, col_name in enumerate(df.columns):
            worksheet.write(0, col_num, col_name, header_format)
        
        # Ajustar ancho de columnas
        for col_num, col_name in enumerate(df.columns):
            max_length = max(len(str(col_name)), 12)
            worksheet.set_column(col_num, col_num, max_length + 2)
        
        # Agregar columnas con fórmulas al final
        num_filas = len(df)
        num_cols = len(df.columns)
        fila_inicio_datos = 2  # Fila 2 en Excel (1-indexed, después del header)
        fila_fin_datos = num_filas + 1
        
        # Columna: Total CAPEX (suma de CAPEX EXT + CAPEX ORD)
        col_total_capex = num_cols
        worksheet.write(0, col_total_capex, 'Total CAPEX', header_format)
        
        # Encontrar índices de columnas de monto
        col_indices = {col: idx for idx, col in enumerate(df.columns)}
        
        capex_ext_col = col_indices.get('Monto CAPEX EXT')
        capex_ord_col = col_indices.get('Monto CAPEX ORD')
        cadm_col = col_indices.get('Monto CADM')
        monto_col = col_indices.get('Monto')
        
        # Escribir fórmulas para cada fila
        for row in range(1, num_filas + 1):
            excel_row = row + 1  # Ajustar para Excel (1-indexed)
            
            # Fórmula Total CAPEX
            if capex_ext_col is not None and capex_ord_col is not None:
                capex_ext_letter = chr(65 + capex_ext_col) if capex_ext_col < 26 else f"{chr(64 + capex_ext_col // 26)}{chr(65 + capex_ext_col % 26)}"
                capex_ord_letter = chr(65 + capex_ord_col) if capex_ord_col < 26 else f"{chr(64 + capex_ord_col // 26)}{chr(65 + capex_ord_col % 26)}"
                formula = f'=IF(ISNUMBER({capex_ext_letter}{excel_row}),{capex_ext_letter}{excel_row},0)+IF(ISNUMBER({capex_ord_letter}{excel_row}),{capex_ord_letter}{excel_row},0)'
                worksheet.write_formula(row, col_total_capex, formula, money_format)
        
        # Columna: Total General (Monto + CAPEX EXT + CAPEX ORD + CADM)
        col_total_general = num_cols + 1
        worksheet.write(0, col_total_general, 'Total General', header_format)
        
        for row in range(1, num_filas + 1):
            excel_row = row + 1
            
            formula_parts = []
            for col_name, col_idx in [('Monto', monto_col), ('Monto CAPEX EXT', capex_ext_col), 
                                       ('Monto CAPEX ORD', capex_ord_col), ('Monto CADM', cadm_col)]:
                if col_idx is not None:
                    col_letter = chr(65 + col_idx) if col_idx < 26 else f"{chr(64 + col_idx // 26)}{chr(65 + col_idx % 26)}"
                    formula_parts.append(f'IF(ISNUMBER({col_letter}{excel_row}),{col_letter}{excel_row},0)')
            
            if formula_parts:
                formula = '=' + '+'.join(formula_parts)
                worksheet.write_formula(row, col_total_general, formula, money_format)
        
        # Columna: Días Vencimiento (diferencia entre hoy y Fecha Vencimiento)
        fecha_venc_col = col_indices.get('Fecha Vencimiento')
        if fecha_venc_col is not None:
            col_dias_venc = num_cols + 2
            worksheet.write(0, col_dias_venc, 'Días Vencimiento', header_format)
            
            fecha_venc_letter = chr(65 + fecha_venc_col) if fecha_venc_col < 26 else f"{chr(64 + fecha_venc_col // 26)}{chr(65 + fecha_venc_col % 26)}"
            
            for row in range(1, num_filas + 1):
                excel_row = row + 1
                formula = f'=IF(ISNUMBER({fecha_venc_letter}{excel_row}),TODAY()-{fecha_venc_letter}{excel_row},"")'
                worksheet.write_formula(row, col_dias_venc, formula, text_format)
        
        # Fila de totales al final
        fila_totales = num_filas + 2
        worksheet.write(fila_totales, 0, 'TOTALES', header_format)
        
        # Agregar fórmulas de suma para columnas de monto
        columnas_suma = ['Monto', 'Monto CAPEX EXT', 'Monto CAPEX ORD', 'Monto CADM']
        for col_name in columnas_suma:
            if col_name in col_indices:
                col_idx = col_indices[col_name]
                col_letter = chr(65 + col_idx) if col_idx < 26 else f"{chr(64 + col_idx // 26)}{chr(65 + col_idx % 26)}"
                formula = f'=SUM({col_letter}2:{col_letter}{num_filas + 1})'
                worksheet.write_formula(fila_totales, col_idx, formula, formula_format)
        
        # Suma para columnas de fórmula
        total_capex_letter = chr(65 + col_total_capex) if col_total_capex < 26 else f"{chr(64 + col_total_capex // 26)}{chr(65 + col_total_capex % 26)}"
        total_general_letter = chr(65 + col_total_general) if col_total_general < 26 else f"{chr(64 + col_total_general // 26)}{chr(65 + col_total_general % 26)}"
        
        worksheet.write_formula(fila_totales, col_total_capex, 
                               f'=SUM({total_capex_letter}2:{total_capex_letter}{num_filas + 1})', 
                               formula_format)
        worksheet.write_formula(fila_totales, col_total_general, 
                               f'=SUM({total_general_letter}2:{total_general_letter}{num_filas + 1})', 
                               formula_format)
        
        # Freeze panes (fijar encabezado)
        worksheet.freeze_panes(1, 0)
        
        print(f"[THREAD-EXCEL] Excel creado con {num_filas} filas y {num_cols + 3} columnas")
    
    return {
        'file_path': str(output_path),
        'file_name': output_path.name,
        'filas': num_filas,
        'columnas_originales': num_cols,
        'columnas_con_formulas': num_cols + 3
    }


def procesar_excel_thread(file_content: bytes, sheet_name: Optional[str], resultado: ResultadoThread):
    """
    Thread 2: Crea el Excel con la hoja 'Detalle' y fórmulas.
    Espera a que el Thread 1 complete para usar el DataFrame procesado.
    """
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Iniciando generación de Excel...")
    
    try:
        # Leer datos (mismo proceso que Thread 1 pero independiente)
        df = leer_excel_con_cabezales(file_content, sheet_name)
        df_limpio = limpiar_datos(df)
        
        # Crear carpeta de resultados si no existe
        RESULTADOS_PATH.mkdir(parents=True, exist_ok=True)
        
        # Generar nombre de archivo con timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f'Prioridades_Pago_Procesado_{timestamp}.xlsx'
        output_path = RESULTADOS_PATH / output_filename
        
        # Crear Excel con fórmulas
        excel_info = crear_excel_con_formulas(df_limpio, output_path)
        
        resultado.excel_result = {
            'success': True,
            'excel_info': excel_info
        }
        
        print(f"[{thread_name}] Excel generado: {output_path}")
        
    except Exception as e:
        resultado.excel_error = str(e)
        print(f"[{thread_name}] ERROR: {str(e)}")


# ============================================================================
# FUNCIÓN PRINCIPAL (MAIN)
# ============================================================================

def procesar_prioridades_pago(file_content: bytes, sheet_name: Optional[str] = None) -> dict:
    """
    Función principal que procesa el archivo de Prioridades de Pago.
    Usa dos threads paralelos:
      - Thread 1: Procesa la data en DataFrame
      - Thread 2: Crea el Excel con hoja 'Detalle' y fórmulas
    
    Args:
        file_content: Contenido del archivo Excel en bytes
        sheet_name: Nombre de la hoja (opcional)
        
    Returns:
        Diccionario con el resultado del procesamiento
    """
    print("=" * 70)
    print("[MAIN] Iniciando procesamiento de Prioridades de Pago - Venezuela")
    print("[MAIN] Modo: Threading paralelo (2 hilos)")
    print("=" * 70)
    
    # Objeto para almacenar resultados de ambos threads
    resultado = ResultadoThread()
    
    # Crear los threads
    thread_dataframe = threading.Thread(
        target=procesar_dataframe_thread,
        args=(file_content, sheet_name, resultado),
        name="THREAD-DF"
    )
    
    thread_excel = threading.Thread(
        target=procesar_excel_thread,
        args=(file_content, sheet_name, resultado),
        name="THREAD-EXCEL"
    )
    
    print("[MAIN] Iniciando threads...")
    
    # Iniciar ambos threads
    thread_dataframe.start()
    thread_excel.start()
    
    # Esperar a que ambos terminen
    thread_dataframe.join()
    print("[MAIN] Thread DataFrame completado")
    
    thread_excel.join()
    print("[MAIN] Thread Excel completado")
    
    print("=" * 70)
    
    # Verificar errores
    errores = []
    if resultado.dataframe_error:
        errores.append(f"DataFrame: {resultado.dataframe_error}")
    if resultado.excel_error:
        errores.append(f"Excel: {resultado.excel_error}")
    
    if errores:
        print(f"[MAIN] Procesamiento completado con errores: {errores}")
        return {
            'success': False,
            'error': 'Errores durante el procesamiento',
            'detalles': errores,
            'data': None
        }
    
    # Combinar resultados
    print("[MAIN] Procesamiento completado exitosamente")
    
    return {
        'success': True,
        'message': 'Archivo procesado correctamente con threading',
        'dataframe': {
            'stats': resultado.dataframe_result['stats'],
            'filas_procesadas': resultado.dataframe_result['stats']['total_filas']
        },
        'excel': resultado.excel_result['excel_info'],
        'data': resultado.dataframe_result['data']
    }


# ============================================================================
# FUNCIÓN PARA OBTENER SOLO EL DATAFRAME
# ============================================================================

def obtener_dataframe(file_content: bytes, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """
    Obtiene el DataFrame procesado y limpio, listo para usar.
    Esta función NO usa threading, es para uso directo.
    
    Args:
        file_content: Contenido del archivo Excel en bytes
        sheet_name: Nombre de la hoja (opcional)
        
    Returns:
        DataFrame procesado
    """
    df = leer_excel_con_cabezales(file_content, sheet_name)
    df_limpio = limpiar_datos(df)
    return df_limpio
