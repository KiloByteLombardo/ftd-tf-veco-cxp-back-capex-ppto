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

# Importar funciones de tasa de cambio
from tasa import obtener_tasa_bolivar_dolar, obtener_tasa_euro_dolar

# Importar conexión a Google Sheets para tabla de áreas
from connection import get_google_sheet_data

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

# ============================================================================
# ARRAYS DE PRIORIDADES PARA CÁLCULOS
# ============================================================================

# Prioridades que mantienen USD como moneda de pago (no se convierten a VES)
PRIORIDADES_USD_MONEDA_PAGO = [69, 70, 71, 72, 73, 74, 75, 76, 77, 87, 86, 88, 84, 85]

# Prioridades que mantienen la cuenta original cuando la moneda es USD
# (incluye 83 adicional para cuenta bancaria)
PRIORIDADES_USD_CUENTA_ORIGINAL = [69, 70, 71, 72, 73, 74, 75, 76, 77, 87, 86, 88, 83, 84, 85]

# Cuenta por defecto cuando USD no está en las prioridades especiales
CUENTA_USD_DEFAULT = "1111"

# Prioridades que mantienen el monto original sin conversión (para Monto Final)
# Si la prioridad está en este array, el monto no se multiplica por la tasa
PRIORIDADES_MONTO_SIN_CONVERSION = [67, 69, 70, 71, 72, 73, 74, 75, 76, 77, 87, 86, 88, 83, 84, 85, 89]

# Margen adicional para tasa de día JUEVES (tasa + 5)
MARGEN_TASA_JUEVES = 5

# ============================================================================
# CONFIGURACIÓN PARA COLUMNA AREA (RECARGAS)
# ============================================================================

# Proveedores que siempre son RECARGAS
PROVEEDORES_SIEMPRE_RECARGAS = [
    "GALAXY ENTERTAINMENT DE VENEZUELA, C.A. (SIMPLE TV )",
    "RECARGAS MOVIL C.A"
]

# Proveedores con condiciones de sucursal para ser RECARGAS
PROVEEDORES_RECARGAS_CONDICIONAL = {
    "CORPORACION DIGITEL, C.A.": ["POSPAGO FACTURA", "PREPAGO RECARGA"],
    "NETUNO, C.A.": ["RECARGAS"],
    "TELEFÓNICA VENEZOLANA, C.A.": ["RECARGAS"],
    "TELEFONICA VENEZOLANA, C.A.": ["RECARGAS"]  # Sin acento por si acaso
}


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


def calcular_columnas_adicionales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula las 3 columnas adicionales: Moneda Pago, Cuenta Bancaria, Dia de Pago.
    
    Args:
        df: DataFrame con los datos limpios
        
    Returns:
        DataFrame con las columnas adicionales
    """
    print("[THREAD-DF] Calculando columnas adicionales...")
    
    df_result = df.copy()
    
    # Obtener columnas necesarias (con manejo de valores nulos)
    moneda = df_result.get('Moneda', pd.Series([''] * len(df_result)))
    prioridad = pd.to_numeric(df_result.get('Prioridad', pd.Series([0] * len(df_result))), errors='coerce').fillna(0).astype(int)
    cuenta = df_result.get('Cuenta', pd.Series([''] * len(df_result)))
    
    # ========================================================================
    # COLUMNA 1: Moneda Pago
    # Lógica: EUR->EUR, COP->COP, USD y prioridad en array->USD, sino->VES
    # ========================================================================
    def calcular_moneda_pago(row_moneda, row_prioridad):
        if pd.isna(row_moneda):
            return 'VES'
        row_moneda = str(row_moneda).strip().upper()
        row_prioridad = int(row_prioridad) if pd.notna(row_prioridad) else 0
        
        if row_moneda == 'EUR':
            return 'EUR'
        elif row_moneda == 'COP':
            return 'COP'
        elif row_moneda == 'USD':
            if row_prioridad in PRIORIDADES_USD_MONEDA_PAGO:
                return 'USD'
            else:
                return 'VES'
        else:
            return 'VES'
    
    df_result['Moneda Pago'] = [
        calcular_moneda_pago(m, p) 
        for m, p in zip(moneda, prioridad)
    ]
    print(f"[THREAD-DF] Columna 'Moneda Pago' calculada")
    
    # ========================================================================
    # COLUMNA 2: Cuenta Bancaria
    # Lógica: Si USD y prioridad en array -> cuenta original, si USD -> "1111", sino -> cuenta original
    # ========================================================================
    def calcular_cuenta_bancaria(row_moneda, row_prioridad, row_cuenta):
        if pd.isna(row_moneda):
            return row_cuenta if pd.notna(row_cuenta) else ''
        row_moneda = str(row_moneda).strip().upper()
        row_prioridad = int(row_prioridad) if pd.notna(row_prioridad) else 0
        cuenta_val = str(row_cuenta) if pd.notna(row_cuenta) else ''
        
        if row_moneda == 'USD':
            if row_prioridad in PRIORIDADES_USD_CUENTA_ORIGINAL:
                return cuenta_val
            else:
                return CUENTA_USD_DEFAULT
        else:
            return cuenta_val
    
    df_result['Cuenta Bancaria'] = [
        calcular_cuenta_bancaria(m, p, c) 
        for m, p, c in zip(moneda, prioridad, cuenta)
    ]
    print(f"[THREAD-DF] Columna 'Cuenta Bancaria' calculada")
    
    # ========================================================================
    # COLUMNA 3: Dia de Pago
    # Lógica: Si Moneda Pago es USD o EUR -> VIERNES, sino -> JUEVES
    # ========================================================================
    def calcular_dia_pago(moneda_pago):
        if pd.isna(moneda_pago):
            return 'JUEVES'
        moneda_pago = str(moneda_pago).strip().upper()
        
        if moneda_pago in ['USD', 'EUR']:
            return 'VIERNES'
        else:
            return 'JUEVES'
    
    df_result['Dia de Pago'] = [
        calcular_dia_pago(mp) 
        for mp in df_result['Moneda Pago']
    ]
    print(f"[THREAD-DF] Columna 'Dia de Pago' calculada")
    
    # ========================================================================
    # OBTENER TASA DE CAMBIO VES/USD
    # ========================================================================
    print("[THREAD-DF] Obteniendo tasa de cambio VES/USD...")
    tasa_info = obtener_tasa_bolivar_dolar()
    
    if tasa_info['success'] and tasa_info['tasa']:
        tasa_ves_usd = float(tasa_info['tasa'])
        tasa_ves_usd_mas_5 = tasa_ves_usd + MARGEN_TASA_JUEVES
        print(f"[THREAD-DF] Tasa VES/USD: {tasa_ves_usd}, Tasa + 5: {tasa_ves_usd_mas_5}")
    else:
        # Tasa por defecto si falla la consulta
        tasa_ves_usd = 36.50
        tasa_ves_usd_mas_5 = 41.50
        print(f"[THREAD-DF] WARN: Usando tasa por defecto: {tasa_ves_usd}")
    
    # Obtener columnas necesarias para Monto Final
    monto = pd.to_numeric(df_result.get('Monto', pd.Series([0] * len(df_result))), errors='coerce').fillna(0)
    capex_ext = pd.to_numeric(df_result.get('Monto CAPEX EXT', pd.Series([0] * len(df_result))), errors='coerce').fillna(0)
    capex_ord = pd.to_numeric(df_result.get('Monto CAPEX ORD', pd.Series([0] * len(df_result))), errors='coerce').fillna(0)
    cadm = pd.to_numeric(df_result.get('Monto CADM', pd.Series([0] * len(df_result))), errors='coerce').fillna(0)
    
    # ========================================================================
    # COLUMNA 4: Monto Final
    # Lógica:
    # - Si Moneda = "VES" → Monto
    # - Si Prioridad está en PRIORIDADES_MONTO_SIN_CONVERSION → Monto
    # - Si Dia de Pago = "JUEVES" → Monto * tasa_ves_usd_mas_5
    # - Sino → Monto * tasa_ves_usd
    # ========================================================================
    def calcular_monto_final(row_moneda, row_prioridad, row_monto, row_dia_pago):
        if pd.isna(row_monto) or row_monto == 0:
            return 0
        
        row_moneda = str(row_moneda).strip().upper() if pd.notna(row_moneda) else ''
        row_prioridad = int(row_prioridad) if pd.notna(row_prioridad) else 0
        row_dia_pago = str(row_dia_pago).strip().upper() if pd.notna(row_dia_pago) else 'JUEVES'
        
        # Si moneda es VES, no hay conversión
        if row_moneda == 'VES':
            return float(row_monto)
        
        # Si prioridad está en el array, no hay conversión
        if row_prioridad in PRIORIDADES_MONTO_SIN_CONVERSION:
            return float(row_monto)
        
        # Aplicar tasa según día de pago
        if row_dia_pago == 'JUEVES':
            return float(row_monto) * tasa_ves_usd_mas_5
        else:
            return float(row_monto) * tasa_ves_usd
    
    df_result['Monto Final'] = [
        calcular_monto_final(m, p, mto, dp) 
        for m, p, mto, dp in zip(moneda, prioridad, monto, df_result['Dia de Pago'])
    ]
    print(f"[THREAD-DF] Columna 'Monto Final' calculada")
    
    # Obtener Monto Final como serie para cálculos siguientes
    monto_final = pd.to_numeric(df_result['Monto Final'], errors='coerce').fillna(0)
    
    # ========================================================================
    # COLUMNA 5: Monto Capex Final
    # Lógica:
    # - Si (CAPEX EXT = 0 Y CAPEX ORD = 0) → 0
    # - Sino → ((CAPEX EXT + CAPEX ORD) / (CAPEX EXT + CAPEX ORD + CADM)) * Monto Final
    # ========================================================================
    def calcular_monto_capex_final(row_capex_ext, row_capex_ord, row_cadm, row_monto_final):
        capex_ext_val = float(row_capex_ext) if pd.notna(row_capex_ext) else 0
        capex_ord_val = float(row_capex_ord) if pd.notna(row_capex_ord) else 0
        cadm_val = float(row_cadm) if pd.notna(row_cadm) else 0
        monto_final_val = float(row_monto_final) if pd.notna(row_monto_final) else 0
        
        # Si ambos CAPEX son 0, retornar 0
        if capex_ext_val == 0 and capex_ord_val == 0:
            return 0
        
        # Calcular total
        total = capex_ext_val + capex_ord_val + cadm_val
        
        if total == 0:
            return 0
        
        # Proporción de CAPEX sobre el total
        proporcion_capex = (capex_ext_val + capex_ord_val) / total
        return proporcion_capex * monto_final_val
    
    df_result['Monto Capex Final'] = [
        calcular_monto_capex_final(ce, co, ca, mf) 
        for ce, co, ca, mf in zip(capex_ext, capex_ord, cadm, monto_final)
    ]
    print(f"[THREAD-DF] Columna 'Monto Capex Final' calculada")
    
    # ========================================================================
    # COLUMNA 6: Monto Opex Final
    # Lógica:
    # - Si (CAPEX EXT = 0 Y CAPEX ORD = 0) → Monto Final
    # - Sino → (CADM / (CAPEX EXT + CAPEX ORD + CADM)) * Monto Final
    # ========================================================================
    def calcular_monto_opex_final(row_capex_ext, row_capex_ord, row_cadm, row_monto_final):
        capex_ext_val = float(row_capex_ext) if pd.notna(row_capex_ext) else 0
        capex_ord_val = float(row_capex_ord) if pd.notna(row_capex_ord) else 0
        cadm_val = float(row_cadm) if pd.notna(row_cadm) else 0
        monto_final_val = float(row_monto_final) if pd.notna(row_monto_final) else 0
        
        # Si ambos CAPEX son 0, retornar Monto Final completo
        if capex_ext_val == 0 and capex_ord_val == 0:
            return monto_final_val
        
        # Calcular total
        total = capex_ext_val + capex_ord_val + cadm_val
        
        if total == 0:
            return monto_final_val
        
        # Proporción de CADM (OPEX) sobre el total
        proporcion_opex = cadm_val / total
        return proporcion_opex * monto_final_val
    
    df_result['Monto Opex Final'] = [
        calcular_monto_opex_final(ce, co, ca, mf) 
        for ce, co, ca, mf in zip(capex_ext, capex_ord, cadm, monto_final)
    ]
    print(f"[THREAD-DF] Columna 'Monto Opex Final' calculada")
    
    # ========================================================================
    # OBTENER TABLA DE ÁREAS DESDE GOOGLE SHEETS
    # ========================================================================
    print("[THREAD-DF] Obteniendo tabla de áreas desde Google Sheets...")
    try:
        df_areas = get_google_sheet_data()
        # Crear diccionario para búsqueda rápida (Solicitante -> Area)
        # Asumiendo que columna A es el código y columna B es el área
        areas_dict = {}
        if len(df_areas.columns) >= 2:
            col_codigo = df_areas.columns[0]
            col_area = df_areas.columns[1]
            for _, row in df_areas.iterrows():
                codigo = row[col_codigo]
                area = row[col_area]
                if pd.notna(codigo):
                    areas_dict[str(codigo).strip()] = str(area).strip() if pd.notna(area) else "SERVICIOS"
        print(f"[THREAD-DF] Tabla de áreas cargada: {len(areas_dict)} registros")
    except Exception as e:
        print(f"[THREAD-DF] WARN: No se pudo cargar tabla de áreas: {str(e)}")
        df_areas = pd.DataFrame()
        areas_dict = {}
    
    # Guardar df_areas en attrs para usarlo en el Excel
    df_result.attrs['df_areas'] = df_areas
    
    # Obtener columnas necesarias para AREA
    proveedor = df_result.get('Proveedor', pd.Series([''] * len(df_result)))
    sucursal = df_result.get('Sucursal', pd.Series([''] * len(df_result)))
    solicitante = df_result.get('Solicitante', pd.Series([''] * len(df_result)))
    
    # Obtener Monto Capex Final y Monto Opex Final para las siguientes columnas
    monto_capex_final = pd.to_numeric(df_result['Monto Capex Final'], errors='coerce').fillna(0)
    monto_opex_final = pd.to_numeric(df_result['Monto Opex Final'], errors='coerce').fillna(0)
    
    # ========================================================================
    # COLUMNA 7: AREA
    # Lógica:
    # - Si proveedor + sucursal cumplen condiciones de RECARGAS → "RECARGAS"
    # - Si Solicitante = 0 → "SERVICIOS"
    # - Si no → BUSCARV en tabla de áreas
    # ========================================================================
    def calcular_area(row_proveedor, row_sucursal, row_solicitante):
        prov = str(row_proveedor).strip().upper() if pd.notna(row_proveedor) else ''
        suc = str(row_sucursal).strip().upper() if pd.notna(row_sucursal) else ''
        sol = str(row_solicitante).strip() if pd.notna(row_solicitante) else '0'
        
        # Verificar proveedores que siempre son RECARGAS
        for prov_recarga in PROVEEDORES_SIEMPRE_RECARGAS:
            if prov_recarga.upper() in prov or prov in prov_recarga.upper():
                return "RECARGAS"
        
        # Verificar proveedores con condiciones de sucursal
        for prov_cond, sucursales in PROVEEDORES_RECARGAS_CONDICIONAL.items():
            if prov_cond.upper() in prov or prov in prov_cond.upper():
                for suc_recarga in sucursales:
                    if suc_recarga.upper() in suc or suc in suc_recarga.upper():
                        return "RECARGAS"
        
        # Si Solicitante es 0 o vacío → SERVICIOS
        try:
            sol_num = float(sol) if sol else 0
            if sol_num == 0:
                return "SERVICIOS"
        except ValueError:
            pass
        
        # Buscar en tabla de áreas
        if sol in areas_dict:
            return areas_dict[sol]
        
        # Si no se encuentra, retornar SERVICIOS
        return "SERVICIOS"
    
    df_result['AREA'] = [
        calcular_area(p, s, sol) 
        for p, s, sol in zip(proveedor, sucursal, solicitante)
    ]
    print(f"[THREAD-DF] Columna 'AREA' calculada")
    
    # ========================================================================
    # COLUMNA 8: Tipo Capex 2
    # Lógica:
    # - Si AREA = "RECARGAS" → "RECARGAS"
    # - Si (Monto CAPEX Final <> 0 Y Monto OPEX Final <> 0) → "MIXTA"
    # - Si Monto CAPEX Final <> 0 → "CAPEX"
    # - Si no → "OPEX"
    # ========================================================================
    def calcular_tipo_capex_2(row_area, row_monto_capex_final, row_monto_opex_final):
        area = str(row_area).strip().upper() if pd.notna(row_area) else ''
        mcf = float(row_monto_capex_final) if pd.notna(row_monto_capex_final) else 0
        mof = float(row_monto_opex_final) if pd.notna(row_monto_opex_final) else 0
        
        if area == "RECARGAS":
            return "RECARGAS"
        
        if mcf != 0 and mof != 0:
            return "MIXTA"
        
        if mcf != 0:
            return "CAPEX"
        
        return "OPEX"
    
    df_result['Tipo Capex 2'] = [
        calcular_tipo_capex_2(a, mcf, mof) 
        for a, mcf, mof in zip(df_result['AREA'], monto_capex_final, monto_opex_final)
    ]
    print(f"[THREAD-DF] Columna 'Tipo Capex 2' calculada")
    
    # ========================================================================
    # COLUMNA 9: Tipo Capex
    # Lógica:
    # - Si AREA = "RECARGAS" → "RECARGAS"
    # - Si Tipo Capex 2 = "OPEX" → "OPEX"
    # - Si (Monto CAPEX EXT <> 0 Y Monto CAPEX ORD <> 0) → "MIXTA"
    # - Si Monto CAPEX EXT <> 0 → "EXT"
    # - Si no → "ORD"
    # ========================================================================
    def calcular_tipo_capex(row_area, row_tipo_capex_2, row_capex_ext, row_capex_ord):
        area = str(row_area).strip().upper() if pd.notna(row_area) else ''
        tc2 = str(row_tipo_capex_2).strip().upper() if pd.notna(row_tipo_capex_2) else ''
        ce = float(row_capex_ext) if pd.notna(row_capex_ext) else 0
        co = float(row_capex_ord) if pd.notna(row_capex_ord) else 0
        
        if area == "RECARGAS":
            return "RECARGAS"
        
        if tc2 == "OPEX":
            return "OPEX"
        
        if ce != 0 and co != 0:
            return "MIXTA"
        
        if ce != 0:
            return "EXT"
        
        return "ORD"
    
    df_result['Tipo Capex'] = [
        calcular_tipo_capex(a, tc2, ce, co) 
        for a, tc2, ce, co in zip(df_result['AREA'], df_result['Tipo Capex 2'], capex_ext, capex_ord)
    ]
    print(f"[THREAD-DF] Columna 'Tipo Capex' calculada")
    
    # ========================================================================
    # COLUMNA 10: Monto Capex ORD 2
    # Lógica:
    # - Si Tipo Capex = "OPEX" o "RECARGAS" o "PRESTAMO" → 0
    # - Si Tipo Capex = "EXT" → 0
    # - Si Tipo Capex = "ORD" → Monto Capex Final
    # - Sino → Monto Capex Final * (CAPEX ORD / (CAPEX ORD + CAPEX EXT))
    # ========================================================================
    def calcular_monto_capex_ord_2(row_tipo_capex, row_monto_capex_final, row_capex_ext, row_capex_ord):
        tc = str(row_tipo_capex).strip().upper() if pd.notna(row_tipo_capex) else ''
        mcf = float(row_monto_capex_final) if pd.notna(row_monto_capex_final) else 0
        ce = float(row_capex_ext) if pd.notna(row_capex_ext) else 0
        co = float(row_capex_ord) if pd.notna(row_capex_ord) else 0
        
        # Si es OPEX, RECARGAS o PRESTAMO → 0
        if tc in ["OPEX", "RECARGAS", "PRESTAMO"]:
            return 0
        
        # Si es EXT → 0
        if tc == "EXT":
            return 0
        
        # Si es ORD → Monto Capex Final
        if tc == "ORD":
            return mcf
        
        # Sino (MIXTA) → Monto Capex Final * (CAPEX ORD / (CAPEX ORD + CAPEX EXT))
        total = co + ce
        if total == 0:
            return 0
        return mcf * (co / total)
    
    df_result['Monto Capex ORD 2'] = [
        calcular_monto_capex_ord_2(tc, mcf, ce, co) 
        for tc, mcf, ce, co in zip(df_result['Tipo Capex'], monto_capex_final, capex_ext, capex_ord)
    ]
    print(f"[THREAD-DF] Columna 'Monto Capex ORD 2' calculada")
    
    # ========================================================================
    # COLUMNA 11: Monto Capex EXT 3
    # Lógica:
    # - Si Tipo Capex = "OPEX" o "RECARGAS" o "PRESTAMO" → 0
    # - Si Tipo Capex = "ORD" → 0
    # - Si Tipo Capex = "EXT" → Monto Capex Final
    # - Sino → Monto Capex Final * (CAPEX EXT / (CAPEX ORD + CAPEX EXT))
    # ========================================================================
    def calcular_monto_capex_ext_3(row_tipo_capex, row_monto_capex_final, row_capex_ext, row_capex_ord):
        tc = str(row_tipo_capex).strip().upper() if pd.notna(row_tipo_capex) else ''
        mcf = float(row_monto_capex_final) if pd.notna(row_monto_capex_final) else 0
        ce = float(row_capex_ext) if pd.notna(row_capex_ext) else 0
        co = float(row_capex_ord) if pd.notna(row_capex_ord) else 0
        
        # Si es OPEX, RECARGAS o PRESTAMO → 0
        if tc in ["OPEX", "RECARGAS", "PRESTAMO"]:
            return 0
        
        # Si es ORD → 0
        if tc == "ORD":
            return 0
        
        # Si es EXT → Monto Capex Final
        if tc == "EXT":
            return mcf
        
        # Sino (MIXTA) → Monto Capex Final * (CAPEX EXT / (CAPEX ORD + CAPEX EXT))
        total = co + ce
        if total == 0:
            return 0
        return mcf * (ce / total)
    
    df_result['Monto Capex EXT 3'] = [
        calcular_monto_capex_ext_3(tc, mcf, ce, co) 
        for tc, mcf, ce, co in zip(df_result['Tipo Capex'], monto_capex_final, capex_ext, capex_ord)
    ]
    print(f"[THREAD-DF] Columna 'Monto Capex EXT 3' calculada")
    
    # ========================================================================
    # OBTENER TASA EUR/USD
    # ========================================================================
    print("[THREAD-DF] Obteniendo tasa EUR/USD...")
    tasa_eur_info = obtener_tasa_euro_dolar()
    if tasa_eur_info['success'] and tasa_eur_info['tasa']:
        tasa_eur_usd = float(tasa_eur_info['tasa'])
        print(f"[THREAD-DF] Tasa EUR/USD: {tasa_eur_usd}")
    else:
        tasa_eur_usd = 1.10  # Tasa por defecto
        print(f"[THREAD-DF] WARN: Usando tasa EUR/USD por defecto: {tasa_eur_usd}")
    
    # Obtener series necesarias para las nuevas columnas
    monto_capex_ord_2 = pd.to_numeric(df_result['Monto Capex ORD 2'], errors='coerce').fillna(0)
    monto_capex_ext_3 = pd.to_numeric(df_result['Monto Capex EXT 3'], errors='coerce').fillna(0)
    moneda_pago = df_result['Moneda Pago']
    dia_pago = df_result['Dia de Pago']
    
    # ========================================================================
    # COLUMNA 12: Monto Capex ORD USD
    # Lógica:
    # - Si Moneda Pago = "USD" → Monto Capex ORD 2
    # - Si Moneda Pago = "EUR" → Monto Capex ORD 2 * tasa_eur_usd
    # - Si Dia de Pago = "MARTES" → Monto Capex ORD 2 / tasa_ves_usd
    # - Sino → Monto Capex ORD 2 / tasa_ves_usd_mas_5
    # ========================================================================
    def calcular_monto_capex_ord_usd(row_monto, row_moneda_pago, row_dia_pago):
        monto = float(row_monto) if pd.notna(row_monto) else 0
        mp = str(row_moneda_pago).strip().upper() if pd.notna(row_moneda_pago) else ''
        dp = str(row_dia_pago).strip().upper() if pd.notna(row_dia_pago) else ''
        
        if monto == 0:
            return 0
        
        if mp == "USD":
            return monto
        
        if mp == "EUR":
            return monto * tasa_eur_usd
        
        # Para VES u otras monedas, dividir por la tasa
        if dp == "MARTES":
            return monto / tasa_ves_usd if tasa_ves_usd != 0 else 0
        else:
            return monto / tasa_ves_usd_mas_5 if tasa_ves_usd_mas_5 != 0 else 0
    
    df_result['Monto Capex ORD USD'] = [
        calcular_monto_capex_ord_usd(m, mp, dp) 
        for m, mp, dp in zip(monto_capex_ord_2, moneda_pago, dia_pago)
    ]
    print(f"[THREAD-DF] Columna 'Monto Capex ORD USD' calculada")
    
    # ========================================================================
    # COLUMNA 13: Monto Capex EXT USD
    # Lógica:
    # - Si Moneda Pago = "USD" → Monto Capex EXT 3
    # - Si Moneda Pago = "COP" → Monto Capex EXT 3 (sin conversión)
    # - Si Moneda Pago = "EUR" → Monto Capex EXT 3 * tasa_eur_usd
    # - Si Dia de Pago = "MARTES" → Monto Capex EXT 3 / tasa_ves_usd
    # - Sino → Monto Capex EXT 3 / tasa_ves_usd_mas_5
    # ========================================================================
    def calcular_monto_capex_ext_usd(row_monto, row_moneda_pago, row_dia_pago):
        monto = float(row_monto) if pd.notna(row_monto) else 0
        mp = str(row_moneda_pago).strip().upper() if pd.notna(row_moneda_pago) else ''
        dp = str(row_dia_pago).strip().upper() if pd.notna(row_dia_pago) else ''
        
        if monto == 0:
            return 0
        
        if mp == "USD":
            return monto
        
        if mp == "COP":
            return monto  # Sin conversión para COP
        
        if mp == "EUR":
            return monto * tasa_eur_usd
        
        # Para VES u otras monedas, dividir por la tasa
        if dp == "MARTES":
            return monto / tasa_ves_usd if tasa_ves_usd != 0 else 0
        else:
            return monto / tasa_ves_usd_mas_5 if tasa_ves_usd_mas_5 != 0 else 0
    
    df_result['Monto Capex EXT USD'] = [
        calcular_monto_capex_ext_usd(m, mp, dp) 
        for m, mp, dp in zip(monto_capex_ext_3, moneda_pago, dia_pago)
    ]
    print(f"[THREAD-DF] Columna 'Monto Capex EXT USD' calculada")
    
    # ========================================================================
    # COLUMNA 14: Monto CAPEX USD
    # Lógica:
    # - Si Moneda Pago = "USD" → Monto Capex Final
    # - Si Moneda Pago = "COP" → Monto Capex Final
    # - Si Moneda Pago = "EUR" → Monto Capex Final * tasa_eur_usd
    # - Si Dia de Pago = "MARTES" → Monto Capex Final / tasa_ves_usd
    # - Sino → Monto Capex Final / tasa_ves_usd_mas_5
    # ========================================================================
    def calcular_monto_capex_usd(row_monto, row_moneda_pago, row_dia_pago):
        monto = float(row_monto) if pd.notna(row_monto) else 0
        mp = str(row_moneda_pago).strip().upper() if pd.notna(row_moneda_pago) else ''
        dp = str(row_dia_pago).strip().upper() if pd.notna(row_dia_pago) else ''
        
        if monto == 0:
            return 0
        
        if mp == "USD":
            return monto
        
        if mp == "COP":
            return monto
        
        if mp == "EUR":
            return monto * tasa_eur_usd
        
        # Para VES u otras monedas, dividir por la tasa
        if dp == "MARTES":
            return monto / tasa_ves_usd if tasa_ves_usd != 0 else 0
        else:
            return monto / tasa_ves_usd_mas_5 if tasa_ves_usd_mas_5 != 0 else 0
    
    df_result['Monto CAPEX USD'] = [
        calcular_monto_capex_usd(m, mp, dp) 
        for m, mp, dp in zip(monto_capex_final, moneda_pago, dia_pago)
    ]
    print(f"[THREAD-DF] Columna 'Monto CAPEX USD' calculada")
    
    # ========================================================================
    # COLUMNA 15: Monto OPEX USD
    # Lógica:
    # - Si Moneda Pago = "USD" → Monto Opex Final
    # - Si Moneda Pago = "EUR" → Monto Opex Final * tasa_eur_usd
    # - Si Dia de Pago = "MARTES" → Monto Opex Final / tasa_ves_usd
    # - Sino → Monto Opex Final / tasa_ves_usd_mas_5
    # ========================================================================
    def calcular_monto_opex_usd(row_monto, row_moneda_pago, row_dia_pago):
        monto = float(row_monto) if pd.notna(row_monto) else 0
        mp = str(row_moneda_pago).strip().upper() if pd.notna(row_moneda_pago) else ''
        dp = str(row_dia_pago).strip().upper() if pd.notna(row_dia_pago) else ''
        
        if monto == 0:
            return 0
        
        if mp == "USD":
            return monto
        
        if mp == "EUR":
            return monto * tasa_eur_usd
        
        # Para VES u otras monedas, dividir por la tasa
        if dp == "MARTES":
            return monto / tasa_ves_usd if tasa_ves_usd != 0 else 0
        else:
            return monto / tasa_ves_usd_mas_5 if tasa_ves_usd_mas_5 != 0 else 0
    
    df_result['Monto OPEX USD'] = [
        calcular_monto_opex_usd(m, mp, dp) 
        for m, mp, dp in zip(monto_opex_final, moneda_pago, dia_pago)
    ]
    print(f"[THREAD-DF] Columna 'Monto OPEX USD' calculada")
    
    # ========================================================================
    # COLUMNA 16: Monto Total USD
    # Lógica: Monto CAPEX USD + Monto OPEX USD
    # ========================================================================
    monto_capex_usd = pd.to_numeric(df_result['Monto CAPEX USD'], errors='coerce').fillna(0)
    monto_opex_usd = pd.to_numeric(df_result['Monto OPEX USD'], errors='coerce').fillna(0)
    
    df_result['Monto Total USD'] = monto_capex_usd + monto_opex_usd
    print(f"[THREAD-DF] Columna 'Monto Total USD' calculada")
    
    # Guardar las tasas en el DataFrame para referencia
    df_result.attrs['tasa_ves_usd'] = tasa_ves_usd
    df_result.attrs['tasa_ves_usd_mas_5'] = tasa_ves_usd_mas_5
    df_result.attrs['tasa_eur_usd'] = tasa_eur_usd
    
    print(f"[THREAD-DF] Columnas adicionales completadas: {df_result.shape[1]} columnas totales")
    
    return df_result


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
        
        # Calcular columnas adicionales (Moneda Pago, Cuenta Bancaria, Dia de Pago)
        df_procesado = calcular_columnas_adicionales(df_limpio)
        
        # Calcular estadísticas
        stats = {
            'total_filas': len(df_procesado),
            'total_columnas': len(df_procesado.columns),
            'columnas': list(df_procesado.columns),
            'montos': {},
            'resumen_moneda_pago': df_procesado['Moneda Pago'].value_counts().to_dict() if 'Moneda Pago' in df_procesado.columns else {},
            'resumen_dia_pago': df_procesado['Dia de Pago'].value_counts().to_dict() if 'Dia de Pago' in df_procesado.columns else {}
        }
        
        # Calcular sumas de montos si existen las columnas
        columnas_monto = ['Monto', 'Monto CAPEX EXT', 'Monto CAPEX ORD', 'Monto CADM']
        for col in columnas_monto:
            if col in df_procesado.columns:
                try:
                    stats['montos'][col] = float(pd.to_numeric(df_procesado[col], errors='coerce').sum())
                except:
                    stats['montos'][col] = 0
        
        resultado.dataframe_result = {
            'success': True,
            'stats': stats,
            'data': dataframe_a_json_serializable(df_procesado),
            'df': df_procesado  # Guardar el DataFrame para el thread de Excel
        }
        
        print(f"[{thread_name}] Procesamiento de DataFrame completado: {stats['total_filas']} registros")
        
    except Exception as e:
        resultado.dataframe_error = str(e)
        print(f"[{thread_name}] ERROR: {str(e)}")


# ============================================================================
# FUNCIONES DE GENERACIÓN DE EXCEL (Thread 2)
# ============================================================================

def indice_a_letra_excel(idx: int) -> str:
    """
    Convierte un índice de columna (0-based) a letra de Excel.
    Ej: 0->A, 25->Z, 26->AA, 27->AB
    """
    resultado = ""
    while idx >= 0:
        resultado = chr(65 + (idx % 26)) + resultado
        idx = idx // 26 - 1
    return resultado


def generar_formula_or_prioridades(col_prioridad: str, prioridades: List[int], excel_row: int) -> str:
    """
    Genera la parte OR de la fórmula para verificar múltiples prioridades.
    Ej: OR(T2=69,T2=70,T2=71,...)
    """
    condiciones = [f'{col_prioridad}{excel_row}={p}' for p in prioridades]
    return f'OR({",".join(condiciones)})'


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
        col_formula_idx = num_cols  # Índice para columnas de fórmula
        
        # Encontrar índices de columnas necesarias
        col_indices = {col: idx for idx, col in enumerate(df.columns)}
        
        # Columnas originales necesarias
        moneda_col = col_indices.get('Moneda')
        prioridad_col = col_indices.get('Prioridad')
        cuenta_col = col_indices.get('Cuenta')
        capex_ext_col = col_indices.get('Monto CAPEX EXT')
        capex_ord_col = col_indices.get('Monto CAPEX ORD')
        cadm_col = col_indices.get('Monto CADM')
        monto_col = col_indices.get('Monto')
        fecha_venc_col = col_indices.get('Fecha Vencimiento')
        
        # Letras de columnas originales
        moneda_letter = indice_a_letra_excel(moneda_col) if moneda_col is not None else None
        prioridad_letter = indice_a_letra_excel(prioridad_col) if prioridad_col is not None else None
        cuenta_letter = indice_a_letra_excel(cuenta_col) if cuenta_col is not None else None
        
        # ====================================================================
        # COLUMNA 1: Moneda Pago
        # Fórmula: =IF(Moneda="EUR","EUR",IF(Moneda="COP","COP",IF(Moneda="USD",IF(OR(Prioridad=69,...),"USD","VES"),"VES")))
        # ====================================================================
        col_moneda_pago = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_moneda_pago, 'Moneda Pago', header_format)
        worksheet.set_column(col_moneda_pago, col_moneda_pago, 14)
        
        if moneda_letter and prioridad_letter:
            for row in range(1, num_filas + 1):
                excel_row = row + 1
                or_prioridades = generar_formula_or_prioridades(prioridad_letter, PRIORIDADES_USD_MONEDA_PAGO, excel_row)
                formula = (
                    f'=IF({moneda_letter}{excel_row}="EUR","EUR",'
                    f'IF({moneda_letter}{excel_row}="COP","COP",'
                    f'IF({moneda_letter}{excel_row}="USD",'
                    f'IF({or_prioridades},"USD","VES"),"VES")))'
                )
                worksheet.write_formula(row, col_moneda_pago, formula, text_format)
        
        print(f"[THREAD-EXCEL] Columna 'Moneda Pago' agregada")
        
        # ====================================================================
        # COLUMNA 2: Cuenta Bancaria
        # Fórmula: =IF(Moneda="USD",IF(OR(Prioridad=69,...),Cuenta,"1111"),Cuenta)
        # ====================================================================
        col_cuenta_bancaria = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_cuenta_bancaria, 'Cuenta Bancaria', header_format)
        worksheet.set_column(col_cuenta_bancaria, col_cuenta_bancaria, 16)
        
        if moneda_letter and prioridad_letter and cuenta_letter:
            for row in range(1, num_filas + 1):
                excel_row = row + 1
                or_prioridades = generar_formula_or_prioridades(prioridad_letter, PRIORIDADES_USD_CUENTA_ORIGINAL, excel_row)
                formula = (
                    f'=IF({moneda_letter}{excel_row}="USD",'
                    f'IF({or_prioridades},{cuenta_letter}{excel_row},"{CUENTA_USD_DEFAULT}"),'
                    f'{cuenta_letter}{excel_row})'
                )
                worksheet.write_formula(row, col_cuenta_bancaria, formula, text_format)
        
        print(f"[THREAD-EXCEL] Columna 'Cuenta Bancaria' agregada")
        
        # ====================================================================
        # COLUMNA 3: Dia de Pago
        # Fórmula: =IF(OR(MonedaPago="USD",MonedaPago="EUR"),"VIERNES","JUEVES")
        # ====================================================================
        col_dia_pago = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_dia_pago, 'Dia de Pago', header_format)
        worksheet.set_column(col_dia_pago, col_dia_pago, 12)
        
        moneda_pago_letter = indice_a_letra_excel(col_moneda_pago)
        
        for row in range(1, num_filas + 1):
            excel_row = row + 1
            formula = (
                f'=IF(OR({moneda_pago_letter}{excel_row}="USD",'
                f'{moneda_pago_letter}{excel_row}="EUR"),"VIERNES","JUEVES")'
            )
            worksheet.write_formula(row, col_dia_pago, formula, text_format)
        
        print(f"[THREAD-EXCEL] Columna 'Dia de Pago' agregada")
        
        # ====================================================================
        # CREAR HOJA "Tasa" CON LAS TASAS DE CAMBIO
        # ====================================================================
        # Obtener tasas de cambio VES/USD
        tasa_info = obtener_tasa_bolivar_dolar()
        if tasa_info['success'] and tasa_info['tasa']:
            tasa_ves_usd = float(tasa_info['tasa'])
        else:
            tasa_ves_usd = 50.0  # Tasa por defecto si falla la API
        tasa_ves_usd_mas_5 = tasa_ves_usd + MARGEN_TASA_JUEVES
        
        # Obtener tasa EUR/USD
        tasa_eur_info = obtener_tasa_euro_dolar()
        if tasa_eur_info['success'] and tasa_eur_info['tasa']:
            tasa_eur_usd = float(tasa_eur_info['tasa'])
        else:
            tasa_eur_usd = 1.10  # Tasa por defecto
        
        print(f"[THREAD-EXCEL] Tasas obtenidas: VES/USD={tasa_ves_usd}, VES+5={tasa_ves_usd_mas_5}, EUR/USD={tasa_eur_usd}")
        
        # Crear hoja "Tasa"
        ws_tasa = workbook.add_worksheet('Tasa')
        
        # Formatos para la hoja Tasa
        tasa_header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#4472C4',
            'font_color': 'white',
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'font_size': 12
        })
        
        tasa_value_format = workbook.add_format({
            'num_format': '#,##0.00',
            'border': 1,
            'align': 'center',
            'font_size': 14,
            'bold': True
        })
        
        tasa_label_format = workbook.add_format({
            'border': 1,
            'align': 'left',
            'font_size': 11
        })
        
        # Escribir encabezados y valores en hoja Tasa
        ws_tasa.write(0, 0, 'Descripción', tasa_header_format)
        ws_tasa.write(0, 1, 'Valor', tasa_header_format)
        ws_tasa.write(0, 2, 'Fuente', tasa_header_format)
        
        # Fila 2: Tasa VES/USD (celda B2)
        ws_tasa.write(1, 0, 'Tasa VES/USD', tasa_label_format)
        ws_tasa.write(1, 1, tasa_ves_usd, tasa_value_format)
        ws_tasa.write(1, 2, tasa_info.get('fuente', 'DolarAPI') if tasa_info['success'] else 'Por defecto', tasa_label_format)
        
        # Fila 3: Tasa VES/USD + 5 (celda B3)
        ws_tasa.write(2, 0, 'Tasa VES/USD + 5', tasa_label_format)
        ws_tasa.write(2, 1, tasa_ves_usd_mas_5, tasa_value_format)
        ws_tasa.write(2, 2, 'Calculada (Tasa + 5)', tasa_label_format)
        
        # Fila 4: Margen
        ws_tasa.write(3, 0, 'Margen día JUEVES', tasa_label_format)
        ws_tasa.write(3, 1, MARGEN_TASA_JUEVES, tasa_value_format)
        ws_tasa.write(3, 2, 'Configuración', tasa_label_format)
        
        # Fila 5: Tasa EUR/USD (celda B5)
        ws_tasa.write(4, 0, 'Tasa EUR/USD', tasa_label_format)
        ws_tasa.write(4, 1, tasa_eur_usd, tasa_value_format)
        ws_tasa.write(4, 2, tasa_eur_info.get('fuente', 'Frankfurter') if tasa_eur_info['success'] else 'Por defecto', tasa_label_format)
        
        # Fila 6: Fecha de consulta
        ws_tasa.write(5, 0, 'Fecha consulta', tasa_label_format)
        ws_tasa.write(5, 1, tasa_info.get('fecha', datetime.now().strftime('%Y-%m-%d')), tasa_label_format)
        ws_tasa.write(5, 2, tasa_info.get('timestamp', ''), tasa_label_format)
        
        # Ajustar ancho de columnas
        ws_tasa.set_column(0, 0, 20)
        ws_tasa.set_column(1, 1, 15)
        ws_tasa.set_column(2, 2, 30)
        
        print(f"[THREAD-EXCEL] Hoja 'Tasa' creada")
        
        # Referencias a las tasas en la hoja Tasa (para usar en fórmulas)
        # Tasa!$B$2 = Tasa VES/USD
        # Tasa!$B$3 = Tasa VES/USD + 5
        # Tasa!$B$5 = Tasa EUR/USD
        tasa_normal_ref = "Tasa!$B$2"
        tasa_mas_5_ref = "Tasa!$B$3"
        tasa_eur_ref = "Tasa!$B$5"
        
        # Letras necesarias para las fórmulas
        monto_letter = indice_a_letra_excel(monto_col) if monto_col is not None else None
        dia_pago_letter = indice_a_letra_excel(col_dia_pago)
        capex_ext_letter = indice_a_letra_excel(capex_ext_col) if capex_ext_col is not None else None
        capex_ord_letter = indice_a_letra_excel(capex_ord_col) if capex_ord_col is not None else None
        cadm_letter = indice_a_letra_excel(cadm_col) if cadm_col is not None else None
        
        # ====================================================================
        # COLUMNA 7: Monto Final
        # Fórmula: =IF(Moneda="VES",Monto,IF(OR(Prioridad=67,...),Monto,IF(DiaPago="JUEVES",Monto*TasaMas5,Monto*TasaNormal)))
        # ====================================================================
        col_monto_final = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_monto_final, 'Monto Final', header_format)
        worksheet.set_column(col_monto_final, col_monto_final, 16)
        
        if moneda_letter and prioridad_letter and monto_letter:
            for row in range(1, num_filas + 1):
                excel_row = row + 1
                or_prioridades = generar_formula_or_prioridades(prioridad_letter, PRIORIDADES_MONTO_SIN_CONVERSION, excel_row)
                formula = (
                    f'=IF({moneda_letter}{excel_row}="VES",{monto_letter}{excel_row},'
                    f'IF({or_prioridades},{monto_letter}{excel_row},'
                    f'IF({dia_pago_letter}{excel_row}="JUEVES",'
                    f'{monto_letter}{excel_row}*{tasa_mas_5_ref},'
                    f'{monto_letter}{excel_row}*{tasa_normal_ref})))'
                )
                worksheet.write_formula(row, col_monto_final, formula, money_format)
        
        monto_final_letter = indice_a_letra_excel(col_monto_final)
        print(f"[THREAD-EXCEL] Columna 'Monto Final' agregada")
        
        # ====================================================================
        # COLUMNA 8: Monto Capex Final
        # Fórmula: =IF(AND(CAPEX_EXT=0,CAPEX_ORD=0),0,((CAPEX_EXT+CAPEX_ORD)/(CAPEX_EXT+CAPEX_ORD+CADM))*MontoFinal)
        # ====================================================================
        col_monto_capex_final = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_monto_capex_final, 'Monto Capex Final', header_format)
        worksheet.set_column(col_monto_capex_final, col_monto_capex_final, 18)
        
        if capex_ext_letter and capex_ord_letter and cadm_letter:
            for row in range(1, num_filas + 1):
                excel_row = row + 1
                formula = (
                    f'=IF(AND({capex_ext_letter}{excel_row}=0,{capex_ord_letter}{excel_row}=0),0,'
                    f'(({capex_ext_letter}{excel_row}+{capex_ord_letter}{excel_row})/'
                    f'({capex_ext_letter}{excel_row}+{capex_ord_letter}{excel_row}+{cadm_letter}{excel_row}))'
                    f'*{monto_final_letter}{excel_row})'
                )
                worksheet.write_formula(row, col_monto_capex_final, formula, money_format)
        
        monto_capex_final_letter = indice_a_letra_excel(col_monto_capex_final)
        print(f"[THREAD-EXCEL] Columna 'Monto Capex Final' agregada")
        
        # ====================================================================
        # COLUMNA 9: Monto Opex Final
        # Fórmula: =IF(AND(CAPEX_EXT=0,CAPEX_ORD=0),MontoFinal,(CADM/(CAPEX_EXT+CAPEX_ORD+CADM))*MontoFinal)
        # ====================================================================
        col_monto_opex_final = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_monto_opex_final, 'Monto Opex Final', header_format)
        worksheet.set_column(col_monto_opex_final, col_monto_opex_final, 18)
        
        if capex_ext_letter and capex_ord_letter and cadm_letter:
            for row in range(1, num_filas + 1):
                excel_row = row + 1
                formula = (
                    f'=IF(AND({capex_ext_letter}{excel_row}=0,{capex_ord_letter}{excel_row}=0),'
                    f'{monto_final_letter}{excel_row},'
                    f'({cadm_letter}{excel_row}/'
                    f'({capex_ext_letter}{excel_row}+{capex_ord_letter}{excel_row}+{cadm_letter}{excel_row}))'
                    f'*{monto_final_letter}{excel_row})'
                )
                worksheet.write_formula(row, col_monto_opex_final, formula, money_format)
        
        monto_opex_final_letter = indice_a_letra_excel(col_monto_opex_final)
        print(f"[THREAD-EXCEL] Columna 'Monto Opex Final' agregada")
        
        # ====================================================================
        # CREAR HOJA "Areas" CON LA TABLA DE ÁREAS
        # ====================================================================
        print("[THREAD-EXCEL] Obteniendo tabla de áreas desde Google Sheets...")
        try:
            df_areas = get_google_sheet_data()
            
            # Crear hoja "Areas"
            ws_areas = workbook.add_worksheet('Areas')
            
            # Escribir encabezados
            for col_idx, col_name in enumerate(df_areas.columns):
                ws_areas.write(0, col_idx, col_name, header_format)
            
            # Escribir datos
            for row_idx, row in df_areas.iterrows():
                for col_idx, value in enumerate(row):
                    ws_areas.write(row_idx + 1, col_idx, value, text_format)
            
            # Ajustar ancho de columnas
            for col_idx in range(len(df_areas.columns)):
                ws_areas.set_column(col_idx, col_idx, 20)
            
            num_areas = len(df_areas)
            print(f"[THREAD-EXCEL] Hoja 'Areas' creada con {num_areas} registros")
        except Exception as e:
            print(f"[THREAD-EXCEL] WARN: No se pudo crear hoja 'Areas': {str(e)}")
            df_areas = pd.DataFrame()
            num_areas = 0
        
        # Letras adicionales necesarias para AREA
        proveedor_col = col_indices.get('Proveedor')
        sucursal_col = col_indices.get('Sucursal')
        solicitante_col = col_indices.get('Solicitante')
        
        proveedor_letter = indice_a_letra_excel(proveedor_col) if proveedor_col is not None else None
        sucursal_letter = indice_a_letra_excel(sucursal_col) if sucursal_col is not None else None
        solicitante_letter = indice_a_letra_excel(solicitante_col) if solicitante_col is not None else None
        
        # ====================================================================
        # COLUMNA 10: AREA
        # Fórmula compleja con múltiples condiciones para RECARGAS y VLOOKUP
        # ====================================================================
        col_area = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_area, 'AREA', header_format)
        worksheet.set_column(col_area, col_area, 15)
        
        if proveedor_letter and sucursal_letter and solicitante_letter and num_areas > 0:
            for row in range(1, num_filas + 1):
                excel_row = row + 1
                # Construir fórmula compleja para AREA
                # Condiciones de RECARGAS
                cond_digitel = f'AND({proveedor_letter}{excel_row}="CORPORACION DIGITEL, C.A.",OR({sucursal_letter}{excel_row}="POSPAGO FACTURA",{sucursal_letter}{excel_row}="PREPAGO RECARGA"))'
                cond_netuno = f'AND({proveedor_letter}{excel_row}="NETUNO, C.A.",{sucursal_letter}{excel_row}="RECARGAS")'
                cond_telefonica1 = f'AND({proveedor_letter}{excel_row}="TELEFÓNICA VENEZOLANA, C.A.",{sucursal_letter}{excel_row}="RECARGAS")'
                cond_telefonica2 = f'AND({proveedor_letter}{excel_row}="TELEFONICA VENEZOLANA, C.A.",{sucursal_letter}{excel_row}="RECARGAS")'
                cond_galaxy = f'{proveedor_letter}{excel_row}="GALAXY ENTERTAINMENT DE VENEZUELA, C.A. (SIMPLE TV )"'
                cond_recargas_movil = f'{proveedor_letter}{excel_row}="RECARGAS MOVIL C.A"'
                
                formula = (
                    f'=IF(OR({cond_digitel},{cond_netuno},{cond_telefonica1},{cond_telefonica2},{cond_galaxy},{cond_recargas_movil}),'
                    f'"RECARGAS",'
                    f'IF({solicitante_letter}{excel_row}=0,"SERVICIOS",'
                    f'IFERROR(VLOOKUP({solicitante_letter}{excel_row},Areas!$A$1:$B${num_areas + 1},2,FALSE),"SERVICIOS")))'
                )
                worksheet.write_formula(row, col_area, formula, text_format)
        
        area_letter = indice_a_letra_excel(col_area)
        print(f"[THREAD-EXCEL] Columna 'AREA' agregada")
        
        # ====================================================================
        # COLUMNA 11: Tipo Capex 2
        # Fórmula: =IF(AREA="RECARGAS","RECARGAS",IF(AND(MontoCapexFinal<>0,MontoOpexFinal<>0),"MIXTA",IF(MontoCapexFinal<>0,"CAPEX","OPEX")))
        # ====================================================================
        col_tipo_capex_2 = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_tipo_capex_2, 'Tipo Capex 2', header_format)
        worksheet.set_column(col_tipo_capex_2, col_tipo_capex_2, 14)
        
        for row in range(1, num_filas + 1):
            excel_row = row + 1
            formula = (
                f'=IF({area_letter}{excel_row}="RECARGAS","RECARGAS",'
                f'IF(AND({monto_capex_final_letter}{excel_row}<>0,{monto_opex_final_letter}{excel_row}<>0),"MIXTA",'
                f'IF({monto_capex_final_letter}{excel_row}<>0,"CAPEX","OPEX")))'
            )
            worksheet.write_formula(row, col_tipo_capex_2, formula, text_format)
        
        tipo_capex_2_letter = indice_a_letra_excel(col_tipo_capex_2)
        print(f"[THREAD-EXCEL] Columna 'Tipo Capex 2' agregada")
        
        # ====================================================================
        # COLUMNA 12: Tipo Capex
        # Fórmula: =IF(AREA="RECARGAS","RECARGAS",IF(TipoCapex2="OPEX","OPEX",IF(AND(CapexExt<>0,CapexOrd<>0),"MIXTA",IF(CapexExt<>0,"EXT","ORD"))))
        # ====================================================================
        col_tipo_capex = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_tipo_capex, 'Tipo Capex', header_format)
        worksheet.set_column(col_tipo_capex, col_tipo_capex, 14)
        
        if capex_ext_letter and capex_ord_letter:
            for row in range(1, num_filas + 1):
                excel_row = row + 1
                formula = (
                    f'=IF({area_letter}{excel_row}="RECARGAS","RECARGAS",'
                    f'IF({tipo_capex_2_letter}{excel_row}="OPEX","OPEX",'
                    f'IF(AND({capex_ext_letter}{excel_row}<>0,{capex_ord_letter}{excel_row}<>0),"MIXTA",'
                    f'IF({capex_ext_letter}{excel_row}<>0,"EXT","ORD"))))'
                )
                worksheet.write_formula(row, col_tipo_capex, formula, text_format)
        
        tipo_capex_letter = indice_a_letra_excel(col_tipo_capex)
        print(f"[THREAD-EXCEL] Columna 'Tipo Capex' agregada")
        
        # ====================================================================
        # COLUMNA 13: Monto Capex ORD 2
        # Fórmula: =IF(OR(TipoCapex="OPEX",TipoCapex="RECARGAS",TipoCapex="PRESTAMO"),0,
        #             IF(TipoCapex="EXT",0,IF(TipoCapex="ORD",MontoCapexFinal,MontoCapexFinal*(CapexOrd/(CapexOrd+CapexExt)))))
        # ====================================================================
        col_monto_capex_ord_2 = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_monto_capex_ord_2, 'Monto Capex ORD 2', header_format)
        worksheet.set_column(col_monto_capex_ord_2, col_monto_capex_ord_2, 18)
        
        if capex_ext_letter and capex_ord_letter:
            for row in range(1, num_filas + 1):
                excel_row = row + 1
                formula = (
                    f'=IF(OR({tipo_capex_letter}{excel_row}="OPEX",{tipo_capex_letter}{excel_row}="RECARGAS",{tipo_capex_letter}{excel_row}="PRESTAMO"),0,'
                    f'IF({tipo_capex_letter}{excel_row}="EXT",0,'
                    f'IF({tipo_capex_letter}{excel_row}="ORD",{monto_capex_final_letter}{excel_row},'
                    f'{monto_capex_final_letter}{excel_row}*({capex_ord_letter}{excel_row}/({capex_ord_letter}{excel_row}+{capex_ext_letter}{excel_row})))))'
                )
                worksheet.write_formula(row, col_monto_capex_ord_2, formula, money_format)
        
        monto_capex_ord_2_letter = indice_a_letra_excel(col_monto_capex_ord_2)
        print(f"[THREAD-EXCEL] Columna 'Monto Capex ORD 2' agregada")
        
        # ====================================================================
        # COLUMNA 14: Monto Capex EXT 3
        # Fórmula: =IF(OR(TipoCapex="OPEX",TipoCapex="RECARGAS",TipoCapex="PRESTAMO"),0,
        #             IF(TipoCapex="ORD",0,IF(TipoCapex="EXT",MontoCapexFinal,MontoCapexFinal*(CapexExt/(CapexOrd+CapexExt)))))
        # ====================================================================
        col_monto_capex_ext_3 = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_monto_capex_ext_3, 'Monto Capex EXT 3', header_format)
        worksheet.set_column(col_monto_capex_ext_3, col_monto_capex_ext_3, 18)
        
        if capex_ext_letter and capex_ord_letter:
            for row in range(1, num_filas + 1):
                excel_row = row + 1
                formula = (
                    f'=IF(OR({tipo_capex_letter}{excel_row}="OPEX",{tipo_capex_letter}{excel_row}="RECARGAS",{tipo_capex_letter}{excel_row}="PRESTAMO"),0,'
                    f'IF({tipo_capex_letter}{excel_row}="ORD",0,'
                    f'IF({tipo_capex_letter}{excel_row}="EXT",{monto_capex_final_letter}{excel_row},'
                    f'{monto_capex_final_letter}{excel_row}*({capex_ext_letter}{excel_row}/({capex_ord_letter}{excel_row}+{capex_ext_letter}{excel_row})))))'
                )
                worksheet.write_formula(row, col_monto_capex_ext_3, formula, money_format)
        
        monto_capex_ext_3_letter = indice_a_letra_excel(col_monto_capex_ext_3)
        print(f"[THREAD-EXCEL] Columna 'Monto Capex EXT 3' agregada")
        
        # ====================================================================
        # COLUMNA 15: Monto Capex ORD USD
        # Fórmula: =IF(MonedaPago="USD",MontoCapexOrd2,IF(MonedaPago="EUR",MontoCapexOrd2*TasaEUR,IF(DiaPago="MARTES",MontoCapexOrd2/TasaVES,MontoCapexOrd2/TasaVES5)))
        # ====================================================================
        col_monto_capex_ord_usd = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_monto_capex_ord_usd, 'Monto Capex ORD USD', header_format)
        worksheet.set_column(col_monto_capex_ord_usd, col_monto_capex_ord_usd, 20)
        
        for row in range(1, num_filas + 1):
            excel_row = row + 1
            formula = (
                f'=IF({moneda_pago_letter}{excel_row}="USD",{monto_capex_ord_2_letter}{excel_row},'
                f'IF({moneda_pago_letter}{excel_row}="EUR",{monto_capex_ord_2_letter}{excel_row}*{tasa_eur_ref},'
                f'IF({dia_pago_letter}{excel_row}="MARTES",{monto_capex_ord_2_letter}{excel_row}/{tasa_normal_ref},'
                f'{monto_capex_ord_2_letter}{excel_row}/{tasa_mas_5_ref})))'
            )
            worksheet.write_formula(row, col_monto_capex_ord_usd, formula, money_format)
        
        monto_capex_ord_usd_letter = indice_a_letra_excel(col_monto_capex_ord_usd)
        print(f"[THREAD-EXCEL] Columna 'Monto Capex ORD USD' agregada")
        
        # ====================================================================
        # COLUMNA 16: Monto Capex EXT USD
        # Fórmula: =IF(MonedaPago="USD",MontoCapexExt3,IF(MonedaPago="COP",MontoCapexExt3,IF(MonedaPago="EUR",MontoCapexExt3*TasaEUR,IF(DiaPago="MARTES",MontoCapexExt3/TasaVES,MontoCapexExt3/TasaVES5))))
        # ====================================================================
        col_monto_capex_ext_usd = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_monto_capex_ext_usd, 'Monto Capex EXT USD', header_format)
        worksheet.set_column(col_monto_capex_ext_usd, col_monto_capex_ext_usd, 20)
        
        for row in range(1, num_filas + 1):
            excel_row = row + 1
            formula = (
                f'=IF({moneda_pago_letter}{excel_row}="USD",{monto_capex_ext_3_letter}{excel_row},'
                f'IF({moneda_pago_letter}{excel_row}="COP",{monto_capex_ext_3_letter}{excel_row},'
                f'IF({moneda_pago_letter}{excel_row}="EUR",{monto_capex_ext_3_letter}{excel_row}*{tasa_eur_ref},'
                f'IF({dia_pago_letter}{excel_row}="MARTES",{monto_capex_ext_3_letter}{excel_row}/{tasa_normal_ref},'
                f'{monto_capex_ext_3_letter}{excel_row}/{tasa_mas_5_ref}))))'
            )
            worksheet.write_formula(row, col_monto_capex_ext_usd, formula, money_format)
        
        monto_capex_ext_usd_letter = indice_a_letra_excel(col_monto_capex_ext_usd)
        print(f"[THREAD-EXCEL] Columna 'Monto Capex EXT USD' agregada")
        
        # ====================================================================
        # COLUMNA 17: Monto CAPEX USD
        # Fórmula: =IF(MonedaPago="USD",MontoCapexFinal,IF(MonedaPago="COP",MontoCapexFinal,IF(MonedaPago="EUR",MontoCapexFinal*TasaEUR,IF(DiaPago="MARTES",MontoCapexFinal/TasaVES,MontoCapexFinal/TasaVES5))))
        # ====================================================================
        col_monto_capex_usd = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_monto_capex_usd, 'Monto CAPEX USD', header_format)
        worksheet.set_column(col_monto_capex_usd, col_monto_capex_usd, 18)
        
        for row in range(1, num_filas + 1):
            excel_row = row + 1
            formula = (
                f'=IF({moneda_pago_letter}{excel_row}="USD",{monto_capex_final_letter}{excel_row},'
                f'IF({moneda_pago_letter}{excel_row}="COP",{monto_capex_final_letter}{excel_row},'
                f'IF({moneda_pago_letter}{excel_row}="EUR",{monto_capex_final_letter}{excel_row}*{tasa_eur_ref},'
                f'IF({dia_pago_letter}{excel_row}="MARTES",{monto_capex_final_letter}{excel_row}/{tasa_normal_ref},'
                f'{monto_capex_final_letter}{excel_row}/{tasa_mas_5_ref}))))'
            )
            worksheet.write_formula(row, col_monto_capex_usd, formula, money_format)
        
        monto_capex_usd_letter = indice_a_letra_excel(col_monto_capex_usd)
        print(f"[THREAD-EXCEL] Columna 'Monto CAPEX USD' agregada")
        
        # ====================================================================
        # COLUMNA 18: Monto OPEX USD
        # Fórmula: =IF(MonedaPago="USD",MontoOpexFinal,IF(MonedaPago="EUR",MontoOpexFinal*TasaEUR,IF(DiaPago="MARTES",MontoOpexFinal/TasaVES,MontoOpexFinal/TasaVES5)))
        # ====================================================================
        col_monto_opex_usd = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_monto_opex_usd, 'Monto OPEX USD', header_format)
        worksheet.set_column(col_monto_opex_usd, col_monto_opex_usd, 18)
        
        for row in range(1, num_filas + 1):
            excel_row = row + 1
            formula = (
                f'=IF({moneda_pago_letter}{excel_row}="USD",{monto_opex_final_letter}{excel_row},'
                f'IF({moneda_pago_letter}{excel_row}="EUR",{monto_opex_final_letter}{excel_row}*{tasa_eur_ref},'
                f'IF({dia_pago_letter}{excel_row}="MARTES",{monto_opex_final_letter}{excel_row}/{tasa_normal_ref},'
                f'{monto_opex_final_letter}{excel_row}/{tasa_mas_5_ref})))'
            )
            worksheet.write_formula(row, col_monto_opex_usd, formula, money_format)
        
        monto_opex_usd_letter = indice_a_letra_excel(col_monto_opex_usd)
        print(f"[THREAD-EXCEL] Columna 'Monto OPEX USD' agregada")
        
        # ====================================================================
        # COLUMNA 19: Monto Total USD
        # Fórmula: =MontoCapexUSD + MontoOpexUSD
        # ====================================================================
        col_monto_total_usd = col_formula_idx
        col_formula_idx += 1
        worksheet.write(0, col_monto_total_usd, 'Monto Total USD', header_format)
        worksheet.set_column(col_monto_total_usd, col_monto_total_usd, 18)
        
        for row in range(1, num_filas + 1):
            excel_row = row + 1
            formula = f'={monto_capex_usd_letter}{excel_row}+{monto_opex_usd_letter}{excel_row}'
            worksheet.write_formula(row, col_monto_total_usd, formula, money_format)
        
        monto_total_usd_letter = indice_a_letra_excel(col_monto_total_usd)
        print(f"[THREAD-EXCEL] Columna 'Monto Total USD' agregada")
        
        # ====================================================================
        # FILA DE TOTALES
        # ====================================================================
        fila_totales = num_filas + 2
        worksheet.write(fila_totales, 0, 'TOTALES', header_format)
        
        # Agregar fórmulas de suma para columnas de monto originales
        columnas_suma = ['Monto', 'Monto CAPEX EXT', 'Monto CAPEX ORD', 'Monto CADM']
        for col_name in columnas_suma:
            if col_name in col_indices:
                col_idx = col_indices[col_name]
                col_letter = indice_a_letra_excel(col_idx)
                formula = f'=SUM({col_letter}2:{col_letter}{num_filas + 1})'
                worksheet.write_formula(fila_totales, col_idx, formula, formula_format)
        
        # Suma para nuevas columnas (Monto Final, Monto Capex Final, Monto Opex Final)
        worksheet.write_formula(fila_totales, col_monto_final, 
                               f'=SUM({monto_final_letter}2:{monto_final_letter}{num_filas + 1})', 
                               formula_format)
        worksheet.write_formula(fila_totales, col_monto_capex_final, 
                               f'=SUM({monto_capex_final_letter}2:{monto_capex_final_letter}{num_filas + 1})', 
                               formula_format)
        worksheet.write_formula(fila_totales, col_monto_opex_final, 
                               f'=SUM({monto_opex_final_letter}2:{monto_opex_final_letter}{num_filas + 1})', 
                               formula_format)
        
        # Suma para nuevas columnas (Monto Capex ORD 2, Monto Capex EXT 3)
        worksheet.write_formula(fila_totales, col_monto_capex_ord_2, 
                               f'=SUM({monto_capex_ord_2_letter}2:{monto_capex_ord_2_letter}{num_filas + 1})', 
                               formula_format)
        worksheet.write_formula(fila_totales, col_monto_capex_ext_3, 
                               f'=SUM({monto_capex_ext_3_letter}2:{monto_capex_ext_3_letter}{num_filas + 1})', 
                               formula_format)
        
        # Suma para columnas USD
        worksheet.write_formula(fila_totales, col_monto_capex_ord_usd, 
                               f'=SUM({monto_capex_ord_usd_letter}2:{monto_capex_ord_usd_letter}{num_filas + 1})', 
                               formula_format)
        worksheet.write_formula(fila_totales, col_monto_capex_ext_usd, 
                               f'=SUM({monto_capex_ext_usd_letter}2:{monto_capex_ext_usd_letter}{num_filas + 1})', 
                               formula_format)
        
        # Suma para columnas finales USD
        worksheet.write_formula(fila_totales, col_monto_capex_usd, 
                               f'=SUM({monto_capex_usd_letter}2:{monto_capex_usd_letter}{num_filas + 1})', 
                               formula_format)
        worksheet.write_formula(fila_totales, col_monto_opex_usd, 
                               f'=SUM({monto_opex_usd_letter}2:{monto_opex_usd_letter}{num_filas + 1})', 
                               formula_format)
        worksheet.write_formula(fila_totales, col_monto_total_usd, 
                               f'=SUM({monto_total_usd_letter}2:{monto_total_usd_letter}{num_filas + 1})', 
                               formula_format)
        
        # Freeze panes (fijar encabezado)
        worksheet.freeze_panes(1, 0)
        
        total_columnas_formula = col_formula_idx - num_cols
        print(f"[THREAD-EXCEL] Excel creado con {num_filas} filas y {num_cols + total_columnas_formula} columnas")
    
    return {
        'file_path': str(output_path),
        'file_name': output_path.name,
        'filas': num_filas,
        'columnas_originales': num_cols,
        'columnas_con_formulas': col_formula_idx,
        'tasas': {
            'tasa_ves_usd': tasa_ves_usd,
            'tasa_ves_usd_mas_5': tasa_ves_usd_mas_5
        },
        'columnas_agregadas': [
            'Moneda Pago',
            'Cuenta Bancaria', 
            'Dia de Pago',
            'Monto Final',
            'Monto Capex Final',
            'Monto Opex Final',
            'AREA',
            'Tipo Capex 2',
            'Tipo Capex',
            'Monto Capex ORD 2',
            'Monto Capex EXT 3',
            'Monto Capex ORD USD',
            'Monto Capex EXT USD',
            'Monto CAPEX USD',
            'Monto OPEX USD',
            'Monto Total USD'
        ],
        'hojas_adicionales': ['Tasa', 'Areas']
    }


def procesar_excel_thread(file_content: bytes, sheet_name: Optional[str], resultado: ResultadoThread):
    """
    Thread 2: Crea el Excel con la hoja 'Detalle' y fórmulas.
    """
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Iniciando generación de Excel...")
    
    try:
        # Leer datos (mismo proceso que Thread 1 pero independiente)
        df = leer_excel_con_cabezales(file_content, sheet_name)
        df_limpio = limpiar_datos(df)
        
        # NO calculamos las columnas adicionales aquí porque serán fórmulas en Excel
        # Las columnas se agregarán como fórmulas en crear_excel_con_formulas
        
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
