"""
Módulo para consultar tasas de cambio de diferentes monedas a USD.
- Bolívar (VES) a USD: DolarAPI Venezuela
- Peso Colombiano (COP) a USD: DolarAPI Colombia  
- Euro (EUR) a USD: Frankfurter API
"""
import requests
from typing import Dict, Optional
from datetime import datetime


# URLs de las APIs
DOLARAPI_VENEZUELA_URL = "https://ve.dolarapi.com/v1/dolares"
DOLARAPI_COLOMBIA_URL = "https://co.dolarapi.com/v1/cotizaciones/usd"
FRANKFURTER_EUR_USD_URL = "https://api.frankfurter.app/latest?from=EUR&to=USD"

# Timeout para las peticiones HTTP (segundos)
REQUEST_TIMEOUT = 10


def obtener_tasa_bolivar_dolar() -> Dict:
    """
    Consulta la tasa de cambio del Bolívar (VES) al Dólar (USD).
    Usa DolarAPI Venezuela.
    
    Returns:
        Dict con la información de la tasa:
        {
            'success': bool,
            'moneda_origen': 'VES',
            'moneda_destino': 'USD',
            'tasa': float,  # Cuántos bolívares por 1 USD
            'fuente': str,
            'fecha': str,
            'timestamp': str,
            'error': str (opcional)
        }
    """
    print("[TASA] Consultando tasa VES/USD desde DolarAPI Venezuela...")
    
    try:
        response = requests.get(DOLARAPI_VENEZUELA_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        
        # DolarAPI Venezuela retorna una lista de diferentes fuentes
        # Buscamos el "dolar oficial" o "bcv" (Banco Central de Venezuela)
        tasa_bcv = None
        tasa_paralelo = None
        
        for cotizacion in data:
            nombre = cotizacion.get('nombre', '').lower()
            if 'bcv' in nombre or 'oficial' in nombre:
                tasa_bcv = cotizacion
            elif 'paralelo' in nombre:
                tasa_paralelo = cotizacion
        
        # Preferimos BCV, si no existe usamos paralelo
        cotizacion_usar = tasa_bcv or tasa_paralelo or (data[0] if data else None)
        
        if cotizacion_usar:
            # Obtener el valor de la tasa (puede venir como string "325.39" o número)
            tasa_promedio = cotizacion_usar.get('promedio', cotizacion_usar.get('compra', 0))
            tasa_compra = cotizacion_usar.get('compra', 0)
            tasa_venta = cotizacion_usar.get('venta', 0)
            
            # Convertir a float manejando strings
            def parse_tasa(valor):
                if valor is None:
                    return 0.0
                if isinstance(valor, (int, float)):
                    return float(valor)
                if isinstance(valor, str):
                    # Remover posibles caracteres no numéricos excepto punto y coma
                    valor = valor.strip().replace(',', '.')
                    try:
                        return float(valor)
                    except ValueError:
                        return 0.0
                return 0.0
            
            resultado = {
                'success': True,
                'moneda_origen': 'VES',
                'moneda_destino': 'USD',
                'tasa': parse_tasa(tasa_promedio),
                'tasa_compra': parse_tasa(tasa_compra),
                'tasa_venta': parse_tasa(tasa_venta),
                'fuente': cotizacion_usar.get('nombre', 'DolarAPI Venezuela'),
                'fecha': cotizacion_usar.get('fechaActualizacion', str(datetime.now().date())),
                'timestamp': datetime.now().isoformat()
            }
            print(f"[TASA] VES/USD: {resultado['tasa']} ({resultado['fuente']})")
            return resultado
        else:
            raise ValueError("No se encontraron cotizaciones en la respuesta")
            
    except requests.exceptions.RequestException as e:
        print(f"[TASA] ERROR al consultar VES/USD: {str(e)}")
        return {
            'success': False,
            'moneda_origen': 'VES',
            'moneda_destino': 'USD',
            'tasa': None,
            'error': f"Error de conexión: {str(e)}",
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"[TASA] ERROR al procesar VES/USD: {str(e)}")
        return {
            'success': False,
            'moneda_origen': 'VES',
            'moneda_destino': 'USD',
            'tasa': None,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def obtener_tasa_peso_colombiano_dolar() -> Dict:
    """
    Consulta la tasa de cambio del Peso Colombiano (COP) al Dólar (USD).
    Usa DolarAPI.
    
    Returns:
        Dict con la información de la tasa:
        {
            'success': bool,
            'moneda_origen': 'COP',
            'moneda_destino': 'USD',
            'tasa': float,  # Cuántos pesos por 1 USD
            'fuente': str,
            'fecha': str,
            'timestamp': str,
            'error': str (opcional)
        }
    """
    print("[TASA] Consultando tasa COP/USD desde DolarAPI...")
    
    try:
        response = requests.get(DOLARAPI_COLOMBIA_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        
        # La respuesta puede ser un objeto directo o una lista
        if isinstance(data, list):
            cotizacion = data[0] if data else None
        else:
            cotizacion = data
        
        if cotizacion:
            resultado = {
                'success': True,
                'moneda_origen': 'COP',
                'moneda_destino': 'USD',
                'tasa': float(cotizacion.get('promedio', cotizacion.get('compra', cotizacion.get('valor', 0)))),
                'tasa_compra': float(cotizacion.get('compra', 0)),
                'tasa_venta': float(cotizacion.get('venta', 0)),
                'fuente': cotizacion.get('nombre', cotizacion.get('fuente', 'DolarAPI Colombia')),
                'fecha': cotizacion.get('fechaActualizacion', str(datetime.now().date())),
                'timestamp': datetime.now().isoformat()
            }
            print(f"[TASA] COP/USD: {resultado['tasa']} ({resultado['fuente']})")
            return resultado
        else:
            raise ValueError("No se encontraron cotizaciones en la respuesta")
            
    except requests.exceptions.RequestException as e:
        print(f"[TASA] ERROR al consultar COP/USD: {str(e)}")
        return {
            'success': False,
            'moneda_origen': 'COP',
            'moneda_destino': 'USD',
            'tasa': None,
            'error': f"Error de conexión: {str(e)}",
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"[TASA] ERROR al procesar COP/USD: {str(e)}")
        return {
            'success': False,
            'moneda_origen': 'COP',
            'moneda_destino': 'USD',
            'tasa': None,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def obtener_tasa_euro_dolar() -> Dict:
    """
    Consulta la tasa de cambio del Euro (EUR) al Dólar (USD).
    Usa Frankfurter API (https://api.frankfurter.app).
    
    Returns:
        Dict con la información de la tasa:
        {
            'success': bool,
            'moneda_origen': 'EUR',
            'moneda_destino': 'USD',
            'tasa': float,  # Cuántos dólares por 1 EUR
            'fuente': str,
            'fecha': str,
            'timestamp': str,
            'error': str (opcional)
        }
    """
    print("[TASA] Consultando tasa EUR/USD desde Frankfurter API...")
    
    try:
        response = requests.get(FRANKFURTER_EUR_USD_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        
        # Estructura de respuesta:
        # {"amount":1.0,"base":"EUR","date":"2026-01-09","rates":{"USD":1.1642}}
        
        tasa_usd = data.get('rates', {}).get('USD')
        
        if tasa_usd:
            resultado = {
                'success': True,
                'moneda_origen': 'EUR',
                'moneda_destino': 'USD',
                'tasa': float(tasa_usd),
                'amount': float(data.get('amount', 1.0)),
                'fuente': 'Frankfurter API (ECB)',
                'fecha': data.get('date', str(datetime.now().date())),
                'timestamp': datetime.now().isoformat()
            }
            print(f"[TASA] EUR/USD: {resultado['tasa']} ({resultado['fuente']})")
            return resultado
        else:
            raise ValueError("No se encontró la tasa USD en la respuesta")
            
    except requests.exceptions.RequestException as e:
        print(f"[TASA] ERROR al consultar EUR/USD: {str(e)}")
        return {
            'success': False,
            'moneda_origen': 'EUR',
            'moneda_destino': 'USD',
            'tasa': None,
            'error': f"Error de conexión: {str(e)}",
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"[TASA] ERROR al procesar EUR/USD: {str(e)}")
        return {
            'success': False,
            'moneda_origen': 'EUR',
            'moneda_destino': 'USD',
            'tasa': None,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def obtener_todas_las_tasas() -> Dict:
    """
    Consulta todas las tasas de cambio disponibles.
    
    Returns:
        Dict con todas las tasas:
        {
            'VES_USD': {...},
            'COP_USD': {...},
            'EUR_USD': {...},
            'timestamp': str
        }
    """
    print("[TASA] Consultando todas las tasas...")
    
    resultado = {
        'VES_USD': obtener_tasa_bolivar_dolar(),
        'COP_USD': obtener_tasa_peso_colombiano_dolar(),
        'EUR_USD': obtener_tasa_euro_dolar(),
        'timestamp': datetime.now().isoformat()
    }
    
    # Resumen
    exitosas = sum(1 for k, v in resultado.items() if isinstance(v, dict) and v.get('success'))
    print(f"[TASA] Consulta completada: {exitosas}/3 tasas obtenidas")
    
    return resultado


# =============================================================================
# MAIN (para pruebas)
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Probando consulta de tasas de cambio")
    print("=" * 60)
    
    # Probar cada función
    print("\n1. Bolívar a Dólar:")
    ves = obtener_tasa_bolivar_dolar()
    print(f"   Resultado: {ves}")
    
    print("\n2. Peso Colombiano a Dólar:")
    cop = obtener_tasa_peso_colombiano_dolar()
    print(f"   Resultado: {cop}")
    
    print("\n3. Euro a Dólar:")
    eur = obtener_tasa_euro_dolar()
    print(f"   Resultado: {eur}")
    
    print("\n" + "=" * 60)

