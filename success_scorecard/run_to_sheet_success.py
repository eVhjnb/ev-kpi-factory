# success_scorecard/run_to_sheet_success.py

from datetime import datetime

import os
import gspread
from google.oauth2.service_account import Credentials

from core.common_dates import get_last_sunday, get_year_week
from core.common_db import get_connection  # usamos la conexión genérica al DWH


# -------------------------------------------------------------------
# 1) Parámetros genéricos / TODOs de configuración
# -------------------------------------------------------------------

# tabla de scorecard en DWH
SCORECARD_TABLE = "vl_analytics.scorecard_vl02"

# nombre del scorecard (debe coincidir con los KPIs insertados)
SC_NAME = "Success"

# ID del Google Sheet de destino (NO el nombre, el ID del URL)
# https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit#gid=0
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID_HERE"

# nombre de la pestaña donde se escribe el scorecard
WORKSHEET_NAME = "SUCCESS_SCORECARD_SHEET"

# índice de columna base para la semana 1
# Ejemplo: 3 => columna "C"
BASE_WEEK_COLUMN_INDEX = 3

# fila donde comienzan los KPIs (encima de eso puedes tener headers)
KPI_ROWS_START = 3

# orden de KPIs / filas
# Lista ordenada de kpi_number en el mismo orden en que estarán en la hoja
KPI_ORDER = [
    "05",  # 4W Ave Offboarding Forms
    "16",  # Replacement Processes Existing Clients
    "32",  # Overall Churn 52 weeks
    # Agrega aquí KPIs adicionales en el orden en que van en la hoja
]


# -------------------------------------------------------------------
# 2) Helpers para Google Sheets
# -------------------------------------------------------------------

def get_gspread_client():
    """
    Devuelve un cliente de gspread usando Service Account.
    Requiere GOOGLE_APPLICATION_CREDENTIALS en el entorno.
    """
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not creds_path:
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS no está definida. "
            "Configura la variable de entorno con la ruta al JSON del Service Account."
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)
    return client


def col_index_to_letter(col_index: int) -> str:
    """
    Convierte un índice de columna (1-based) a letra de columna estilo A1.
    Ej: 1 -> 'A', 2 -> 'B', 27 -> 'AA'
    """
    result = []
    while col_index > 0:
        col_index, remainder = divmod(col_index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


# -------------------------------------------------------------------
# 3) Lectura de KPIs desde DWH
# -------------------------------------------------------------------

def fetch_kpis_for_last_sunday(last_sunday_str: str):
    """
    Lee los KPIs del scorecard 'SC_NAME' para el último domingo.
    Devuelve una lista de tuplas (kpi_number, field_name, field_value).
    """
    query = f"""
        SELECT
            kpi_number,
            field_name,
            field_value
        FROM {SCORECARD_TABLE}
        WHERE sc_name = %s
          AND last_sunday = %s
          AND kpi_number IN %s
        ORDER BY kpi_number::INT ASC;
    """

    # Para usar el IN con psycopg2, pasamos una tupla
    kpi_numbers_tuple = tuple(KPI_ORDER)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (SC_NAME, last_sunday_str, kpi_numbers_tuple))
            rows = cur.fetchall()

    # rows -> [(kpi_number, field_name, field_value), ...]
    return rows


# -------------------------------------------------------------------
# 4) Escritura en Google Sheets
# -------------------------------------------------------------------

def write_to_google_sheet(last_sunday_str: str, year_week_str: str, rows):
    """
    Escribe:
    - La fecha de corte (last_sunday_str) en la fila de encabezado.
    - Los valores de cada KPI en la columna correspondiente a la semana.
    """

    # 1) Calcular columna destino en función de la semana
    week_num = int(year_week_str[-2:])  # 'YYYY-WW' -> 'WW'
    target_col_index = BASE_WEEK_COLUMN_INDEX + week_num - 1
    col_letter = col_index_to_letter(target_col_index)

    # 2) Preparar datos en orden según KPI_ORDER
    #    Creamos un dict {kpi_number: field_value}
    values_by_kpi = {kpi: None for kpi in KPI_ORDER}
    for kpi_number, field_name, field_value in rows:
        if kpi_number in values_by_kpi:
            values_by_kpi[kpi_number] = field_value

    # Lista final de valores en el orden de KPI_ORDER
    kpi_values_ordered = [
        values_by_kpi[kpi_number] for kpi_number in KPI_ORDER
    ]

    # 3) Conectarnos a Google Sheets
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    # 4) Escribir la fecha de corte en la fila 1 (header de semana)
    header_cell = f"{col_letter}1"
    ws.update(header_cell, [[last_sunday_str]])

    # 5) Escribir los valores KPI desde KPI_ROWS_START hacia abajo
    start_row = KPI_ROWS_START
    end_row = KPI_ROWS_START + len(kpi_values_ordered) - 1
    value_range = f"{col_letter}{start_row}:{col_letter}{end_row}"

    # Construimos una matriz de una sola columna
    values_matrix = [[v] for v in kpi_values_ordered]

    ws.update(value_range, values_matrix)

    print(f"Datos escritos en columna {col_letter} (semana {week_num}).")


# -------------------------------------------------------------------
# 5) Runner principal
# -------------------------------------------------------------------

def run_to_sheet_success():
    """
    Runner para publicar los KPIs de Success en Google Sheets.
    """

    print("=====================================================")
    print("   PUBLISHING SUCCESS SCORECARD TO GOOGLE SHEETS")
    print("   Timestamp:", datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("=====================================================")

    # Fechas base
    last_sunday = get_last_sunday()
    last_sunday_str = last_sunday.strftime("%Y-%m-%d")
    year_week_str = get_year_week(last_sunday)

    print(f"Último domingo: {last_sunday_str} (week {year_week_str})")

    # 1) Obtener KPIs de DWH
    rows = fetch_kpis_for_last_sunday(last_sunday_str)

    if not rows:
        print("No se encontraron registros de KPIs para esta fecha.")
        return

    # 2) Escribir en Google Sheets
    write_to_google_sheet(last_sunday_str, year_week_str, rows)

    print("PUBLICACIÓN FINALIZADA.")
    print("=====================================================")


if __name__ == "__main__":
    run_to_sheet_success()
