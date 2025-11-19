# success_scorecard/16_replacement_processes_existing_clients.py

"""
KPI 16 – New Replacement Processes for Existing Clients (weekly)

Ejemplo de KPI basado en:
- Google Sheets (CS Weekly Report)
- Limpieza / normalización con DuckDB
- Inserción en tabla de scorecard vía core.common_db
"""

import os
import duckdb
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from core.common_dates import get_last_sunday, get_year_week
from core.common_db import insert_scorecard_record


# -------------------------------------------------------------------
# 1) Helpers para leer el Google Sheet
# -------------------------------------------------------------------

def get_gspread_client():
    """
    Crea un cliente de gspread usando un Service Account.
    Supone que la ruta al JSON está en la variable de entorno
    GOOGLE_APPLICATION_CREDENTIALS.
    """
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not creds_path:
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS no está definida. "
            "Configura la variable de entorno con la ruta al JSON del Service Account."
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)
    return client


def load_weekly_report(sheet_name: str, worksheet_name: str) -> pd.DataFrame:
    """
    Lee un Google Sheet y lo regresa como DataFrame.
    - sheet_name: nombre del archivo en Google Sheets
    - worksheet_name: pestaña específica
    """
    client = get_gspread_client()
    sh = client.open(sheet_name)
    ws = sh.worksheet(worksheet_name)

    data = ws.get_all_records()
    df = pd.DataFrame(data)

    return df


# -------------------------------------------------------------------
# 2) Cálculo del KPI con DuckDB
# -------------------------------------------------------------------

# CUSTOMIZAR ESTOS NOMBRES SEGÚN TU ENTORNO
SHEET_NAME = "CS_WEEKLY_SHEET_NAME"              # nombre del Google Sheet
WORKSHEET_NAME = "CS_WEEKLY_WORKSHEET"           # nombre de la pestaña
COLUMN_WEEK = "WEEK_REPORTED_COL"                # columna semana (ej. 'week_reported')
COLUMN_REPLACEMENTS = "REPLACEMENTS_COUNT_COL"   # columna con el número de replacements


def calculate_kpi_value(last_sunday_str: str, year_week_str: str) -> int:
    """
    Carga el sheet, limpia datos y calcula el KPI usando DuckDB.
    Este valor es el que finalmente se insertará en el scorecard.
    """

    # 1) Cargar datos desde Google Sheets
    df = load_weekly_report(SHEET_NAME, WORKSHEET_NAME)

    # 2) Normalizar columnas mínimas necesarias
    # Asegúrate de que COLUMN_WEEK y COLUMN_REPLACEMENTS existan en df
    df[COLUMN_WEEK] = df[COLUMN_WEEK].astype(str)

    # 3) Conectar DuckDB en memoria y registrar el DataFrame
    con = duckdb.connect(":memory:")
    con.register("weekly_report", df)

    # 4) Query en DuckDB:
    #    - Filtrar por la semana deseada (year_week_str)
    #    - Sumar el número de replacements de esa semana
    query_duck = f"""
        SELECT
            COALESCE(SUM("{COLUMN_REPLACEMENTS}"), 0) AS total_replacements
        FROM weekly_report
        WHERE {COLUMN_WEEK} = '{year_week_str}'
    """

    result = con.execute(query_duck).fetchone()
    con.close()

    total_replacements = result[0] if result else 0
    return int(total_replacements or 0)


# -------------------------------------------------------------------
# 3) Wrapper para integrarlo al scorecard
# -------------------------------------------------------------------

def run_kpi_16():
    ### Ejecuta el KPI 16 y lo inserta en la tabla del scorecard.
    
    from datetime import datetime

    # Fechas base
    last_sunday = get_last_sunday()
    last_sunday_str = last_sunday.strftime("%Y-%m-%d")
    year_week_str = get_year_week(last_sunday)
    year_week_num = year_week_str[-2:]
    now = datetime.now()
    timestamp_time = now.strftime("%Y-%m-%d %H:%M")
    year = last_sunday.year

    # Calcular el valor real del KPI
    value = calculate_kpi_value(last_sunday_str, year_week_str)

    # CUSTOMIZAR NOMBRE DE TABLA SI ES DIFERENTE
    TABLE_NAME = "vl_analytics.scorecard_vl02"

    # Insertar en la tabla de scorecard
    insert_scorecard_record(
        table_name=TABLE_NAME,
        year=year,
        print_date=timestamp_time,
        sc_name="Success",   # TODO: cambiar si usas otro nombre de scorecard
        last_sunday=last_sunday_str,
        kpi_number="16",
        range_type="weekly",
        week_month=year_week_num,
        field_name="New Replacement Processes for existing clients",  # texto visible
        field_details=None,
        field_value=value,
    )

    # Log
    print("---------------------------------------------")
    print("Scorecard: Success")
    print("KPI 16 – New Replacement Processes for existing clients")
    print(f"Year: {year}")
    print(f"Week: {year_week_num}")
    print(f"Last Sunday: {last_sunday_str}")
    print(f"Value: {value}")
    print("---------------------------------------------")


if __name__ == "__main__":
    run_kpi_16()
