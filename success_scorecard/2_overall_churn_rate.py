# success_scorecard/32_overall_churn_rate.py

"""
KPI 32 – Overall Churn Rate (52 weeks, weekly)

Ejemplo de KPI:
- Tabla de acuerdos / contratos en DWH
- Cálculo de churn real en últimas 52 semanas
- Inserción en tabla de scorecard vía core.common_db
"""

from datetime import datetime, timedelta

from core.common_dates import get_last_sunday, get_year_week
from core.common_db import fetch_single_value, insert_scorecard_record


# -------------------------------------------------------------------
# 1) Parámetros genéricos para el modelo de churn
# -------------------------------------------------------------------

# CUSTOMIZAR NOMBRES Y VALORES SEGÚN TU ESQUEMA EN DWH

# Tabla de acuerdos / contratos
AGREEMENTS_TABLE = "SCHEMA_NAME.AGREEMENTS_TABLE"  # ej. "stg_hubspot.agreements"

# Columnas mínimas necesarias
COL_AGREEMENT_ID = "agreement_id"          # identificador único de acuerdo
COL_CLIENT_TYPE = "client_type"           # para filtrar internos/test
COL_STATUS = "agreement_status"           # estado del acuerdo
COL_START_DATE = "start_date"             # cuándo inicia
COL_END_DATE = "end_date"                 # cuándo termina (si aplica)

# Valores que identifican tipo de cliente
INTERNAL_CLIENT_VALUE = "INTERNAL" 
TEST_CLIENT_VALUE = "TEST" 

# Valores de estado de acuerdo
STATUS_ACTIVE = "ACTIVE"                 
STATUS_TERMINATED_BY_CLIENT = "TERM_CLIENT"
STATUS_TERMINATED_OTHER = "TERM_OTHER"     

# Ventana de análisis (en semanas)
WINDOW_WEEKS = 52


# -------------------------------------------------------------------
# 2) Construcción del query para churn
# -------------------------------------------------------------------

def build_query(last_sunday_str: str) -> str:
    """
    Construye un query SQL genérico para calcular churn real
    en una ventana de 52 semanas a partir del último domingo.

    churn = acuerdos terminados por cliente en ventana /
            acuerdos expuestos en ventana
    """

    # Punto final = último domingo
    end_date = last_sunday_str

    # Punto inicial = 52 semanas antes
    start_date = (
        datetime.strptime(last_sunday_str, "%Y-%m-%d") - timedelta(weeks=WINDOW_WEEKS)
    ).strftime("%Y-%m-%d")

    query = f"""
    WITH base_agreements AS (
        SELECT
            {COL_AGREEMENT_ID} AS agreement_id,
            {COL_CLIENT_TYPE}  AS client_type,
            {COL_STATUS}       AS status,
            {COL_START_DATE}   AS start_date,
            {COL_END_DATE}     AS end_date
        FROM {AGREEMENTS_TABLE}
        WHERE {COL_START_DATE} IS NOT NULL
          AND {COL_START_DATE} <= '{end_date}'
          -- Opcional: excluir acuerdos muy viejos si quieres acotar más
    ),

    filtered_agreements AS (
        SELECT *
        FROM base_agreements
        WHERE client_type NOT IN ('{INTERNAL_CLIENT_VALUE}', '{TEST_CLIENT_VALUE}')
    ),

    churned_in_window AS (
        SELECT COUNT(DISTINCT agreement_id) AS churned_count
        FROM filtered_agreements
        WHERE status IN ('{STATUS_TERMINATED_BY_CLIENT}', '{STATUS_TERMINATED_OTHER}')
          AND end_date IS NOT NULL
          AND end_date >= '{start_date}'
          AND end_date <= '{end_date}'
    ),

    exposed_in_window AS (
        -- acuerdos que estuvieron "vivos" en algún momento de la ventana
        SELECT COUNT(DISTINCT agreement_id) AS exposed_count
        FROM filtered_agreements
        WHERE start_date <= '{end_date}'
          AND (
                end_date IS NULL
             OR end_date >= '{start_date}'
          )
    )

    SELECT
        CASE
            WHEN exposed_count = 0 THEN 0::FLOAT
            ELSE churned_count::FLOAT / exposed_count::FLOAT
        END AS churn_rate
    FROM churned_in_window, exposed_in_window;
    """

    return query


def calculate_kpi_value(last_sunday_str: str) -> float:
    """
    Calcula el churn rate ejecutando el query en DWH y
    devolviendo un valor numérico (float).
    """
    query = build_query(last_sunday_str)
    result = fetch_single_value(query)
    return float(result or 0.0)


# -------------------------------------------------------------------
# 3) Wrapper para integrarlo al scorecard
# -------------------------------------------------------------------

def run_kpi_32():
    """
    Ejecuta el KPI 32 y lo inserta en la tabla de scorecard.
    """

    # Fechas base
    last_sunday = get_last_sunday()
    last_sunday_str = last_sunday.strftime("%Y-%m-%d")
    year_week_str = get_year_week(last_sunday)
    year_week_num = year_week_str[-2:]
    now = datetime.now()
    timestamp_time = now.strftime("%Y-%m-%d %H:%M")
    year = last_sunday.year

    # Calcular valor de churn en la ventana definida
    churn_value = calculate_kpi_value(last_sunday_str)

    # CUSTOMIZAR NOMBRE DE TABLA DE SCORECARD SI ES NECESARIO
    TABLE_NAME = "vl_analytics.scorecard_vl02"

    # Insertar en la tabla de scorecard
    insert_scorecard_record(
        table_name=TABLE_NAME,
        year=year,
        print_date=timestamp_time,
        sc_name="Success",   # TODO: cambiar si este KPI va a otro scorecard
        last_sunday=last_sunday_str,
        kpi_number="32",
        range_type="weekly",
        week_month=year_week_num,
        field_name="Overall churn [real churn] - (52 weeks)",
        field_details=f"Window start: {WINDOW_WEEKS} weeks before last Sunday",
        field_value=churn_value,
    )

    # Log
    print("---------------------------------------------")
    print("Scorecard: Success")
    print("KPI 32 – Overall churn [real churn] - (52 weeks)")
    print(f"Year: {year}")
    print(f"Week: {year_week_num}")
    print(f"Last Sunday: {last_sunday_str}")
    print(f"Churn value: {churn_value}")
    print("---------------------------------------------")


if __name__ == "__main__":
    run_kpi_32()
