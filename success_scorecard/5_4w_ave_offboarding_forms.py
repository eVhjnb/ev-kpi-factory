# success_scorecard/5_4w_ave_offboarding_forms.py

"""
KPI 5 – 4 Week Average (derived KPI)

Ejemplo de KPI derivado basado en:
- Valores previos de un KPI base en la tabla de scorecard
- Cálculo del promedio de las últimas 4 semanas
- Inserción del resultado en la misma tabla de scorecard

Este patrón sirve para smoothing / trailing averages.
"""

from datetime import datetime, timedelta

from core.common_dates import get_last_sunday, get_year_week
from core.common_db import fetch_single_value, insert_scorecard_record


# -------------------------------------------------------------------
# 1) Parámetros genéricos del KPI derivado
# -------------------------------------------------------------------

# CUSTOMIZAR SEGÚN TU CONFIGURACIÓN REAL

# Tabla de scorecard donde ya están almacenados los KPIs base
SCORECARD_TABLE = "vl_analytics.scorecard_vl02"

# Scorecard y KPI base desde el cual calcularemos el promedio
BASE_SC_NAME = "Success"      # Scorecard del KPI base
BASE_KPI_NUMBER = "06"        # KPI base 

# Nuevo KPI (este archivo)
DERIVED_KPI_NUMBER = "05"
DERIVED_FIELD_NAME = "4 Week Ave. Offboarding Forms Completed"


# -------------------------------------------------------------------
# 2) Cálculo del KPI derivado (promedio 4 semanas)
# -------------------------------------------------------------------

def build_query(last_sunday_str: str) -> str:
    """
    Construye un query que calcula el promedio de field_value
    del KPI base en las últimas 4 semanas (incluyendo la semana
    del último domingo).
    """

    # Último domingo como fecha final del rango
    end_date = datetime.strptime(last_sunday_str, "%Y-%m-%d")

    # Fecha inicial = 4 semanas antes (28 días)
    start_date = (end_date - timedelta(weeks=4)).strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    query = f"""
        SELECT
            COALESCE(AVG(field_value), 0) AS avg_last_4_weeks
        FROM {SCORECARD_TABLE}
        WHERE sc_name = '{BASE_SC_NAME}'
          AND kpi_number = '{BASE_KPI_NUMBER}'
          AND range_type = 'weekly'
          AND last_sunday >= '{start_date}'
          AND last_sunday <= '{end_date_str}';
    """

    return query


def calculate_kpi_value(last_sunday_str: str) -> float:
    """
    Ejecuta el query de promedio y regresa el valor numérico.
    """
    query = build_query(last_sunday_str)
    result = fetch_single_value(query)
    return float(result or 0.0)


# -------------------------------------------------------------------
# 3) Wrapper para integrarlo al scorecard
# -------------------------------------------------------------------

def run_kpi_5():
    """
    Ejecuta el KPI 5 (promedio 4 semanas) y lo inserta en la tabla
    de scorecard como un KPI derivado.
    """

    # Fechas base
    last_sunday = get_last_sunday()
    last_sunday_str = last_sunday.strftime("%Y-%m-%d")
    year_week_str = get_year_week(last_sunday)
    year_week_num = year_week_str[-2:]
    now = datetime.now()
    timestamp_time = now.strftime("%Y-%m-%d %H:%M")
    year = last_sunday.year

    # Calcular valor del promedio 4 semanas
    avg_value = calculate_kpi_value(last_sunday_str)

    # Insertar en la tabla de scorecard
    insert_scorecard_record(
        table_name=SCORECARD_TABLE,
        year=year,
        print_date=timestamp_time,
        sc_name=BASE_SC_NAME,  # normalmente mismo scorecard que el KPI base
        last_sunday=last_sunday_str,
        kpi_number=DERIVED_KPI_NUMBER,
        range_type="weekly",
        week_month=year_week_num,
        field_name=DERIVED_FIELD_NAME,
        field_details=f"Average of KPI {BASE_KPI_NUMBER} over last 4 weeks",
        field_value=avg_value,
    )

    # Log
    print("---------------------------------------------")
    print(f"Scorecard: {BASE_SC_NAME}")
    print(f"KPI {DERIVED_KPI_NUMBER} – {DERIVED_FIELD_NAME}")
    print(f"Year: {year}")
    print(f"Week: {year_week_num}")
    print(f"Last Sunday: {last_sunday_str}")
    print(f"4-week average value: {avg_value}")
    print("---------------------------------------------")


if __name__ == "__main__":
    run_kpi_5()
