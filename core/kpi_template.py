# core/kpi_template.py

from datetime import datetime
from typing import Callable

from core.common_dates import get_last_sunday, get_year_week
from core.common_db import fetch_single_value, insert_scorecard_record


def run_kpi(
    sc_name: str,
    kpi_number: str,
    range_type: str,
    field_name: str,
    field_details: str | None,
    build_query_func: Callable[[str, str], str],
    table_name: str = "vl_analytics.scorecard_vl02",
):
    """
    Ejecuta un KPI usando:
    - un nombre de scorecard (sc_name)
    - un número de KPI
    - un tipo de rango (ej. 'weekly', 'monthly')
    - nombre y detalles del campo
    - una función que construye el query (build_query_func)
    - el nombre de la tabla de destino en DWH

    build_query_func(last_sunday_str, year_week_str) -> str
    """
    # 1) Fechas de referencia
    last_sunday = get_last_sunday()
    last_sunday_str = last_sunday.strftime("%Y-%m-%d")
    year_week = get_year_week(last_sunday)
    year_week_num = year_week[-2:]  # 'YYYY-WW' -> 'WW'

    now = datetime.now()
    timestamp_time = now.strftime("%Y-%m-%d %H:%M")
    year = last_sunday.year

    # 2) Construir query específico del KPI
    query = build_query_func(last_sunday_str, year_week)

    # 3) Ejecutar query y obtener valor
    result_value = fetch_single_value(query)

    # 4) Insertar en DWH (scorecard)
    insert_scorecard_record(
        table_name=table_name,
        year=year,
        print_date=timestamp_time,
        sc_name=sc_name,
        last_sunday=last_sunday_str,
        kpi_number=kpi_number,
        range_type=range_type,
        week_month=year_week_num,
        field_name=field_name,
        field_details=field_details,
        field_value=result_value,
    )

    # 5) Log sencillo en stdout (para revisar en cron)
    print("---------------------------------------------")
    print(f"Scorecard: {sc_name}")
    print(f"KPI: {kpi_number} – {field_name}")
    print(f"Year: {year}")
    print(f"Print date: {timestamp_time}")
    print(f"Last Sunday: {last_sunday_str}")
    print(f"Range type: {range_type}")
    print(f"Week: {year_week_num}")
    print(f"Field details: {field_details}")
    print(f"Field value: {result_value}")
    print("---------------------------------------------")
