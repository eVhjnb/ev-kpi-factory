# core/common_db.py

import os
import psycopg2
from contextlib import contextmanager


DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
}


@contextmanager
def get_connection():
    """Context manager para conexión a la DB."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
    finally:
        if conn is not None:
            conn.close()


def fetch_single_value(query: str, params: tuple | None = None):
    """
    Ejecuta un query que devuelve un solo valor (ej. SELECT ...).
    Devuelve el primer valor o 0 si no hay resultados.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                result = cur.fetchone()
                return result[0] if result else 0
    except Exception as e:
        print(f"[fetch_single_value] Error: {e}")
        return None


def insert_scorecard_record(
    table_name: str,
    year: int,
    print_date: str,
    sc_name: str,
    last_sunday: str,
    kpi_number: str,
    range_type: str,
    week_month: str,
    field_name: str,
    field_details: str | None,
    field_value: float | int | None,
):
    """
    Inserta un registro en la tabla de scorecard.

    table_name: normalmente 'vl_analytics.scorecard_vl02' o similar.
    """
    sql = f"""
        INSERT INTO {table_name}
        ("year", "print_date", "sc_name", "last_sunday",
         "kpi_number", "range_type", "week_month",
         "field_name", "field_details", "field_value")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        year,
                        print_date,
                        sc_name,
                        last_sunday,
                        kpi_number,
                        range_type,
                        week_month,
                        field_name,
                        field_details,
                        field_value,
                    ),
                )
            conn.commit()
    except Exception as e:
        print(f"[insert_scorecard_record] Error: {e}")
