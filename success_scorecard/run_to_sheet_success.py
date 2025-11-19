# success_scorecard/run_to_sheet_success.py

"""
SUCCESS SCORECARD → GOOGLE SHEETS (VOLCADO MASIVO POR SEMANA)

Flujo:
1. Calcula la week_month actual a partir del último domingo local.
2. Consulta en DWH cuál es el last_sunday registrado para esa week_month.
3. Trae TODOS los KPIs del scorecard 'SC_NAME' para ese last_sunday.
4. En el Google Sheet:
   - Busca (o crea) una columna en la fila 1 con ese last_sunday.
   - Recorre las filas de KPIs (columna A = número de KPI).
   - Escribe el valor de cada KPI en la columna de la semana.
"""

from datetime import datetime
import os

import gspread
from google.oauth2.service_account import Credentials

from core.common_dates import get_last_sunday, get_year_week
from core.common_db import get_connection


# -------------------------------------------------------------------
# 1) Parámetros de configuración (TODOs)
# -------------------------------------------------------------------

# Tabla del scorecard en DWH
SCORECARD_TABLE = "vl_analytics.scorecard_vl"  

# Nombre del scorecard (sc_name en la tabla)
SC_NAME = "Success" 

# ID del Google Sheet (parte del URL)
# https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit#gid=0
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID_HERE" 

# Nombre de la pestaña donde vive el scorecard (ej. "sc2025")
WORKSHEET_NAME = "sc2025"    
 anho = date.today().year
datetime.now()

# Índice de columna base donde empiezan las semanas (1 = A, 2 = B, 3 = C, ...)
# En tu ejemplo: semanas empiezan en la columna C => 3
BASE_WEEK_COL_INDEX = 3      

# Fila donde comienzan los KPIs (en tu ejemplo: fila 2)
KPI_ROWS_START = 2


# -------------------------------------------------------------------
# 2) Helpers Google Sheets
# -------------------------------------------------------------------

def get_gspread_client():
    """
    Devuelve un cliente de gspread usando Service Account.
    Requiere GOOGLE_APPLICATION_CREDENTIALS con la ruta al JSON.
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
    return gspread.authorize(creds)


def col_index_to_letter(col_index: int) -> str:
    """
    Convierte índice 1-based a letra de columna: 1->A, 2->B, 27->AA...
    """
    result = []
    while col_index > 0:
        col_index, remainder = divmod(col_index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


# -------------------------------------------------------------------
# 3) Lecturas desde DWH
# -------------------------------------------------------------------

def get_last_sunday_from_db(week_month: str) -> str | None:

    query = f"""
        SELECT DISTINCT last_sunday
        FROM {SCORECARD_TABLE}
        WHERE week_month = %s
          AND sc_name = %s
        ORDER BY last_sunday DESC
        LIMIT 1;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (week_month, SC_NAME))
            row = cur.fetchone()

    if row:
        # row[0] suele ser un date/datetime
        return row[0].strftime("%Y-%m-%d") if hasattr(row[0], "strftime") else str(row[0])
    return None


def fetch_kpis_for_last_sunday(last_sunday_str: str):
    query = f"""
        SELECT
            kpi_number,
            field_value
        FROM {SCORECARD_TABLE}
        WHERE sc_name = %s
          AND last_sunday = %s
        ORDER BY kpi_number::INT ASC;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (SC_NAME, last_sunday_str))
            rows = cur.fetchall()

    # Normalizamos kpi_number para que pueda matchear con lo del sheet
    # E.g. '05' y 5 => '5'
    kpi_values = {}
    for kpi_number, field_value in rows:
        if kpi_number is None:
            continue
        k_str = str(kpi_number).strip()
        # quitamos ceros a la izquierda para compararlo con el número del sheet
        k_norm = k_str.lstrip("0") or "0"
        kpi_values[k_norm] = field_value

    return kpi_values


# -------------------------------------------------------------------
# 4) Lógica de escritura en el Sheet (volcado masivo)
# -------------------------------------------------------------------

def find_or_create_week_column(ws, last_sunday_str: str) -> int:
    """
    Busca en la fila 1 una columna cuyo valor sea igual a last_sunday_str
    (o equivalente de fecha). Si no existe, crea una nueva columna al final.

    Devuelve el índice de columna (1-based).
    """
    max_col = ws.max_col if hasattr(ws, "max_col") else ws.col_count
    # Row 1: encabezados de semanas a partir de BASE_WEEK_COL_INDEX
    target_col = None

    for col in range(BASE_WEEK_COL_INDEX, max_col + 1):
        val = ws.cell(row=1, column=col).value
        if val is None:
            continue

        # Normalizamos valor: si es fecha, la pasamos a YYYY-MM-DD
        if hasattr(val, "strftime"):
            v_norm = val.strftime("%Y-%m-%d")
        else:
            v_norm = str(val).strip()

        if v_norm == last_sunday_str:
            target_col = col
            break

    if target_col is not None:
        return target_col

    # Si no se encontró, creamos una nueva columna a la derecha
    target_col = max_col + 1
    ws.update_cell(1, target_col, last_sunday_str)
    return target_col


def write_mass_dump_to_sheet(last_sunday_str: str, year_week_str: str, kpi_values: dict):
    """
    Escribe el volcado masivo:
    - Usa la columna A de la hoja (fila 2..N) como lista de KPI numbers.
    - Para cada fila, busca su kpi_number en kpi_values.
    - Escribe todos los valores en la columna de la semana correspondiente.
    """
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    # 1) Encontrar (o crear) columna para esta semana
    #    basada en el last_sunday que viene del DWH.
    target_col_index = find_or_create_week_column(ws, last_sunday_str)
    col_letter = col_index_to_letter(target_col_index)

    print(f"Publicando en columna {col_letter} (last_sunday = {last_sunday_str})")

    # 2) Leer KPIs definidos en la hoja (columna A, filas KPI_ROWS_START..N)
    #    y armar el vector de valores en el mismo orden de filas.
    #    kpi_values: dict con clave kpi_number_normalizado -> field_value
    max_row = ws.row_count
    values_matrix = []
    last_row_with_kpi = KPI_ROWS_START

    for row in range(KPI_ROWS_START, max_row + 1):
        kpi_id_cell = ws.cell(row=row, column=1).value  # Col A = KPI number
        if kpi_id_cell is None:
            # Si encontramos una fila completamente vacía, asumimos que no hay más KPIs.
            # Rompemos para no mandar un rango gigantesco innecesario.
            break

        kpi_id_norm = str(kpi_id_cell).strip()
        kpi_id_norm = kpi_id_norm.lstrip("0") or "0"

        value = kpi_values.get(kpi_id_norm)
        values_matrix.append([value])
        last_row_with_kpi = row

    # 3) Escribir los valores en la columna objetivo, de forma masiva
    start_row = KPI_ROWS_START
    end_row = last_row_with_kpi
    value_range = f"{col_letter}{start_row}:{col_letter}{end_row}"

    if values_matrix:
        ws.update(value_range, values_matrix)
        print(f"Valores escritos en rango {value_range}")
    else:
        print("No se encontraron filas de KPI para actualizar.")


# -------------------------------------------------------------------
# 5) Runner principal
# -------------------------------------------------------------------

def run_to_sheet_success():
    """
    Publica todos los KPIs del scorecard 'SC_NAME' para la week_month actual,
    usando el last_sunday que ya está en la tabla del scorecard.
    """

    print("=====================================================")
    print("   PUBLISHING SUCCESS SCORECARD TO GOOGLE SHEETS")
    print("   Timestamp:", datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("=====================================================")

    # 1) Calcular week_month localmente
    last_sunday_local = get_last_sunday()
    year_week_str = get_year_week(last_sunday_local)
    week_month = year_week_str[-2:]  # 'YYYY-WW' -> 'WW'

    print(f"Semana local (year-week): {year_week_str}  |  week_month: {week_month}")

    # 2) Preguntar al DWH cuál es el last_sunday para esa week_month y sc_name
    db_last_sunday_str = get_last_sunday_from_db(week_month)

    if not db_last_sunday_str:
        print("No se encontró last_sunday en DWH para esta week_month / sc_name. Abortando publicación.")
        return

    print(f"Último domingo (desde DWH): {db_last_sunday_str}")

    # 3) Leer TODOS los KPIs de ese last_sunday
    kpi_values = fetch_kpis_for_last_sunday(db_last_sunday_str)

    if not kpi_values:
        print("No se encontraron KPIs en el scorecard para esa fecha. Nada que publicar.")
        return

    # 4) Volcado masivo en el sheet
    write_mass_dump_to_sheet(db_last_sunday_str, year_week_str, kpi_values)

    print("PUBLICACIÓN FINALIZADA.")
    print("=====================================================")


if __name__ == "__main__":
    run_to_sheet_success()
