# core/common_dates.py

from datetime import date, timedelta


def get_last_sunday(reference_date: date | None = None) -> date:
    """
    Devuelve el último domingo a partir de la fecha de referencia.
    Si no se recibe fecha, usa hoy.
    """
    if reference_date is None:
        reference_date = date.today()
    weekday = reference_date.weekday()  # 0 = lunes, 6 = domingo
    return reference_date - timedelta(days=weekday + 1)


def get_year_week(fecha: date) -> str:
    """
    Devuelve el año-semana en formato 'YYYY-WW' usando ISO week.
    """
    iso = fecha.isocalendar()
    return f"{iso.year}-{iso.week:02d}"
