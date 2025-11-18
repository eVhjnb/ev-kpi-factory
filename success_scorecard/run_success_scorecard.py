# success_scorecard/run_success_scorecard.py

"""
SUCCESS SCORECARD – RUNNER

Este script ejecuta todos los KPIs del scorecard "Success".
Actúa como orquestador: importa cada KPI, los ejecuta uno por uno
y deja el resultado en la tabla del scorecard definida en cada KPI.

Este archivo se usa típicamente con un CRON que corre una vez por semana.
"""

from datetime import datetime

# Importar KPIs individuales
# ------------------------------------------------------------
# Cada KPI tiene su propio archivo y un método run_kpi_XX()

from success_scorecard.16_replacement_processes_existing_clients import run_kpi_16
from success_scorecard.32_overall_churn_rate import run_kpi_32

# aqui se agregan mas KPIS
# from success_scorecard.00_name_of_kpi import run_kpi_00
# ...


def run_success_scorecard():
    """
    Ejecuta todos los KPIs del domain Success.
    """

    print("=====================================================")
    print("   RUNNING SUCCESS SCORECARD")
    print("   Timestamp:", datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("=====================================================")

    # ------------------------
    # Lista de KPIs a ejecutar
    # ------------------------
    kpis = [
        ("KPI 16 – Replacement Processes", run_kpi_16),
        ("KPI 32 – Overall Churn 52 weeks", run_kpi_32),
        # Agregar aquí mas:
        #("KPI 00 – name of kpi", run_kpi_00)
    ]

    # ------------------------
    # Ejecución secuencial
    # ------------------------
    for label, kpi_function in kpis:
        print(f"\n>>> Ejecutando {label}...")
        try:
            kpi_function()
            print(f"OK – {label} finalizado.")
        except Exception as e:
            print(f"ERROR en {label}: {e}")

    print("\n=====================================================")
    print("   SUCCESS SCORECARD – FINALIZADO")
    print("=====================================================")


if __name__ == "__main__":
    run_success_scorecard()
