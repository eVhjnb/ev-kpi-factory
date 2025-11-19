# eV KPI Factory
Automated KPI engine designed to process operational, commercial and service-related metrics from multiple data sources and publish weekly scorecards for different business domains.

This framework centralizes:
- KPI logic (one file = one KPI)
- standard metadata insertion
- DWH integration (PostgreSQL)
- Google Sheets publishing
- scheduling via cron
- date handling (weeks, cut-offs, print dates)

The result is a scalable and consistent workflow where 95% of the code is shared across KPIs, and only the query logic changes per metric.

___

## Architecture Overview
Trigger (cron)
|
├── run_success_scorecard.py
├── run_recruitment_scorecard.py
├── run_applications_scorecard.py
└── run_leadership_scorecard.py
|
|--- executes multiple kpi_XX.py scripts
|--- inserts results into DWH
|
└── run_to_sheet_*.py --> publishes result to Google Sheets


Data sources flow into a central DWH from:
- HubSpot (Airbyte integration)
- Jotform → Airtable → Webhook → Python server → DWH
- Google Sheets → Python pipeline on Hetzner → DWH

Each KPI script performs a focused calculation using:
- SQL queries (direct to DWH)
- DuckDB for transformations
- Google Sheets extraction (via API) when operational data is needed

All KPIs write in a standardized format to: vl_analytics.scorecard_<domain>

__
## KPI Design Pattern

All KPIs follow the same template:
1. Import base template and helpers
2. Load reference dates (last Sunday, year-week)
3. Execute the custom query (SQL or DuckDB)
4. Insert metadata + value into scorecard table


This pattern makes the system scalable and easy to maintain:
- new KPIs can be added in minutes,
- no repeated database code,
- consistent metadata (sc_name, kpi_number, field_name, range_type, etc.),
- easy debugging and auditing.

___

## Example KPIs in this repository

### 1. Replacement Processes – Existing Clients (KPI 16)
- Reads operational inputs from Google Sheets  
- Cleans and normalizes data using DuckDB  
- Calculates replacements started this week  
- Inserts standardized output to DWH

### 2. Overall Churn Rate – 52 Weeks (KPI 32)
- Uses HubSpot staging data in DWH  
- Filters invalid agreements, internal clients, test accounts  
- Computes churn ratio with YoY exposure  
- Outputs weekly churn performance

### 3. 4-Week Average Offboarding Forms (KPI 5)
- Derived KPI  
- Uses historical values from scorecard  
- Calculates trailing 4-week average  
- Inserts aggregated value

These examples reflect real operational scenarios such as workload monitoring, customer success performance, churn, and process execution trends.

___

## Publishing Scorecards
Each scorecard domain has a `run_to_sheet_<domain>.py` script that:

- extracts the latest KPI values from DWH  
- maps week → to the correct column  
- writes data and formatting into Google Sheets  
- maintains one tab per year (e.g., `sc2025`)  
- allows teams to consume fresh weekly metrics without accessing DWH  

___

## Folder Structure
ev-kpi-factory/
│
├─ core/
│ ├─ kpi_template.py
│ ├─ common_db.py
│ ├─ common_dates.py
│ └─ common_logging.py
│
├─ success_scorecard/
│ ├─ run_success_scorecard.py
│ ├─ run_to_sheet_success.py
│ ├─ 5_4w_ave_offboarding_forms.py
│ ├─ 16_replacement_processes_existing_clients.py
│ └─ 32_overall_churn_rate.py
│
├─ recruitment_scorecard/
│ └─ (placeholder)
│
├─ applications_scorecard/
│ └─ (placeholder)
│
├─ leadership_scorecard/
│ └─ (placeholder)
│
└─ docs/
├─ architecture.md
├─ data_flow_diagram.png
└─ example_kpi_design.md

___

## Security Considerations

- No credentials are stored in code.  
- Connections use environment variables.  
- All sample KPIs are anonymized and simplified.  
- Sensitive business logic is abstracted inside the template.

---

## Goal of This Repository

To demonstrate a scalable production-ready approach to KPIs:
- reusable,
- automated,
- version controlled,
- auditable,
- integrable with any BI stack.

This repository reflects real-world operational analytics and data process design.


---

## Crosslink con el módulo de ingestión

The data used by these KPIs comes from multiple sources and automations.

**ev-airtable-webhooks**
Automations in Airtable + Webhooks in Python
https://github.com/eVhjnb/ev-airtable-webhooks




