# Data Quality Check Project

## Overview
This project performs automated data quality checks on transactional datasets:
- `transactions.csv`
- `products.csv`
- `customers.csv`

It identifies issues like:
- Missing values
- Duplicate records
- Invalid emails
- Inconsistent product IDs
- Negative or zero transaction amounts

## Repository Structure
- `manual_checks.ipynb` – Initial manual data exploration and validation.
- `scripts/automated_quality_report.py` – Python script for automated quality checks.
- `dags/daily_quality_check_dag.py` – Airflow DAG for scheduled daily runs.
- `reports/` – Folder where generated reports are saved.
- `data/` – Source CSV files.
- `.env` – Environment variables (paths).

## Quick Start

### 1. Manual Data Checking  
Open `manual_checks.ipynb` in JupyterLab to review the step-by-step analysis.

### 2. Run Python Script
- Install dependencies:  
```bash
pip install pandas python-dotenv
```
- Run the automated report generation:  
```bash
python scripts/automated_quality_report.py
```
The report will be saved in the reports/ folder with today’s date.

### 3. Run with Airflow (Optional)  
- Initialize the Airflow database:  
```bash
docker-compose run airflow-init
```
- Start Docker services:
```
docker-compose up -d
```
- Open Airflow UI: http://localhost:8080  
Username: admin
Password: admin
  
- Trigger the daily_data_quality_check DAG manually or let it run automatically on schedule.
  
- Changing the Schedule  
To modify the schedule (e.g., hourly, every 5 minutes), adjust the schedule_interval inside dags/daily_quality_check_dag.py.

