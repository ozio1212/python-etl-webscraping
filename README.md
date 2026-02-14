# ETL Bank Project

Extract, transform, and load the largest banks market cap data from a web source, convert USD market caps into GBP/EUR/INR, and store results in both CSV and SQLite. The script also logs each ETL step.

## What This Project Does

- Extracts bank name and market cap (USD billions) from a web page.
- Transforms market cap into GBP, EUR, and INR using exchange rates in a CSV.
- Saves results to a CSV file and a SQLite database table.
- Logs progress to a timestamped log file.

## Files

- banks_project.py: main ETL script.
- exchange_rate.csv: currency conversion rates used for transform.
- Largest_banks_data.csv: output CSV created by the script.
- Banks.db: output SQLite database created by the script.
- code_log.txt: ETL run log.

## Requirements

- Python 3.8+
- Packages:
  - beautifulsoup4
  - requests
  - pandas
  - numpy

## Setup

Create and activate a virtual environment (optional), then install dependencies:

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install beautifulsoup4 requests pandas numpy
```

## Run

From the project folder:

```bash
python banks_project.py
```

## Outputs

- Largest_banks_data.csv: contains the transformed market cap columns.
- Banks.db: SQLite database containing the Largest_banks table.
- code_log.txt: timestamps for each ETL step.

## Notes

- The script uses a web archive URL to keep the data source stable.
- If you change the exchange rates, update exchange_rate.csv and re-run the script.
