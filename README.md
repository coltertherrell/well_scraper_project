# Well Scraper Project

A Python scraper for New Mexico Oil & Gas well data from the [N.M. Energy, Minerals and Natural Resources Department (EMNRD) Well Details](https://wwwapps.emnrd.nm.gov/OCD/OCDPermitting/Data/WellDetails.aspx) website.  

This project reads a CSV of API numbers, scrapes well information for each, stores it in a SQLite database, and optionally exports it to CSV or JSON. It supports multithreading, logging, and automatic retries when being rate limited.

---

## Features

- Scrape detailed well information including:
  - Operator, Status, Well Type, Work Type, Directional Status
  - Multi-Lateral, Mineral Owner, Surface Owner
  - Surface Location, Elevations (GL, KB, DF)
  - Completions, Potash Waiver, Spud Date, Last Inspection, TVD
  - Latitude, Longitude, CRS
- Store scraped data in a SQLite database (`sqlite.db` by default)
- Optional export to CSV or JSON
- Multithreaded scraping for faster processing
- Handles HTTP errors and retries with exponential backoff
- Logging to console with timestamps and log levels
- **API endpoint** for querying wells by API number (via FastAPI)

---

## Project Structure

```text
well_scraper_project/
├── well_scraper/
│   ├── __init__.py
│   ├── constants.py
│   ├── well_scraper.py
│   ├── database.py
│   ├── app.py
│   └── models/
│       ├── __init__.py
│       └── well_record.py
├── data/
│   ├── apis_pythondev_test.csv         # Input CSV of API numbers
│   ├── sqlite.db                       # SQLite database
│   └── wells_export.csv                # Optional csv export of sqlite.db to easily view data    
├── main.py                             # CLI scraping entrypoint
├── api_main.py                         # FastAPI entrypoint
├── requirements.txt
├── README.md
└── tests/
    ├── test_well_scraper.py
    ├── test_database.py
    └── test_app.py
```

---

## Installation

1. Clone the repository:

```bash
git clone https://github.com/coltertherrell/well_scraper_project.git
cd well_scraper_project
```

2. Create a virtual environment and activate it:

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Usage

### Basic scraping

```bash
python main.py --csv data/apis_pythondev_test.csv
```

This scrapes all API numbers in `data/apis_pythondev_test.csv` and stores them in `sqlite.db`.

---

### Multithreaded scraping

```bash
python main.py --csv data/apis_pythondev_test.csv --multithread --threads 10
```

- `--multithread` enables threading
- `--threads` sets the number of concurrent threads (default is 5)

---

### Exporting the database

You can optionally export the SQLite database to CSV or JSON after scraping:

```bash
python main.py --csv data/apis_pythondev_test.csv --export_path data/wells_export.csv --export_format csv
python main.py --csv data/apis_pythondev_test.csv --export_path data/wells_export.json --export_format json
```

- `--export_path`: File path to save exported data
- `--export_format`: Either `csv` or `json`

---

## Logging

Logs include timestamps, log levels, and messages about progress, skipped rows, and HTTP errors.

```text
2026-01-07 18:17:02,507 [INFO] ScraperApp: Inserted 30-015-25325
2026-01-07 18:17:03,123 [WARNING] ScraperApp: Skipping row 481: missing API
```

---

## CSV Input Format

Input CSV should have a header `api` or `API`:

```csv
api
30-015-25325
30-015-25327
30-015-25330
```

Empty or missing API rows are skipped with a warning.

---

## API Endpoints

The project provides a **FastAPI endpoint** to query well data by API number.

### Start the API server

```bash
uvicorn api_main:app --reload
```

- `--reload` automatically reloads when code changes
- Default URL: `http://127.0.0.1:8000`
- Docs URL: `http://127.0.0.1:8000/docs`

### GET /well/{api_number}

Retrieve all data for a well by its API number.

**Request:**

```http
GET /well/30-015-25325
```

**Response:**

```json
{
  "API": "30-015-25325",
  "Operator": "Some Operator",
  "Status": "Active",
  "Well_Type": "Oil",
  "Work_Type": "Exploratory",
  "Directional_Status": null,
  "Multi_Lateral": null,
  "Mineral_Owner": null,
  "Surface_Owner": null,
  "Surface_Location": null,
  "GL_Elevation": 5000.0,
  "KB_Elevation": 5020.0,
  "DF_Elevation": 4980.0,
  "Single_Multiple_Completion": null,
  "Potash_Waiver": null,
  "Spud_Date": "2025-05-01",
  "Last_Inspection": "2025-12-01",
  "TVD": 10000.0,
  "Latitude": 35.123,
  "Longitude": -106.456,
  "CRS": "NAD83"
}
```

- Returns `404` if the well is not found
- Uses **dependency-injected database** for safe and testable queries

---

## Testing

Run unit tests with pytest:

```bash
pytest tests/
```

Tests cover database operations, web scraping logic, app functionality, and API endpoints (mocked).

---

## Notes

- Handles content-based (as opposed to HTTP status code based) rate limiting with exponential backoff
- Multithreading improves speed for large CSVs
- Latitude, Longitude, CRS are parsed from the same field
- SQLite DB can be exported anytime using `export_data()` method in `database.py`
- API endpoint allows programmatic access to well data without rerunning the scraper