# New Mexico OCD Well Scraper

This project scrapes well data from the New Mexico Oil Conservation Division (OCD) public website and stores the results in a local SQLite database. It is designed to be reliable, modular, and production-ready, with support for retry/backoff, logging, streaming input, and optional multithreading.

---

## Features

- Scrapes well details by API number from NM OCD
- Streams API numbers from a CSV file (memory-efficient)
- Handles:
  - HTTP 429 rate limiting with exponential backoff
  - Network timeouts and request failures
  - Missing or malformed HTML fields
- Parses and stores:
  - Operator, status, well/work type
  - Elevations and true vertical depth (TVD)
  - Spud date and last inspection date
  - Latitude, longitude, and CRS
- Stores results in SQLite
- Optional multithreaded scraping
- Structured, object-oriented design

---

## Project Structure

well_scraper_project/
├── well_scraper/
│ ├── init.py
│ ├── constants.py # Centralized field IDs (WellFields)
│ ├── well_scraper.py # Scraping logic + retry/backoff
│ ├── database.py # SQLite database layer
│ └── app.py # Orchestration (ScraperApp)
├── data/
│ ├── apis.csv # Input API numbers
│ └── sqlite.db # Output SQLite database

## Requirements

- Python 3.9+
- Internet access

## Install

```powershell
python -m pip install -r requirements.txt
```

## Run

Run the scraper with a CSV of API numbers (example CSV included in `data/`):

```powershell
python main.py --csv data/apis_pythondev_test.csv
```

Options:
- `--db`: path to SQLite DB (default: `data/sqlite.db`)
- `--multithread`: enable multithreaded scraping
- `--threads N`: number of worker threads (default: 5)