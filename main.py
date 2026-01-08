"""
Main entry point for the well scraper application.

This script parses command-line arguments and runs the ScraperApp to scrape well data
from a CSV file and store it in a SQLite database.
"""

import argparse
import logging
from well_scraper.app import ScraperApp

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
logger.addHandler(console_handler)

# File handler
file_handler = logging.FileHandler("scraper.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
logger.addHandler(file_handler)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to CSV with API numbers")
    parser.add_argument("--db", default="data/sqlite.db", help="Path to SQLite database")
    parser.add_argument("--multithread", action="store_true", help="Enable multithreading")
    parser.add_argument("--threads", type=int, default=5, help="Number of threads for multithreading")
    args = parser.parse_args()

    app = ScraperApp(
        csv_path=args.csv,
        db_path=args.db,
        multithread=args.multithread,
        threads=args.threads,
    )
    app.run()