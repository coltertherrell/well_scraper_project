# =========
# main.py
# =========
import argparse
import logging
from well_scraper.app import ScraperApp

# Set up logging to console and file
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("Main")

def main():
    parser = argparse.ArgumentParser(description="Scrape well data from NM OCD website.")
    parser.add_argument("--csv", required=True, help="Path to CSV of API numbers")
    parser.add_argument("--db", default="data/sqlite.db", help="SQLite database path")
    parser.add_argument("--multithread", action="store_true", help="Enable multithreaded scraping")
    parser.add_argument("--threads", type=int, default=5, help="Number of threads if multithreading")
    parser.add_argument("--export_path", help="Optional path to export scraped data")
    parser.add_argument("--export_format", choices=["csv", "json"], help="Export format: csv or json")
    
    args = parser.parse_args()

    # Create the ScraperApp instance
    app = ScraperApp(
        csv_path=args.csv,
        db_path=args.db,
        multithread=args.multithread,
        threads=args.threads,
    )

    # Run scraping
    logger.info("Starting scraping process...")
    app.run()
    logger.info("Scraping complete!")

    # Export if requested
    if args.export_path and args.export_format:
        logger.info(f"Exporting data to {args.export_path} as {args.export_format.upper()}")
        app.db.export_data(args.export_path, args.export_format)
        logger.info("Export complete!")

if __name__ == "__main__":
    main()