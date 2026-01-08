import argparse
import logging
from well_scraper.app import ScraperApp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--db", default="data/sqlite.db")
    parser.add_argument("--multithread", action="store_true")
    parser.add_argument("--threads", type=int, default=5)
    args = parser.parse_args()

    app = ScraperApp(
        csv_path=args.csv,
        db_path=args.db,
        multithread=args.multithread,
        threads=args.threads,
    )
    app.run()
