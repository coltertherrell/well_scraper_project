import csv
import logging
import concurrent.futures
import threading
from .well_scraper import WellScraper
from .database import WellDatabase

class ScraperApp:
    def __init__(self, csv_path, db_path, multithread=False, threads=5):
        self.csv_path = csv_path
        self.scraper = WellScraper()
        self.db = WellDatabase(db_path)
        self.multithread = multithread
        self.threads = threads
        self.logger = logging.getLogger(self.__class__.__name__)
        self.inserted = 0
        self.errors = 0
        self.skipped = 0
        self.lock = threading.Lock()

    def _process_api(self, api):
        try:
            data = self.scraper.scrape_api(api)
            if data:
                self.db.insert(data)
                with self.lock:
                    self.inserted += 1
                self.logger.info(f"Inserted {api}")
            else:
                with self.lock:
                    self.errors += 1
                self.logger.warning(f"No data scraped for {api}")
        except Exception as e:
            with self.lock:
                self.errors += 1
            self.logger.error(f"Error processing {api}: {e}")

    def run(self):
        apis = []
        with open(self.csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=2):
                api = row.get("api") or row.get("API")
                if api and api.strip():
                    apis.append(api.strip())
                else:
                    self.skipped += 1
                    self.logger.warning(f"Skipping row {row_num}: missing API")

        total_apis = len(apis) + self.skipped

        if self.multithread:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.threads
            ) as executor:
                for api in apis:
                    executor.submit(self._process_api, api)
        else:
            for api in apis:
                self._process_api(api)

        print(f"Total APIs in CSV: {total_apis}")
        print(f"Skipped (missing API): {self.skipped}")
        print(f"Successfully inserted: {self.inserted}")
        print(f"Errors/Issues: {self.errors}")
