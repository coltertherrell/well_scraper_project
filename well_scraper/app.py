# ======================
# well_scraper/app.py
# ======================
import csv
import logging
import concurrent.futures
import threading
import time
from .well_scraper import WellScraper
from .database import WellDatabase

class ScraperApp:
    def __init__(self, csv_path, db_path, multithread=False, threads=5, max_retries=2):
        """
        Initialize the ScraperApp with paths and options.

        Args:
            csv_path (str): Path to the CSV file containing API numbers.
            db_path (str): Path to the SQLite database file.
            multithread (bool): Whether to use multithreading for scraping. Defaults to False.
            threads (int): Number of threads to use if multithreaded. Defaults to 5.
            max_retries (int): Number of retry attempts for failed APIs. Defaults to 2.
        """
        self.csv_path = csv_path
        self.scraper = WellScraper()
        self.db = WellDatabase(db_path)
        self.multithread = multithread
        self.threads = threads
        self.max_retries = max_retries
        self.logger = logging.getLogger(self.__class__.__name__)
        self.inserted = 0
        self.errors = 0
        self.skipped = 0
        self.lock = threading.Lock()
        self.failed_apis = []
        self.failed_lock = threading.Lock()
        self.export_path = None
        self.export_format = None

    def _process_api(self, api, attempt=1):
        """
        Process a single API: scrape data and insert into database.

        Args:
            api (str): The API number to process.
            attempt (int): Current attempt number for retries.
        """
        try:
            data = self.scraper.scrape_api(api)
            if data:
                self.db.insert(data)
                with self.lock:
                    self.inserted += 1
                if attempt > 1:
                    self.logger.info(f"Retry succeeded for {api} on attempt {attempt}")
                else:
                    self.logger.info(f"Inserted {api}")
            else:
                if attempt <= self.max_retries:
                    self.logger.warning(f"No data for {api}, retrying attempt {attempt}")
                    self._process_api(api, attempt + 1)
                else:
                    with self.failed_lock:
                        self.failed_apis.append(api)
                    with self.lock:
                        self.errors += 1
                    self.logger.error(f"Failed to scrape {api} after {self.max_retries} attempts")
        except Exception as e:
            if attempt <= self.max_retries:
                self.logger.warning(f"Error processing {api} (attempt {attempt}): {e}, retrying")
                self._process_api(api, attempt + 1)
            else:
                with self.failed_lock:
                    self.failed_apis.append(api)
                with self.lock:
                    self.errors += 1
                self.logger.error(f"Error processing {api} after {self.max_retries} attempts: {e}")

    def run(self):
        """
        Run the scraping process: read APIs from CSV, process them, retry failed ones, 
        and print summary.
        """
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

        # First pass
        if self.multithread:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                for api in apis:
                    executor.submit(self._process_api, api)
        else:
            for api in apis:
                self._process_api(api)
                time.sleep(1)

        # Retry failed APIs sequentially at the end
        if self.failed_apis:
            self.logger.info(f"Retrying {len(self.failed_apis)} failed APIs sequentially")
            for api in list(self.failed_apis): 
                self.failed_apis.remove(api)
                self._process_api(api)

        print(f"Total APIs in CSV: {total_apis}")
        print(f"Skipped (missing API): {self.skipped}")
        print(f"Successfully inserted: {self.inserted}")
        print(f"Errors/Issues: {self.errors}")

        # Export if requested
        if self.export_path and self.export_format:
            self.db.export_data(self.export_path, self.export_format)
