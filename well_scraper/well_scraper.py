# =============================
# well_scraper/well_scraper.py
# =============================
import time
import logging
import requests
from bs4 import BeautifulSoup
from .constants import WellFields


class WellScraper:
    BASE_URL = (
        "https://wwwapps.emnrd.nm.gov/OCD/OCDPermitting/Data/WellDetails.aspx?api={}"
    )

    RATE_LIMIT_TEXT = "Site Busy - Rate Limit Reached"

    def __init__(self, max_retries=5, backoff_factor=1):
        """
        Initialize the WellScraper with retry settings.

        Args:
            max_retries (int): Maximum number of retries for failed requests.
            backoff_factor (int): Base factor for exponential backoff.
        """
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.logger = logging.getLogger(self.__class__.__name__)

    def _get_field_text(self, soup, span_id):
        """
        Extract text from a span element by ID, removing nested tags.
        """
        if not span_id:
            return None

        span = soup.find("span", id=span_id)
        if not span:
            return None

        for tag in span.find_all():
            tag.extract()

        return span.get_text(strip=True) or None

    @staticmethod
    def parse_lat_lon_crs(text):
        """
        Parse latitude, longitude, and CRS from coordinate text.
        """
        if not text:
            return None, None, None
        try:
            coords, crs = text.rsplit(" ", 1)
            lat, lon = coords.split(",")
            return float(lat), float(lon), crs
        except ValueError:
            return None, None, None

    def scrape_api(self, api_number):
        """
        Scrape well data for a given API number.
        """
        url = self.BASE_URL.format(api_number)

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.get(url, timeout=30)

                if self.RATE_LIMIT_TEXT in resp.text:
                    wait = self.backoff_factor * (2 ** (attempt - 1))
                    self.logger.warning(f"Rate limit page detected for {api_number}, retry {attempt}/{self.max_retries}, sleeping {wait}s")
                    time.sleep(wait)
                    continue

                resp.raise_for_status()

                if attempt > 1:
                    self.logger.info(f"Retry succeeded for {api_number} on attempt {attempt}")

                break

            except requests.RequestException as e:
                if attempt >= self.max_retries:
                    self.logger.error(f"Failed {api_number} after {attempt} attempts: {e}")
                    return None

                wait = self.backoff_factor * (2 ** (attempt - 1))
                self.logger.warning(f"HTTP error for {api_number} (attempt {attempt}), sleeping {wait}s: {e}")
                time.sleep(wait)

        soup = BeautifulSoup(resp.text, "html.parser")
        data = {"API": api_number}

        # Parse coordinates ONCE
        coord_span_id = WellFields.FIELD_IDS.get("Coordinates")
        coord_text = self._get_field_text(soup, coord_span_id)
        lat, lon, crs = self.parse_lat_lon_crs(coord_text)

        data["Latitude"] = lat
        data["Longitude"] = lon
        data["CRS"] = crs

        for field, span_id in WellFields.FIELD_IDS.items():
            if field in {"API", "Coordinates"}:
                continue

            data[field] = self._get_field_text(soup, span_id)

        return data