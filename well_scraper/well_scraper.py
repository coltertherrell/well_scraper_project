import time
import logging
import requests
from bs4 import BeautifulSoup
from .constants import WellFields

class WellScraper:
    BASE_URL = (
        "https://wwwapps.emnrd.nm.gov/OCD/OCDPermitting/Data/WellDetails.aspx?api={}"
    )

    def __init__(self, max_retries=5, backoff_factor=1):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.logger = logging.getLogger(self.__class__.__name__)

    def _get_field_text(self, soup, span_id):
        span = soup.find("span", id=span_id)
        if not span:
            return None

        # remove nested tags (e.g., <a>)
        for tag in span.find_all():
            tag.extract()

        return span.get_text(strip=True) or None

    @staticmethod
    def parse_lat_lon_crs(text):
        if not text:
            return None, None, None
        try:
            coords, crs = text.rsplit(" ", 1)
            lat, lon = coords.split(",")
            return float(lat), float(lon), crs
        except ValueError:
            return None, None, None

    def scrape_api(self, api_number):
        url = self.BASE_URL.format(api_number)

        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.get(url, timeout=30)

                if resp.status_code == 429:
                    wait = self.backoff_factor * (2 ** attempt)
                    self.logger.warning(f"429 for {api_number}, backing off {wait}s")
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                break

            except requests.RequestException as e:
                if attempt >= self.max_retries:
                    self.logger.error(f"Failed {api_number}: {e}")
                    return None
                wait = self.backoff_factor * (2 ** attempt)
                self.logger.warning(f"Retry {attempt+1} for {api_number}, sleeping {wait}s")
                time.sleep(wait)

        soup = BeautifulSoup(resp.text, "html.parser")
        data = {"API": api_number}

        for field, span_id in WellFields.FIELD_IDS.items():
            raw_value = self._get_field_text(soup, span_id)

            if field == "Coordinates":
                lat, lon, crs = self.parse_lat_lon_crs(raw_value)
                data["Latitude"] = lat
                data["Longitude"] = lon
                data["CRS"] = crs
            else:
                data[field] = raw_value

        return data
