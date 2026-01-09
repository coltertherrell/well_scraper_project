from unittest.mock import patch, MagicMock
import requests
from well_scraper.well_scraper import WellScraper
from well_scraper.constants import WellFields


def test_parse_lat_lon_crs_valid():
    text = "35.123,-106.456 NAD83"
    lat, lon, crs = WellScraper.parse_lat_lon_crs(text)
    assert lat == 35.123
    assert lon == -106.456
    assert crs == "NAD83"


def test_parse_lat_lon_crs_invalid():
    lat, lon, crs = WellScraper.parse_lat_lon_crs("invalid")
    assert lat is None
    assert lon is None
    assert crs is None


@patch("well_scraper.well_scraper.requests.get")
def test_scrape_api_success(mock_get):
    scraper = WellScraper(max_retries=1, backoff_factor=0)

    coord_id = WellFields.FIELD_IDS["Coordinates"]

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = f"""
    <html>
      <body>
        <span id="{coord_id}">35.123,-106.456 NAD83</span>
        <span id="{WellFields.FIELD_IDS['Operator']}">Test Operator</span>
        <span id="{WellFields.FIELD_IDS['Status']}">Active</span>
      </body>
    </html>
    """

    mock_get.return_value = mock_response

    data = scraper.scrape_api("test_api")

    assert data["API"] == "test_api"
    assert data["Latitude"] == 35.123
    assert data["Longitude"] == -106.456
    assert data["CRS"] == "NAD83"
    assert data["Operator"] == "Test Operator"
    assert data["Status"] == "Active"


@patch("well_scraper.well_scraper.requests.get")
def test_scrape_api_failure(mock_get):
    scraper = WellScraper(max_retries=1, backoff_factor=0)
    mock_get.side_effect = requests.RequestException("Network error")

    data = scraper.scrape_api("test_api")
    assert data is None
