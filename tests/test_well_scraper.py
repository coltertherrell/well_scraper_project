from unittest.mock import patch, MagicMock
import pytest
import requests
from well_scraper.well_scraper import WellScraper
from well_scraper.constants import WellFields


def test_parse_lat_lon_crs_valid():
    text = "35.123, -106.456 NAD83"
    lat, lon, crs = WellScraper.parse_lat_lon_crs(text)
    assert lat == pytest.approx(35.123)
    assert lon == pytest.approx(-106.456)
    assert crs == "NAD83"

def test_parse_lat_lon_crs_invalid():
    text = "invalid"
    lat, lon, crs = WellScraper.parse_lat_lon_crs(text)
    assert lat is None
    assert lon is None
    assert crs is None

def test_parse_lat_lon_crs_none():
    lat, lon, crs = WellScraper.parse_lat_lon_crs(None)
    assert lat is None
    assert lon is None
    assert crs is None

@patch("well_scraper.well_scraper.requests.get")
def test_scrape_api_success(mock_get):
    scraper = WellScraper(max_retries=1, backoff_factor=0)

    mock_response = MagicMock()
    mock_response.status_code = 200

    coordinates_id = WellFields.FIELD_IDS["Coordinates"]
    operator_id = WellFields.FIELD_IDS["Operator"]
    status_id = WellFields.FIELD_IDS["Status"]

    mock_response.text = f"""
    <html>
        <body>
            <span id="{operator_id}">Test Operator</span>
            <span id="{status_id}">Active</span>
            <span id="{coordinates_id}">35.123, -106.456 NAD83</span>
        </body>
    </html>
    """
    mock_get.return_value = mock_response

    data = scraper.scrape_api("test_api")

    assert data is not None
    assert data["API"] == "test_api"
    assert data["Operator"] == "Test Operator"
    assert data["Status"] == "Active"
    # Coordinates parsed correctly
    assert data.get("Latitude") == pytest.approx(35.123)
    assert data.get("Longitude") == pytest.approx(-106.456)
    assert data.get("CRS") == "NAD83"


@patch("well_scraper.well_scraper.requests.get")
def test_scrape_api_failure(mock_get):
    scraper = WellScraper(max_retries=1, backoff_factor=0)
    mock_get.side_effect = requests.RequestException("Network error")

    data = scraper.scrape_api("test_api")
    assert data is None


@patch("well_scraper.well_scraper.requests.get")
def test_scrape_api_rate_limit(mock_get):
    scraper = WellScraper(max_retries=2, backoff_factor=0)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "Site Busy - Rate Limit Reached"
    mock_get.return_value = mock_response

    data = scraper.scrape_api("test_api")

    # Since the page eventually returns, data dict is returned with all None fields
    assert isinstance(data, dict)
    assert data["API"] == "test_api"
    # Lat/Lon/CRS should all be None
    assert data["Latitude"] is None
    assert data["Longitude"] is None
    assert data["CRS"] is None

