import tempfile
import os
from unittest.mock import patch, MagicMock
import pytest
from well_scraper.app import ScraperApp
from well_scraper.models import WellRecord


@pytest.fixture
def temp_files():
    csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(csv_fd, "w") as f:
        f.write("api\n30-015-25325\n30-015-25327\n")

    db_path = tempfile.mktemp()
    yield csv_path, db_path

    os.unlink(csv_path)


@patch("well_scraper.app.WellDatabase")
@patch("well_scraper.app.WellScraper")
def test_run_single_thread(mock_scraper_class, mock_db_class, temp_files):
    mock_scraper = MagicMock()
    mock_scraper.scrape_api.return_value = {
        "API": "30-015-25325",
        "Operator": "Test Operator",
    }
    mock_scraper_class.return_value = mock_scraper

    mock_db = MagicMock()
    mock_db_class.return_value = mock_db

    csv_path, db_path = temp_files
    app = ScraperApp(csv_path, db_path, multithread=False, threads=1)

    app.run()

    assert mock_scraper.scrape_api.call_count == 2
    assert mock_db.insert.call_count == 2

    inserted_record = mock_db.insert.call_args[0][0]
    assert isinstance(inserted_record, WellRecord)
    assert inserted_record.API == "30-015-25325"

    assert app.inserted == 2
    assert app.errors == 0
    assert app.skipped == 0


@patch("well_scraper.app.WellDatabase")
@patch("well_scraper.app.WellScraper")
def test_run_with_errors(mock_scraper_class, mock_db_class, temp_files):
    mock_scraper = MagicMock()
    mock_scraper.scrape_api.side_effect = [
        {"API": "30-015-25325", "Operator": "Test"},
        None,
    ]
    mock_scraper_class.return_value = mock_scraper

    mock_db = MagicMock()
    mock_db_class.return_value = mock_db

    csv_path, db_path = temp_files
    app = ScraperApp(csv_path, db_path, multithread=False, threads=1)

    app.run()

    assert app.inserted == 1
    assert app.errors == 1
    mock_db.insert.assert_called_once()


def test_csv_with_missing_api(temp_files):
    csv_path, db_path = temp_files
    with open(csv_path, "w") as f:
        f.write("api\n \n")

    with patch("well_scraper.app.WellScraper") as mock_scraper_class, \
         patch("well_scraper.app.WellDatabase") as mock_db_class:

        mock_scraper_class.return_value = MagicMock()
        mock_db_class.return_value = MagicMock()

        app = ScraperApp(csv_path, db_path)
        app.run()

        assert app.skipped == 1
        assert app.inserted == 0
