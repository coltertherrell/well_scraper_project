import tempfile
import os
import csv
import json
import pytest
from well_scraper.database import WellDatabase
from well_scraper.models.well_record import WellRecord


@pytest.fixture
def temp_db():
    db_fd, db_path = tempfile.mkstemp()
    db = WellDatabase(db_path)
    yield db
    db.conn.close()
    os.close(db_fd)
    os.unlink(db_path)


def test_table_creation(temp_db):
    cursor = temp_db.conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='api_well_data'"
    )
    assert cursor.fetchone() is not None


def test_insert(temp_db):
    record = WellRecord(
        API="test_api",
        Operator="Test Operator",
        Status="Active",
    )
    temp_db.insert(record)

    cursor = temp_db.conn.cursor()
    cursor.execute("SELECT API, Operator FROM api_well_data WHERE API = ?", ("test_api",))
    row = cursor.fetchone()

    assert row is not None
    assert row[0] == "test_api"
    assert row[1] == "Test Operator"


def test_export_csv(temp_db):
    temp_db.insert(WellRecord(API="test_api", Operator="Test Operator"))

    export_fd, export_path = tempfile.mkstemp(suffix=".csv")
    os.close(export_fd)

    temp_db.export_data(export_path, format="csv")

    with open(export_path, newline="") as f:
        rows = list(csv.reader(f))

    assert len(rows) == 2
    assert rows[0][0] == "API"
    assert rows[1][0] == "test_api"

    os.unlink(export_path)


def test_export_json(temp_db):
    temp_db.insert(WellRecord(API="test_api", Operator="Test Operator"))

    export_fd, export_path = tempfile.mkstemp(suffix=".json")
    os.close(export_fd)

    temp_db.export_data(export_path, format="json")

    with open(export_path) as f:
        data = json.load(f)

    assert len(data) == 1
    assert data[0]["API"] == "test_api"

    os.unlink(export_path)


def test_export_invalid_format(temp_db):
    with pytest.raises(ValueError):
        temp_db.export_data("dummy", format="xml")
