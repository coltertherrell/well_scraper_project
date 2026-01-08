import tempfile
import os
import csv
import json
import pytest
from well_scraper.database import WellDatabase


@pytest.fixture
def temp_db():

    db_fd, db_path = tempfile.mkstemp()
    os.close(db_fd)  # Close FD immediately (important on Windows)

    db = WellDatabase(db_path)
    yield db

    # Cleanup
    db.conn.close()
    os.unlink(db_path)


def test_table_creation(temp_db):

    cursor = temp_db.conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='api_well_data'")
    assert cursor.fetchone() is not None


def test_insert(temp_db):

    record = {"API": "test_api", "Operator": "Test Operator", "Status": "Active"}

    temp_db.insert(record)

    cursor = temp_db.conn.cursor()
    cursor.execute("SELECT API, Operator, Status FROM api_well_data WHERE API = ?", ("test_api",))
    row = cursor.fetchone()

    assert row is not None
    assert row[0] == "test_api"
    assert row[1] == "Test Operator"
    assert row[2] == "Active"


def test_export_csv(temp_db):

    record = {"API": "test_api", "Operator": "Test Operator", "Status": "Active"}
    temp_db.insert(record)

    export_fd, export_path = tempfile.mkstemp(suffix=".csv")
    os.close(export_fd)

    temp_db.export_data(export_path, format="csv")

    with open(export_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    # Header + 1 data row
    assert len(rows) == 2
    assert "API" in rows[0]
    assert rows[1][rows[0].index("API")] == "test_api"

    os.unlink(export_path)


def test_export_json(temp_db):

    record = {"API": "test_api", "Operator": "Test Operator", "Status": "Active"}
    temp_db.insert(record)

    export_fd, export_path = tempfile.mkstemp(suffix=".json")
    os.close(export_fd)

    temp_db.export_data(export_path, format="json")

    with open(export_path, encoding="utf-8") as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["API"] == "test_api"
    assert data[0]["Operator"] == "Test Operator"

    os.unlink(export_path)


def test_export_invalid_format(temp_db):

    with pytest.raises(ValueError):
        temp_db.export_data("dummy_path", format="xml")
