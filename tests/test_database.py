import tempfile
import os
import csv
import json
import pytest

from well_scraper.database import WellDatabase
from well_scraper.models import WellRecord


@pytest.fixture
def temp_db():

    db_fd, db_path = tempfile.mkstemp()
    os.close(db_fd)
    db = WellDatabase(db_path)
    yield db
    db.conn.close()
    os.unlink(db_path)


def test_table_creation(temp_db):

    cursor = temp_db.conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='api_well_data'"
    )
    assert cursor.fetchone() is not None


def test_insert_well_record(temp_db):

    record = WellRecord(
        API="30-015-25325",
        Operator="Test Operator",
        Status="Active",
    )

    temp_db.insert(record)

    cursor = temp_db.conn.cursor()
    cursor.execute(
        "SELECT * FROM api_well_data WHERE API = ?",
        ("30-015-25325",),
    )
    row = cursor.fetchone()

    assert row is not None

    # Convert row to dict using column list
    row_dict = dict(zip(temp_db.COLUMNS, row))

    assert row_dict["API"] == "30-015-25325"
    assert row_dict["Operator"] == "Test Operator"
    assert row_dict["Status"] == "Active"


def test_insert_missing_optional_fields(temp_db):

    record = WellRecord(API="test_api_only")

    temp_db.insert(record)

    cursor = temp_db.conn.cursor()
    cursor.execute(
        "SELECT * FROM api_well_data WHERE API = ?",
        ("test_api_only",),
    )
    row = cursor.fetchone()
    row_dict = dict(zip(temp_db.COLUMNS, row))

    assert row_dict["API"] == "test_api_only"
    assert row_dict["Operator"] is None
    assert row_dict["Latitude"] is None


def test_export_csv(temp_db):

    record = WellRecord(
        API="csv_test",
        Operator="CSV Operator",
        Status="Active",
    )
    temp_db.insert(record)

    fd, export_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)

    temp_db.export_data(export_path, format="csv")

    with open(export_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    # Header validation
    assert rows[0] == temp_db.COLUMNS

    # Data validation
    data_row = rows[1]
    row_dict = dict(zip(temp_db.COLUMNS, data_row))

    assert row_dict["API"] == "csv_test"
    assert row_dict["Operator"] == "CSV Operator"

    os.unlink(export_path)


def test_export_json(temp_db):

    record = WellRecord(
        API="json_test",
        Operator="JSON Operator",
        Status="Inactive",
    )
    temp_db.insert(record)

    fd, export_path = tempfile.mkstemp(suffix=".json")
    os.close(fd)

    temp_db.export_data(export_path, format="json")

    with open(export_path, encoding="utf-8") as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 1

    row = data[0]
    assert row["API"] == "json_test"
    assert row["Operator"] == "JSON Operator"
    assert row["Status"] == "Inactive"

    os.unlink(export_path)


def test_export_invalid_format_raises(temp_db):

    with pytest.raises(ValueError):
        temp_db.export_data("dummy.out", format="xml")
