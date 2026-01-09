import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock
from api_main import app, get_db
from well_scraper.models import WellRecord

# Fixture for FastAPI test client
@pytest.fixture
def client():
    return TestClient(app)

@pytest.fixture
def client(mock_db):
    # Override the get_db dependency
    def get_mock_db():
        return mock_db

    app.dependency_overrides[get_db] = get_mock_db  # <-- override
    yield TestClient(app)
    app.dependency_overrides.clear()  # clean up after test


@pytest.fixture
def mock_db():
    return MagicMock()

def test_get_well_success(client, mock_db):
    # Setup: return a single row from the "database"
    row = (
        "30-015-25325",  # API
        "Test Operator",  # Operator
        "Active",  # Status
        None, None, None, None, None, None, None,  # Well_Type, Work_Type, Directional_Status, etc.
        None, None, None, None, None, None, None, None,  # Elevations, Spud_Date, Last_Inspection, TVD
        35.123,  # Latitude
        -106.456,  # Longitude
        "NAD83",  # CRS
    )

    columns = [f.name for f in WellRecord.__dataclass_fields__.values()]
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = row
    mock_cursor.description = [(col,) for col in columns]
    mock_db.conn.cursor.return_value = mock_cursor

    response = client.get("/well/30-015-25325")
    assert response.status_code == 200

    data = response.json()
    assert data["API"] == "30-015-25325"
    assert data["Operator"] == "Test Operator"
    assert data["Latitude"] == 35.123
    assert data["Longitude"] == -106.456
    assert data["CRS"] == "NAD83"

def test_get_well_not_found(client, mock_db):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_db.conn.cursor.return_value = mock_cursor

    response = client.get("/well/invalid-api")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]
