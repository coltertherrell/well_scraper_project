# =========================
# well_scraper/api_main.py
# =========================
from fastapi import FastAPI, Depends, HTTPException
from typing import Optional
from shapely.geometry import Point, Polygon
from well_scraper.database import WellDatabase
from well_scraper.models import WellRecord
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("API")

# FastAPI app
app = FastAPI(
    title="NM Oil & Gas Well API",
    description="Retrieve New Mexico well data by API number",
    version="1.0.0",
)

# Dependency: get a database instance
def get_db() -> WellDatabase:
    db_path = "data/sqlite.db"
    db = WellDatabase(db_path)
    return db

@app.get("/well/{api_number}", response_model=WellRecord)
def get_well(api_number: str, db: WellDatabase = Depends(get_db)):
    """
    Retrieve well data by API number.

    Args:
        api_number (str): The API number of the well
        db (WellDatabase): Injected database instance

    Returns:
        WellRecord: The well data
    """
    logger.info(f"Fetching well data for API: {api_number}")
    cursor = db.conn.cursor()
    cursor.execute("SELECT * FROM api_well_data WHERE API = ?", (api_number,))
    row = cursor.fetchone()

    if not row:
        logger.warning(f"Well not found: {api_number}")
        raise HTTPException(status_code=404, detail=f"Well {api_number} not found")

    # Map the row to WellRecord
    columns = [description[0] for description in cursor.description]
    record_dict = dict(zip(columns, row))
    logger.info(f"Well data retrieved for API: {api_number}\n Well data: {record_dict}\n")
    return WellRecord(**record_dict)

@app.get("/polygon")
def get_apis_in_polygon(coords: str, db: WellDatabase = Depends(get_db)):
    """
    GET endpoint for retrieving APIs inside a polygon.
    
    Args:
        coords (str): Comma-separated list of lat/lon pairs.
    """
    logger.info(f"Fetching APIs within polygon defined by coords: {coords}")

    try:
        flat = [float(c) for c in coords.split(",")]
    except ValueError:
        raise HTTPException(status_code=400, detail="Coordinates must be numeric values")

    if len(flat) % 2 != 0:
        raise HTTPException(status_code=400, detail="Must provide an even number of values for lat/lon pairs")

    points = [(flat[i], flat[i + 1]) for i in range(0, len(flat), 2)]

    if len(points) < 3:
        raise HTTPException(status_code=400, detail="Polygon must have at least 3 coordinate pairs")

    try:
        polygon = Polygon(points)
        if not polygon.is_valid:
            raise HTTPException(status_code=400, detail="Invalid polygon geometry")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="Unable to construct polygon from provided coordinates")


    cursor = db.conn.cursor()
    cursor.execute("SELECT API, Latitude, Longitude FROM api_well_data")
    result = []
    for api, lat, lon in cursor.fetchall():
        if lat is None or lon is None:
            continue
        if polygon.contains(Point(lat, lon)):
            result.append(api)

    logger.info(f"Found {len(result)} APIs within polygon\n apis: {result}")
    return {"apis": result}

@app.get("/health")
def health_check():
    """
    Health check endpoint.
    """
    return {"status": "ok"}