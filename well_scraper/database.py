# ==========================
# well_scraper/database.py
# ==========================
import sqlite3
import csv
import json
import logging
from dataclasses import asdict
from .models import WellRecord


class WellDatabase:
    """
    SQLite database wrapper for storing and exporting well data.
    """

    TABLE_NAME = "api_well_data"

    # Single source of truth for DB column order
    COLUMNS = [
        "API",
        "Operator",
        "Status",
        "Well_Type",
        "Work_Type",
        "Directional_Status",
        "Multi_Lateral",
        "Mineral_Owner",
        "Surface_Owner",
        "Surface_Location",
        "GL_Elevation",
        "KB_Elevation",
        "DF_Elevation",
        "Single_Multiple_Completion",
        "Potash_Waiver",
        "Spud_Date",
        "Last_Inspection",
        "TVD",
        "Latitude",
        "Longitude",
        "CRS",
    ]

    def __init__(self, db_path: str):
        """
        Initialize the WellDatabase with a SQLite database path.
        """
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._create_table()

    def _create_table(self):
        """
        Create the api_well_data table if it does not exist.
        """
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                API TEXT PRIMARY KEY,
                Operator TEXT,
                Status TEXT,
                Well_Type TEXT,
                Work_Type TEXT,
                Directional_Status TEXT,
                Multi_Lateral TEXT,
                Mineral_Owner TEXT,
                Surface_Owner TEXT,
                Surface_Location TEXT,
                GL_Elevation REAL,
                KB_Elevation REAL,
                DF_Elevation REAL,
                Single_Multiple_Completion TEXT,
                Potash_Waiver TEXT,
                Spud_Date TEXT,
                Last_Inspection TEXT,
                TVD REAL,
                Latitude REAL,
                Longitude REAL,
                CRS TEXT
            )
            """
        )
        self.conn.commit()
        self.logger.info("Database table 'api_well_data' ensured.")

    def insert(self, record: WellRecord):
        """
        Insert or replace a WellRecord into the database safely.
        """
        record_dict = asdict(record)

        # Align data strictly to DB columns
        values = [record_dict.get(col) for col in self.COLUMNS]
        placeholders = ",".join("?" for _ in self.COLUMNS)

        sql = f"""
            INSERT OR REPLACE INTO {self.TABLE_NAME}
            ({",".join(self.COLUMNS)})
            VALUES ({placeholders})
        """

        self.conn.execute(sql, values)
        self.conn.commit()

        self.logger.debug(f"Inserted/Updated record for API {record.API}")

    def export_data(self, output_path, format="csv"):
        """
        Export all well data to CSV or JSON format.
        """
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {self.TABLE_NAME}")
        rows = cursor.fetchall()

        if format.lower() == "csv":
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(self.COLUMNS)
                writer.writerows(rows)
            self.logger.info(f"Exported {len(rows)} rows to CSV: {output_path}")

        elif format.lower() == "json":
            self.conn.row_factory = sqlite3.Row
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT * FROM {self.TABLE_NAME}")
            rows = cursor.fetchall()
            data = [dict(row) for row in rows]
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)

            self.logger.info(f"Exported {len(data)} rows to JSON: {output_path}")

        else:
            raise ValueError("format must be 'csv' or 'json'")