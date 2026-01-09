# ==========================
# well_scraper/database.py
# ==========================
import sqlite3
import csv
import json
import logging
from .models import WellRecord

class WellDatabase:
    def __init__(self, db_path: str):
        """
        Initialize the WellDatabase with a SQLite database path.
        """
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._create_table()

    def _create_table(self):
        """
        Create the 'api_well_data' table if it does not exist.
        """
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS api_well_data (
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
        Insert or replace a WellRecord into the database.
        """
        columns = [field.name for field in record.__dataclass_fields__.values()]
        placeholders = ",".join("?" * len(columns))
        values = [getattr(record, col) for col in columns]

        sql = f"INSERT OR REPLACE INTO api_well_data ({','.join(columns)}) VALUES ({placeholders})"
        self.conn.execute(sql, values)
        self.conn.commit()
        self.logger.debug(f"Inserted/Updated record for API {record.API}")

    def export_data(self, output_path, format="csv"):
        """
        Export all well data to CSV or JSON format.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM api_well_data")
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]

        if format.lower() == "csv":
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
            self.logger.info(f"Exported {len(rows)} rows to CSV: {output_path}")

        elif format.lower() == "json":
            self.conn.row_factory = sqlite3.Row
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM api_well_data")
            rows = cursor.fetchall()
            data = [dict(row) for row in rows]
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            self.logger.info(f"Exported {len(data)} rows to JSON: {output_path}")

        else:
            self.logger.error(f"Invalid format '{format}' for export_data()")
            raise ValueError("format must be 'csv' or 'json'")