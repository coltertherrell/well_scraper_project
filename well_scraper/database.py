import sqlite3

class WellDatabase:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_table()

    def _create_table(self):
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

    def insert(self, record: dict):
        columns = ",".join(record.keys())
        placeholders = ",".join("?" * len(record))
        self.conn.execute(
            f"INSERT OR REPLACE INTO api_well_data ({columns}) VALUES ({placeholders})",
            tuple(record.values()),
        )
        self.conn.commit()