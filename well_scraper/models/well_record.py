# ===================================
# well_scraper/models/well_record.py
# ===================================
from dataclasses import dataclass
from typing import Optional

@dataclass
class WellRecord:
    API: str
    Operator: Optional[str] = None
    Status: Optional[str] = None
    Well_Type: Optional[str] = None
    Work_Type: Optional[str] = None
    Directional_Status: Optional[str] = None
    Multi_Lateral: Optional[str] = None
    Mineral_Owner: Optional[str] = None
    Surface_Owner: Optional[str] = None
    Surface_Location: Optional[str] = None
    GL_Elevation: Optional[float] = None
    KB_Elevation: Optional[float] = None
    DF_Elevation: Optional[float] = None
    Single_Multiple_Completion: Optional[str] = None
    Potash_Waiver: Optional[str] = None
    Spud_Date: Optional[str] = None
    Last_Inspection: Optional[str] = None
    TVD: Optional[float] = None
    Latitude: Optional[float] = None
    Longitude: Optional[float] = None
    CRS: Optional[str] = None