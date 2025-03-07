from datetime import datetime
from pydantic import BaseModel
from typing_extensions import Optional


class SourceConfig(BaseModel):
    source_id: int
    source_type: str
    url_additional: str
    scrape_since: datetime
    token: Optional[str] = None
    end_table: str
