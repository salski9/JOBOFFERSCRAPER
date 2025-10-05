from pydantic import BaseModel, HttpUrl
from typing import Optional


class JobModel(BaseModel):
    source: str
    source_job_id: Optional[str] = None
    title: str
    company: Optional[str] = None
    location: Optional[str] = None
    country_code: Optional[str] = None
    is_remote: bool = False
    apply_url: str
    description_text: Optional[str] = None
    posted_at: Optional[str] = None
    language: Optional[str] = None
    tags: list[str] = []

