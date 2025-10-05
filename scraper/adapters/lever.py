from typing import Iterable, Optional
from datetime import datetime, timezone
from scraper.adapters.base import BaseAdapter
from scraper.client.http import get_client
from scraper.models.job import JobModel
from scraper.pipeline.normalize import normalize_tags

def _ms_to_iso(value) -> Optional[str]:
    """Lever uses epoch milliseconds; return RFC3339 string."""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()
    if isinstance(value, str):
        return value
    return None

class LeverAdapter(BaseAdapter):
    source_name = "lever"

    def __init__(self, company_slug: str, company: Optional[str] = None):
        self.company_slug = company_slug
        self.company = company

    def discover(self) -> Iterable[JobModel]:
        url = f"https://api.lever.co/v0/postings/{self.company_slug}?mode=json"
        with get_client() as client:
            r = client.get(url)
            r.raise_for_status()
            data = r.json()
        for j in data:
            title = (j.get("text") or "").strip()
            loc = (j.get("categories") or {}).get("location")
            desc = j.get("descriptionPlain") or j.get("description")
            posted = _ms_to_iso(j.get("createdAt") or j.get("updatedAt"))

            yield JobModel(
                source=self.source_name,
                source_job_id=j.get("id"),
                title=title,
                company=self.company,
                location=loc,
                apply_url=j.get("hostedUrl") or j.get("applyUrl") or "",
                description_text=desc,
                posted_at=posted,
                tags=normalize_tags(title, desc),
            )
