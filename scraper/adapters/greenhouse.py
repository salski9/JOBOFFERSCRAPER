from typing import Iterable
from scraper.adapters.base import BaseAdapter
from scraper.client.http import get_client
from scraper.models.job import JobModel
from scraper.pipeline.normalize import normalize_tags


API_BASE = "https://boards-api.greenhouse.io/v1/boards"


class GreenhouseAdapter(BaseAdapter):
    source_name = "greenhouse"


    def __init__(self, board_slug: str, company: str | None = None):
        self.board_slug = board_slug
        self.company = company


    def discover(self) -> Iterable[JobModel]:
        url = f"{API_BASE}/{self.board_slug}/jobs?content=true"
        with get_client() as client:
            r = client.get(url)
            r.raise_for_status()
            data = r.json()
            for j in data.get("jobs", []):
                title = (j.get("title") or "").strip()
                loc = (j.get("location") or {}).get("name")
                desc = (j.get("content") or "")
                yield JobModel(
                    source=self.source_name,
                    source_job_id=str(j.get("id")) if j.get("id") else None,
                    title=title,
                    company=self.company,
                    location=loc,
                    apply_url=j.get("absolute_url") or "",
                    description_text=desc,
                    posted_at=j.get("updated_at") or j.get("created_at"),
                    tags=normalize_tags(title, desc),
                )