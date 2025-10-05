from typing import Iterable, Optional
from urllib.parse import urljoin
from scraper.adapters.base import BaseAdapter
from scraper.client.http import get_client
from scraper.models.job import JobModel
from scraper.pipeline.normalize import normalize_tags

class BambooHRAdapter(BaseAdapter):
    """
    BambooHR public careers list JSON:
    https://<company>.bamboohr.com/careers/list
    """
    source_name = "bamboohr"

    def __init__(self, company_slug: str, company: Optional[str] = None):
        self.company_slug = company_slug
        self.base = f"https://{company_slug}.bamboohr.com/careers/"
        self.company = company

    def discover(self) -> Iterable[JobModel]:
        with get_client() as client:
            r = client.get(urljoin(self.base, "list"), headers={"Accept": "application/json"})
            r.raise_for_status()
            data = r.json() or {}
        positions = data.get("positions") or data.get("result") or []
        for p in positions:
            title = (p.get("jobOpeningName") or p.get("jobTitle") or "").strip()
            loc_obj = p.get("location") or {}
            loc_parts = [loc_obj.get("city"), loc_obj.get("state"), loc_obj.get("country")]
            loc = ", ".join([x for x in loc_parts if x]) or None
            apply_url = p.get("jobPostingUrl") or p.get("jobUrl") or self.base
            pid = p.get("jobOpeningId") or p.get("id")
            desc = p.get("jobDescription") or p.get("description")

            yield JobModel(
                source=self.source_name,
                source_job_id=str(pid) if pid else None,
                title=title,
                company=self.company,
                location=loc,
                apply_url=apply_url,
                description_text=desc,
                posted_at=p.get("dateOpening") or p.get("postedOn"),
                tags=normalize_tags(title, desc or ""),
            )
