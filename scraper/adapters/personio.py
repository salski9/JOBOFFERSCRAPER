from typing import Iterable, Optional
from urllib.parse import urljoin
from scraper.adapters.base import BaseAdapter
from scraper.client.http import get_client
from scraper.models.job import JobModel
from scraper.pipeline.normalize import normalize_tags

class PersonioAdapter(BaseAdapter):
    """
    Personio careers search JSON (tries en → fr → default).
    Some tenants return a LIST; others return a dict with "jobs"/"positions".
    """
    source_name = "personio"

    def __init__(self, company_slug: str, company: Optional[str] = None):
        self.company_slug = company_slug
        self.base = f"https://{company_slug}.jobs.personio.de/"
        self.company = company

    def _fetch(self):
        endpoints = ["search.json?language=en", "search.json?language=fr", "search.json"]
        with get_client() as client:
            for ep in endpoints:
                url = urljoin(self.base, ep)
                r = client.get(url)
                if r.status_code == 200:
                    try:
                        return r.json()
                    except Exception:
                        return None
        return None

    def discover(self) -> Iterable[JobModel]:
        data = self._fetch()
        if not data:
            return

        # Normalize to a list of job dicts
        if isinstance(data, list):
            jobs = data
        elif isinstance(data, dict):
            jobs = data.get("jobs") or data.get("positions") or []
        else:
            jobs = []

        for j in jobs:
            title = (j.get("name") or j.get("title") or "").strip()
            loc = j.get("office") or j.get("locations") or j.get("employmentOffice") or j.get("location")

            # Apply URL
            apply_url = j.get("url") or j.get("jobUrl")
            if not apply_url:
                jid = j.get("id") or j.get("positionId")
                if jid:
                    apply_url = urljoin(self.base, f"job/{jid}")

            desc = j.get("description") or j.get("descriptionText")
            jid = j.get("id") or j.get("positionId")

            yield JobModel(
                source=self.source_name,
                source_job_id=str(jid) if jid else None,
                title=title,
                company=self.company,
                location=loc,
                apply_url=apply_url or self.base,
                description_text=desc,
                posted_at=j.get("publishedAt") or j.get("createdAt") or j.get("created_at"),
                tags=normalize_tags(title, desc or ""),
            )
