from typing import Iterable, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from scraper.adapters.base import BaseAdapter
from scraper.client.http import get_client
from scraper.models.job import JobModel
from scraper.pipeline.normalize import normalize_tags

class TeamtailorAdapter(BaseAdapter):
    """HTML adapter for Teamtailor career sites (public pages)."""
    source_name = "teamtailor"

    def __init__(self, company_slug: str, company: Optional[str] = None):
        self.company_slug = company_slug
        self.base = f"https://{company_slug}.teamtailor.com/"
        self.company = company

    def discover(self) -> Iterable[JobModel]:
        list_url = urljoin(self.base, "jobs")
        with get_client() as client:
            r = client.get(list_url)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")

        seen = set()
        for a in soup.select("a[href*='/jobs/']"):
            href = a.get("href", "")
            title = a.get_text(strip=True)
            if len(title) < 3:
                continue
            url = urljoin(self.base, href)
            if url in seen:
                continue
            seen.add(url)
            yield JobModel(
                source=self.source_name,
                source_job_id=None,
                title=title,
                company=self.company,
                location=None,
                apply_url=url,
                description_text=None,
                posted_at=None,
                tags=normalize_tags(title, None),
            )
