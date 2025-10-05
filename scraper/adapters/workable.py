from typing import Iterable, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from scraper.adapters.base import BaseAdapter
from scraper.client.http import get_client
from scraper.models.job import JobModel
from scraper.pipeline.normalize import normalize_tags

class WorkableAdapter(BaseAdapter):
    """
    Workable public jobs API v3 with HTML fallback.
    API: https://apply.workable.com/api/v3/accounts/{account}/jobs?state=published&limit=100
    """
    source_name = "workable"

    def __init__(self, account_slug: str, company: Optional[str] = None):
        self.account_slug = account_slug
        self.company = company

    def discover(self) -> Iterable[JobModel]:
        api = f"https://apply.workable.com/api/v3/accounts/{self.account_slug}/jobs"
        params = {"state": "published", "limit": 100}
        with get_client() as client:
            r = client.get(api, params=params)
            if r.status_code == 200:
                data = r.json() or {}
                for it in data.get("results", []) or []:
                    title = (it.get("title") or "").strip()
                    loc = ", ".join([x for x in [it.get("city"), it.get("country")] if x]) or None
                    url = it.get("url") or it.get("application_url") or f"https://apply.workable.com/{self.account_slug}/"
                    jid = it.get("id") or it.get("shortcode")
                    desc = it.get("description")

                    yield JobModel(
                        source=self.source_name,
                        source_job_id=str(jid) if jid else None,
                        title=title,
                        company=self.company,
                        location=loc,
                        apply_url=url,
                        description_text=desc,
                        posted_at=it.get("published_on") or it.get("created_at"),
                        tags=normalize_tags(title, desc or ""),
                    )
                return  # API worked; stop here

        # Fallback to HTML
        base = f"https://apply.workable.com/{self.account_slug}/"
        with get_client() as client:
            r = client.get(base)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
        seen = set()
        for a in soup.select("a[href*='/jobs/']"):
            href = a.get("href", "")
            title = a.get_text(strip=True)
            if len(title) < 3:
                continue
            url = urljoin(base, href)
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
