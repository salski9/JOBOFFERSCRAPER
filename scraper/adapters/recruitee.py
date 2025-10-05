from typing import Iterable, Optional
from urllib.parse import urljoin
from scraper.adapters.base import BaseAdapter
from scraper.client.http import get_client
from scraper.models.job import JobModel
from scraper.pipeline.normalize import normalize_tags

class RecruiteeAdapter(BaseAdapter):
    """
    Public Recruitee offers JSON.
    Typical endpoint: https://<company>.recruitee.com/api/offers/?limit=100
    """
    source_name = "recruitee"

    def __init__(self, company_slug: str, company: Optional[str] = None):
        self.company_slug = company_slug
        self.base = f"https://{company_slug}.recruitee.com/"
        self.company = company

    def discover(self) -> Iterable[JobModel]:
        url = urljoin(self.base, "api/offers/")
        params = {"limit": 200}
        with get_client() as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            data = r.json() or {}
        offers = data.get("offers") or data.get("items") or []
        for o in offers:
            title = (o.get("title") or o.get("name") or "").strip()

            # Location
            loc = None
            locs = o.get("locations")
            if isinstance(locs, list) and locs:
                city = (locs[0] or {}).get("city") or ""
                country = (locs[0] or {}).get("country_code") or ""
                loc = ", ".join([x for x in [city, country] if x]) or None

            # Apply URL
            url_path = o.get("careers_url") or o.get("slug")
            apply_url = urljoin(self.base, f"o/{url_path}") if url_path else self.base

            desc = o.get("description") or o.get("description_preview")
            oid = o.get("id")

            yield JobModel(
                source=self.source_name,
                source_job_id=str(oid) if oid else None,
                title=title,
                company=self.company,
                location=loc,
                apply_url=apply_url,
                description_text=desc,
                posted_at=o.get("created_at") or o.get("updated_at"),
                tags=normalize_tags(title, desc or ""),
            )
