from typing import Iterable, Optional
from scraper.adapters.base import BaseAdapter
from scraper.client.http import get_client
from scraper.models.job import JobModel
from scraper.pipeline.normalize import normalize_tags

# API docs: https://api.smartrecruiters.com/v1/companies/{company}/postings
API_BASE = "https://api.smartrecruiters.com/v1/companies/{company}/postings"
DETAIL_BASE = "https://api.smartrecruiters.com/v1/companies/{company}/postings/{posting_id}"

class SmartRecruitersAdapter(BaseAdapter):
    """Public SmartRecruiters postings API with pagination + detail fetch for apply URL/description."""
    source_name = "smartrecruiters"

    def __init__(self, company_slug: str, company: Optional[str] = None):
        self.company_slug = company_slug
        self.company = company

    def discover(self) -> Iterable[JobModel]:
        params = {"limit": 100}
        next_page = None
        with get_client() as client:
            while True:
                if next_page:
                    params["nextPageId"] = next_page
                r = client.get(API_BASE.format(company=self.company_slug), params=params)
                r.raise_for_status()
                data = r.json() or {}
                items = data.get("content") or data.get("data") or data.get("postings") or []
                for it in items:
                    pid = it.get("id") or it.get("identifier") or it.get("refNumber")
                    title = (it.get("name") or it.get("title") or "").strip()

                    # Location string
                    loc = None
                    loc_obj = it.get("location") or {}
                    if isinstance(loc_obj, dict):
                        city = loc_obj.get("city") or ""
                        region = loc_obj.get("region") or ""
                        country = loc_obj.get("countryCode") or loc_obj.get("country") or ""
                        loc = ", ".join([x for x in [city, region, country] if x]) or None

                    # Detail call to get apply_url & description
                    apply_url, desc = None, None
                    if pid:
                        rd = client.get(DETAIL_BASE.format(company=self.company_slug, posting_id=pid))
                        if rd.status_code == 200:
                            jd = rd.json() or {}
                            apply_url = (
                                jd.get("applyUrl")
                                or (jd.get("jobAd") or {}).get("applyUrl")
                            )
                            desc = (jd.get("jobAd") or {}).get("sections", {}).get("jobDescription", {}).get("text")

                    apply_url = apply_url or it.get("applyUrl") or ""
                    yield JobModel(
                        source=self.source_name,
                        source_job_id=str(pid) if pid else None,
                        title=title,
                        company=self.company,
                        location=loc,
                        apply_url=apply_url,
                        description_text=desc,
                        posted_at=it.get("releasedDate") or it.get("createdOn") or it.get("updatedOn"),
                        tags=normalize_tags(title, desc or ""),
                    )
                next_page = data.get("nextPageId")
                if not next_page:
                    break
