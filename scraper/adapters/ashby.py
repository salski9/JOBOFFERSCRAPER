from typing import Iterable, Optional
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from scraper.adapters.base import BaseAdapter
from scraper.client.http import get_client
from scraper.models.job import JobModel
from scraper.pipeline.normalize import normalize_tags

class AshbyAdapter(BaseAdapter):
    """
    Robust Ashby adapter:
      1) Parse __NEXT_DATA__ JSON to enumerate jobs (preferred)
      2) Fallback: LD+JSON JobPosting
      3) Last resort: anchor scraping
    """
    source_name = "ashby"

    def __init__(self, org_slug: str, company: Optional[str] = None):
        self.org_slug = org_slug
        self.base = f"https://jobs.ashbyhq.com/{org_slug}"
        self.company = company

    def _from_next_data(self, soup: BeautifulSoup):
        script = soup.find("script", id="__NEXT_DATA__", type="application/json")
        if not script or not script.string:
            return []
        data = json.loads(script.string)
        # common paths observed; keep defensive
        jobs = []
        try:
            pp = data.get("props", {}).get("pageProps", {})
            # Some orgs put jobs directly
            for j in pp.get("jobs", []) or []:
                jobs.append(j)
            # Others under sections -> jobs
            for sec in pp.get("sections", []) or []:
                for j in (sec.get("jobs") or []):
                    jobs.append(j)
        except Exception:
            pass
        return jobs

    def _from_ldjson(self, soup: BeautifulSoup):
        jobs = []
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                obj = json.loads(tag.string or "{}")
            except Exception:
                continue
            if isinstance(obj, dict) and obj.get("@type") == "JobPosting":
                jobs.append(obj)
            elif isinstance(obj, list):
                jobs.extend([o for o in obj if isinstance(o, dict) and o.get("@type") == "JobPosting"])
        return jobs

    def discover(self) -> Iterable[JobModel]:
        with get_client() as client:
            r = client.get(self.base)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")

        # Strategy 1: __NEXT_DATA__
        jobs = self._from_next_data(soup)
        if jobs:
            for j in jobs:
                title = (j.get("title") or j.get("name") or "").strip()
                loc = j.get("location") or (j.get("office") or {}).get("name")
                url = j.get("jobUrl") or j.get("absoluteUrl") or urljoin(self.base, j.get("canonicalPath") or "")
                desc = j.get("description") or j.get("descriptionText") or ""
                jid = j.get("id") or j.get("slug")
                yield JobModel(
                    source=self.source_name,
                    source_job_id=str(jid) if jid else None,
                    title=title,
                    company=self.company,
                    location=loc,
                    apply_url=url or self.base,
                    description_text=desc,
                    posted_at=j.get("publishedAt") or j.get("createdAt"),
                    tags=normalize_tags(title, desc),
                )
            return

        # Strategy 2: LD+JSON
        ld = self._from_ldjson(soup)
        if ld:
            for j in ld:
                title = (j.get("title") or "").strip()
                loc_obj = j.get("jobLocation", {})
                if isinstance(loc_obj, list) and loc_obj:
                    loc_obj = loc_obj[0]
                loc = None
                if isinstance(loc_obj, dict):
                    addr = loc_obj.get("address", {})
                    city = addr.get("addressLocality") or ""
                    country = addr.get("addressCountry") or ""
                    loc = ", ".join([x for x in [city, country] if x]) or None
                url = j.get("hiringOrganization", {}).get("sameAs") or j.get("url") or self.base
                desc = j.get("description") or ""
                jid = j.get("identifier", {}).get("value") if isinstance(j.get("identifier"), dict) else None
                yield JobModel(
                    source=self.source_name,
                    source_job_id=str(jid) if jid else None,
                    title=title,
                    company=self.company,
                    location=loc,
                    apply_url=url,
                    description_text=desc,
                    posted_at=j.get("datePosted") or j.get("validThrough"),
                    tags=normalize_tags(title, desc),
                )
            return

        # Strategy 3: anchor fallback
        seen = set()
        for a in soup.select("a[href*='/jobs/'], a[href*='/job/']"):
            href = a.get("href", "")
            title = a.get_text(strip=True)
            if len(title) < 3:
                continue
            job_url = urljoin(self.base, href)
            if job_url in seen:
                continue
            seen.add(job_url)
            yield JobModel(
                source=self.source_name,
                source_job_id=None,
                title=title,
                company=self.company,
                location=None,
                apply_url=job_url,
                description_text=None,
                posted_at=None,
                tags=normalize_tags(title, None),
            )
