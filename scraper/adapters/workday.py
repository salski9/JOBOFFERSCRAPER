from typing import Iterable, Optional
from urllib.parse import urljoin
import httpx

from scraper.adapters.base import BaseAdapter
from scraper.client.http import get_client
from scraper.models.job import JobModel
from scraper.pipeline.normalize import normalize_tags


class WorkdayAdapter(BaseAdapter):
    """
    Generic Workday adapter.

    slug must be a dict: {"tenant": "...", "site": "CareersSiteName"}.

    Strategy:
      1) Try Candidate Experience Service (CXS):
         POST https://{tenant}.wd{N}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs
      2) Fallback to legacy:
         POST https://{tenant}.wd{N}.myworkdayjobs.com/{site}/search

    We try root host (no wdN) then wd1, wd2, wd3, wd5. Skips non-JSON or DNS failures.
    """
    source_name = "workday"

    def __init__(self, slug: dict, company: Optional[str] = None):
        self.tenant = slug.get("tenant")
        self.site = slug.get("site")
        self.company = company
        self.suffixes = [None, "1", "2", "3", "5"]  # try root first

    # ---------- host helpers ----------
    def _host_for(self, suffix: Optional[str]) -> str:
        if suffix is None:
            return f"https://{self.tenant}.myworkdayjobs.com/"
        return f"https://{self.tenant}.wd{suffix}.myworkdayjobs.com/"

    # ---------- HTTP helpers ----------
    _HDRS = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        # A plain UA avoids some anti-bot edges where HTML is returned with 200
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    }

    def _post_json(self, client: httpx.Client, url: str, payload: dict) -> Optional[dict]:
        try:
            r = client.post(url, json=payload, headers=self._HDRS, timeout=20.0)
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.HTTPError):
            return None
        if r.status_code != 200:
            return None
        # ensure JSON (some tenants return HTML with 200)
        ctype = r.headers.get("Content-Type", "")
        if "json" not in ctype.lower():
            return None
        try:
            return r.json()
        except Exception:
            return None

    # ---------- CXS mode ----------
    def _discover_cxs(self, client: httpx.Client, host: str) -> Iterable[JobModel]:
        url = f"{host}wday/cxs/{self.tenant}/{self.site}/jobs"
        offset = 0
        page = 0
        while True:
            payload = {"appliedFacets": {}, "limit": 50, "offset": offset, "searchText": ""}
            data = self._post_json(client, url, payload)
            if not data:
                break
            items = data.get("jobPostings") or []
            if not items:
                break
            for it in items:
                title = (it.get("title") or it.get("title_friendly") or "").strip()
                loc = it.get("locationsText") or it.get("locations") or it.get("location")
                # CXS usually gives 'externalPath' relative to site base
                path = it.get("externalPath") or it.get("externalUrlPath") or it.get("url") or ""
                base = urljoin(host, f"{self.site}/")
                job_url = urljoin(base, path)
                jid = it.get("id") or it.get("bulletFields")
                desc = it.get("shortDescription")
                yield JobModel(
                    source=self.source_name,
                    source_job_id=str(jid) if jid else None,
                    title=title,
                    company=self.company,
                    location=loc,
                    apply_url=job_url or base,
                    description_text=desc,
                    posted_at=it.get("postedOn") or it.get("publicationDate"),
                    tags=normalize_tags(title, desc or ""),
                )
            if len(items) < 50:
                break
            page += 1
            offset = page * 50

    # ---------- legacy /search mode ----------
    def _discover_legacy(self, client: httpx.Client, host: str) -> Iterable[JobModel]:
        base = urljoin(host, f"{self.site}/")
        url = urljoin(base, "search")
        offset = 0
        page = 0
        while True:
            payload = {"appliedFacets": {}, "limit": 50, "offset": offset, "searchText": ""}
            data = self._post_json(client, url, payload)
            if not data:
                break
            items = data.get("jobPostings") or data.get("items") or []
            if not items:
                break
            for it in items:
                title = (it.get("title") or it.get("title_friendly") or "").strip()
                loc = it.get("locationsText") or it.get("locations") or it.get("location")
                path = it.get("externalPath") or it.get("externalUrlPath") or it.get("url") or ""
                job_url = urljoin(base, path)
                jid = it.get("id") or it.get("bulletFields")
                desc = it.get("shortDescription")
                yield JobModel(
                    source=self.source_name,
                    source_job_id=str(jid) if jid else None,
                    title=title,
                    company=self.company,
                    location=loc,
                    apply_url=job_url or base,
                    description_text=desc,
                    posted_at=it.get("postedOn") or it.get("publicationDate"),
                    tags=normalize_tags(title, desc or ""),
                )
            if len(items) < 50:
                break
            page += 1
            offset = page * 50

    # ---------- main ----------
    def discover(self) -> Iterable[JobModel]:
        with get_client() as client:
            for suffix in self.suffixes:
                host = self._host_for(suffix)
                got_any = False

                # 1) Try CXS
                try:
                    for job in self._discover_cxs(client, host):
                        got_any = True
                        yield job
                except Exception:
                    # swallow and try legacy
                    pass

                if got_any:
                    return

                # 2) Try legacy /search
                try:
                    for job in self._discover_legacy(client, host):
                        got_any = True
                        yield job
                except Exception:
                    pass

                if got_any:
                    return
