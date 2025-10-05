import httpx
from scraper.settings import settings


_headers = {"User-Agent": settings.USER_AGENT}


def get_client() -> httpx.Client:
    return httpx.Client(headers=_headers, timeout=settings.REQUEST_TIMEOUT, follow_redirects=True)

