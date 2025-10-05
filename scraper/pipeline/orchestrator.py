# scraper/pipeline/orchestrator.py
from __future__ import annotations

from typing import Iterable, Any
import httpx
import traceback

from scraper.pipeline.storage import init_engine, get_session, upsert_job
from scraper.settings import settings
from scraper.pipeline.normalize import (
    looks_like_france,
    is_target_lang,
    score_cs,
    score_internship,
)

# Built-in adapters
from scraper.adapters.greenhouse import GreenhouseAdapter
from scraper.adapters.lever import LeverAdapter
from scraper.adapters.ashby import AshbyAdapter

# --- Configure sources here (REPLACE placeholders with real slugs!) ---
SOURCES: list[dict] = [
    # Greenhouse
    {"type": "greenhouse", "slug": "algolia",   "company": "Algolia"},
    {"type": "greenhouse", "slug": "doctolib",  "company": "Doctolib"},
    {"type": "greenhouse", "slug": "datadog",   "company": "Datadog"},

    # Lever
    {"type": "lever", "slug": "qonto",      "company": "Qonto"},
    {"type": "lever", "slug": "blablacar",  "company": "BlaBlaCar"},

    # Ashby (public job board)
    {"type": "ashby", "slug": "alan", "company": "Alan"},

    # SmartRecruiters
    {"type": "smartrecruiters", "slug": "deezer", "company": "Deezer"},

    # Recruitee
    {"type": "recruitee", "slug": "wallarm", "company": "Wallarm"},

    # Teamtailor (public pages, token optional for API)
    {"type": "teamtailor", "slug": "yousign", "company": "Yousign"},

    # Workday (tenant + site)
    {"type": "workday", "slug": {"tenant": "criteo", "site": "Criteo_Career_Site"}, "company": "Criteo"},

    # Workable (HTML-backed adapter)
    {"type": "workable", "slug": "intersec-group", "company": "Intersec"},
    {"type": "workable", "slug": "quandela",       "company": "Quandela"},
    {"type": "workable", "slug": "payplug",        "company": "Payplug"},

    # Personio
    {"type": "personio", "slug": "tradedoubler-en", "company": "Tradedoubler"},
    {"type": "personio", "slug": "sungrow-emea",    "company": "Sungrow EMEA"},
    {"type": "personio", "slug": "autarcenergy",    "company": "Autarc"},
]

# Tencent (Workday)
SOURCES += [
    {"type": "workday",
     "slug": {"tenant": "tencent", "site": "Tencent_Careers"},
     "company": "Tencent"},

    # (optional) Tencent Lightspeed Studios (same tenant, different site)
    {"type": "workday",
     "slug": {"tenant": "tencent", "site": "Lightspeed"},
     "company": "Tencent Lightspeed Studios"},
]


TARGET_FILTERS = {
    "intern_only": True,     # keep internships/stage/alternance
    "cs_only": True,         # keep CS/SE/AI/ML/Data
    "france_only": False,    # set True to keep only France-looking locations
    "lang_fr_en_only": True, # keep FR/EN postings
}

# --- helpers -----------------------------------------------------------------

_PLACEHOLDER_STRINGS = {
    "company1", "company2", "company3", "company4", "company5",
    "company6", "company7", "company8", "company9", "company11",
    "tenant", "careers",
}

def _is_placeholder_slug(slug: Any) -> bool:
    if isinstance(slug, str):
        return slug.strip().lower() in _PLACEHOLDER_STRINGS
    if isinstance(slug, dict):
        t = str(slug.get("tenant", "")).lower()
        s = str(slug.get("site", "")).lower()
        return (t in _PLACEHOLDER_STRINGS) or (s in _PLACEHOLDER_STRINGS)
    return False

def _adapter_label(adapter: Any) -> str:
    company = getattr(adapter, "company", None)
    attrs = [
        getattr(adapter, "board_slug", None),
        getattr(adapter, "company_slug", None),
        getattr(adapter, "account_slug", None),
        getattr(adapter, "tenant", None),
    ]
    tail = next((a for a in attrs if a), None)
    if isinstance(tail, dict):
        tail = f"{tail.get('tenant')}/{tail.get('site')}"
    return f"{adapter.source_name}:{company or tail or 'unknown'}"

# --- adapter factory ----------------------------------------------------------

def iter_adapters() -> Iterable:
    for cfg in SOURCES:
        t = cfg.get("type", "").lower()
        slug = cfg.get("slug")
        if _is_placeholder_slug(slug):
            print(f"[skip] placeholder config for type={t}: slug={slug!r}. Replace with a real board/company slug.")
            continue

        if t == "greenhouse":
            yield GreenhouseAdapter(str(slug), company=cfg.get("company"))

        elif t == "lever":
            yield LeverAdapter(str(slug), company=cfg.get("company"))

        elif t == "ashby":
            yield AshbyAdapter(str(slug), company=cfg.get("company"))

        elif t == "smartrecruiters":
            from scraper.adapters.smartrecruiters import SmartRecruitersAdapter
            yield SmartRecruitersAdapter(str(slug), company=cfg.get("company"))

        elif t == "recruitee":
            from scraper.adapters.recruitee import RecruiteeAdapter
            yield RecruiteeAdapter(str(slug), company=cfg.get("company"))

        elif t == "personio":
            from scraper.adapters.personio import PersonioAdapter
            yield PersonioAdapter(str(slug), company=cfg.get("company"))

        elif t == "bamboohr":
            from scraper.adapters.bamboohr import BambooHRAdapter
            yield BambooHRAdapter(str(slug), company=cfg.get("company"))

        elif t == "workable":
            from scraper.adapters.workable import WorkableAdapter
            yield WorkableAdapter(str(slug), company=cfg.get("company"))

        elif t == "workday":
            from scraper.adapters.workday import WorkdayAdapter
            if not isinstance(slug, dict):
                print(f"[skip] workday requires slug as dict {{'tenant':..., 'site':...}}. Got: {slug!r}")
                continue
            yield WorkdayAdapter(slug, company=cfg.get("company"))

        elif t == "teamtailor":
            from scraper.adapters.teamtailor import TeamtailorAdapter
            yield TeamtailorAdapter(str(slug), company=cfg.get("company"))

        else:
            print(f"[skip] unknown adapter type: {t!r}")

# --- main run ----------------------------------------------------------------

def run_once():
    """Run all adapters once, apply filters, upsert into DB."""
    init_engine(settings.DB_URL)

    total = kept = 0
    per_adapter = {}  # {label: {"seen": int, "kept": int}}

    for adapter in iter_adapters():
        label = _adapter_label(adapter)
        per_adapter.setdefault(label, {"seen": 0, "kept": 0})
        print(f"[run] {label}")

        try:
            for job in adapter.discover():
                per_adapter[label]["seen"] += 1
                total += 1

                # Build a text blob for heuristics
                text_title = job.title or ""
                text_body  = " ".join([
                    job.description_text or "",
                    job.location or "",
                    job.company or "",
                ])

                # Language (FR/EN) filter
                if TARGET_FILTERS["lang_fr_en_only"] and not is_target_lang(text_title + " " + text_body):
                    continue

                # Internship filter
                if TARGET_FILTERS["intern_only"] and score_internship(text_title, text_body) < 2:
                    continue

                # CS/AI/ML/Data filter
                if TARGET_FILTERS["cs_only"] and score_cs(text_title + " " + text_body) < 2:
                    continue

                # France filter (optional)
                if TARGET_FILTERS["france_only"] and not looks_like_france(job.location or ""):
                    continue

                with get_session() as s:
                    upsert_job(s, job)
                kept += 1
                per_adapter[label]["kept"] += 1

        except httpx.HTTPStatusError as e:
            code = e.response.status_code if e.response is not None else "?"
            print(f"[skip] {label} HTTP {code} → {e.request.method} {e.request.url if e.request else ''}")
            continue
        except Exception as e:
            print(f"[skip] {label} error: {e}\n{traceback.format_exc()}")
            continue

    # Summary
    print("—" * 60)
    for label, stats in per_adapter.items():
        print(f"[done] {label:40s} seen={stats['seen']:4d}  kept={stats['kept']:4d}")
    print("—" * 60)
    return total, kept

if __name__ == "__main__":
    t, k = run_once()
    print(f"Scraped {t} postings, kept {k} after filters → DB: {settings.DB_URL}")
