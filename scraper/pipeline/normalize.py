# scraper/pipeline/normalize.py
import re
from typing import Sequence
from langdetect import detect, LangDetectException

# ---------------------------
# Internship detection (FR+EN)
# ---------------------------

# Strong signals if present in the TITLE
INTERNSHIP_TERMS_TITLE = [
    # EN
    r"\bintern\b",
    r"\binternship\b",
    r"\bco[- ]?op\b",
    r"\btrainee\b",
    r"\bworking[- ]student\b",
    r"\bsummer[- ]?intern(ship)?\b",

    # FR
    r"\bstage\b",
    r"\bstagiaire\b",
    r"\bstage\s+(de|en)\b",
    r"\bpfe\b",  # projet de fin d'études
    r"\b(projet|projet de) fin d[’']études\b",
    r"\balternance\b",
    r"\balternant(e)?\b",
    r"\bapprentissage\b",
    r"\bcontrat d[’']?apprentissage\b",
    r"\bcontrat de professionn?alisation\b",
]

# Additional signals allowed in the BODY/description
INTERNSHIP_TERMS_BODY = INTERNSHIP_TERMS_TITLE + [
    r"\bplacement\b",
    r"\bindustrial placement\b",
    r"\bgraduate? program\b",
    r"\bjunior\b",
    r"\bétudiant(e)?\b",
    r"\bjeune diplômé(e)?\b",
    r"\bVIE\b",  # early-career program (sometimes acceptable)
]

# Negative seniority cues (we only penalize if no positive internship signal)
NEGATIVE_NON_INTERN = [
    r"\bdirector\b",
    r"\bmanager\b",
    r"\bhead\b",
    r"\barchitect\b",
    r"\bsenior\b",
    r"\bstaff\b",
    r"\blead\b",
    r"\bprincipal\b",
    # Contracts often appear on internship pages too, so don't over-penalize CDI/CDD.
]

# -----------------------------------
# Computer Science / Engineering tags
# -----------------------------------
CS_BUCKETS: dict[str, list[str]] = {
    # Generic SWE/CS
    "backend": [
        r"\bbackend\b", r"\bback[- ]?end\b", r"\bapi\b",
        r"\bmicroservices?\b", r"\bdistributed systems?\b",
        r"\bscalab(le|ility)\b", r"\bconcurrency\b",
    ],
    "frontend": [
        r"\bfrontend\b", r"\bfront[- ]?end\b", r"\bui\b", r"\bux\b",
        r"\breact\b", r"\bvue\b", r"\bangular\b", r"\btypescript\b", r"\bjavascript\b",
    ],
    "mobile": [
        r"\bmobile\b", r"\bandroid\b", r"\bios\b", r"\bswift\b", r"\bkotlin\b",
        r"\breact native\b", r"\bflutter\b",
    ],
    "data": [
        r"\bdata engineer\b", r"\bdata scientist\b", r"\bdata analyst\b",
        r"\b(etl|elt)\b", r"\bdata (platform|pipeline)\b",
        r"\bsql\b", r"\bwarehouse\b", r"\blacke?\b", r"\bspark\b", r"\bhadoop\b",
        r"\bdbt\b", r"\bairflow\b", r"\bbigquery\b", r"\bsnowflake\b", r"\bpostgres\b", r"\bmysql\b",
    ],
    "ai-ml": [
        r"\bml\b", r"\bmachine learning\b", r"\bapprentissage automatique\b",
        r"\bdeep[- ]?learning\b", r"\b(pytorch|tensorflow|keras)\b",
        r"\bnlp\b", r"\bnatural language\b",
        r"\bcomputer vision\b", r"\bvision par ordinateur\b",
        r"\breinforcement learning\b",
        r"\bml[- ]?ops?\b",
    ],
    "devops-sre": [
        r"\bdevops\b", r"\bsre\b", r"\bsite reliability\b",
        r"\bkubernetes\b", r"\bdocker\b", r"\bterraform\b", r"\bansible\b",
        r"\bci/?cd\b", r"\bprometheus\b", r"\bgrafana\b",
        r"\baws\b", r"\bgcp\b", r"\bazure\b",
        r"\bcloud\b", r"\bplatform engineer\b",
    ],
    "security": [
        r"\b(security|sécurité)\b", r"\bappsec\b", r"\bcyber\b",
        r"\bpentest(ing)?\b", r"\bsiem\b", r"\bsoc\b", r"\biam\b",
        r"\bzero[- ]?trust\b", r"\bcryptograph(y|ie)\b",
    ],
    "systems-embedded": [
        r"\bsystem(s)?\b", r"\bkernel\b", r"\bembedded\b", r"\bfirmware\b",
        r"\brtos\b", r"\btemps réel\b", r"\bdsp\b",
    ],
    "languages": [
        r"\bpython\b", r"\bjava\b", r"\bc\+\+\b", r"\bc#\b",
        r"\bgolang\b", r"\brust\b", r"\btypescript\b", r"\bnode\.?js\b",
    ],
}

# ----------------
# France detection
# ----------------
FRANCE_HINTS = [
    r"\bfrance\b", r"\bfr\b",
    r"\bparis\b", r"\blyon\b", r"\blille\b", r"\bnantes\b", r"\brennes\b",
    r"\btoulouse\b", r"\bbordeaux\b", r"\bmarseille\b", r"\bgrenoble\b", r"\bnice\b",
    r"\bstrasbourg\b", r"\bmontpellier\b",
    r"\bîle[- ]?de[- ]?france\b", r"\bile[- ]?de[- ]?france\b",
    r"\bremote france\b", r"\bfrance remote\b",
]

# ---------------
# Compiled regex
# ---------------
def _join(pats: list[str]) -> re.Pattern:
    return re.compile("|".join(pats), re.I)

_title_re = _join(INTERNSHIP_TERMS_TITLE)
_body_re  = _join(INTERNSHIP_TERMS_BODY)
_neg_re   = _join(NEGATIVE_NON_INTERN)
_fr_re    = _join(FRANCE_HINTS)

# For CS scoring we want to count bucket hits, not a single big regex
_CS_BUCKET_RES: dict[str, list[re.Pattern]] = {
    bucket: [re.compile(p, re.I) for p in pats] for bucket, pats in CS_BUCKETS.items()
}

# ---------------------
# Language detection FR/EN
# ---------------------
def detect_lang(text: str) -> str | None:
    try:
        return detect(text)
    except LangDetectException:
        return None

def is_target_lang(text: str) -> bool:
    lang = detect_lang(text or "")
    # allow unknown/short texts too
    return (lang in {"fr", "en"}) or (lang is None)

# ---------------------
# Scoring & heuristics
# ---------------------
def score_internship(title: str, body: str = "") -> int:
    """
    Returns a score; treat >= 2 as internship.
    - Title signals are weighted heavily (+3)
    - Body signals add up to +2 (capped)
    - Seniority/management terms penalize when no positive signal was found
    """
    score = 0
    title = title or ""
    body = body or ""

    if _title_re.search(title):
        score += 3

    # Add up to +2 for body matches
    body_hits = len(_body_re.findall(body))
    score += min(body_hits, 2)

    # Penalize clearly non-intern senior roles if no positive signal
    if score == 0 and _neg_re.search(title + " " + body):
        score -= 2

    return score

def score_cs(text: str) -> int:
    """
    Returns a score; treat >= 2 as CS/Software/AI/Data relevant.
    Counts how many distinct CS buckets are hit in the text (max 5).
    """
    txt = (text or "")
    buckets_hit = 0
    for bucket, patterns in _CS_BUCKET_RES.items():
        if any(p.search(txt) for p in patterns):
            buckets_hit += 1
    # Cap to keep scale small
    return min(buckets_hit, 5)

def looks_like_france(text: str) -> bool:
    return bool(_fr_re.search(text or ""))

# ---------------------
# Tag normalization
# ---------------------
def normalize_tags(title: str, description: str | None) -> list[str]:
    """
    Produce compact, meaningful tags from buckets + common techs.
    """
    base = (title or "") + "\n" + (description or "")
    base_l = base.lower()
    tags: list[str] = []

    # Bucket tags
    bucket_to_label = {
        "backend": "backend",
        "frontend": "frontend",
        "mobile": "mobile",
        "data": "data",
        "ai-ml": "ai-ml",
        "devops-sre": "devops-sre",
        "security": "security",
        "systems-embedded": "systems",
    }
    for bucket, label in bucket_to_label.items():
        if any(p.search(base_l) for p in _CS_BUCKET_RES[bucket]):
            tags.append(label)

    # Popular tech tags (short curated list)
    techs = [
        "python", "java", "c++", "c#", "golang", "rust",
        "typescript", "javascript", "react", "node", "kubernetes",
        "docker", "sql", "postgres", "pytorch", "tensorflow",
        "spark", "airflow", "dbt",
    ]
    tags.extend([t for t in techs if t in base_l])

    # De-dupe & limit
    return sorted(set(tags))[:15]
