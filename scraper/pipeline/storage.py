from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.base import Base
from db.schemas import Job
from scraper.models.job import JobModel

_engine = None
_Session = None


def init_engine(db_url: str):
    global _engine, _Session
    _engine = create_engine(db_url, future=True)
    Base.metadata.create_all(_engine)
    _Session = sessionmaker(bind=_engine, expire_on_commit=False)


@contextmanager
def get_session():
    sess = _Session()
    try:
        yield sess
        sess.commit()
    except Exception:
        sess.rollback()
        raise
    finally:
        sess.close()


def upsert_job(sess, jm: JobModel):
    # Upsert by (source, source_job_id) when available; else uniqueness by (apply_url, title)
    if jm.source_job_id:
        existing = (
            sess.query(Job)
            .filter(Job.source == jm.source, Job.source_job_id == jm.source_job_id)
            .one_or_none()
        )
    else:
        existing = (
            sess.query(Job)
            .filter(Job.source == jm.source, Job.apply_url == jm.apply_url, Job.title == jm.title)
            .one_or_none()
        )
    
    if existing:
        existing.location = jm.location
        existing.country_code = jm.country_code
        existing.is_remote = jm.is_remote
        existing.apply_url = jm.apply_url
        existing.description_text = jm.description_text
        existing.posted_at = jm.posted_at
        existing.language = jm.language
        existing.company = jm.company
        existing.tags = ",".join(jm.tags)
        return existing
    
    row = Job(
        source=jm.source,
        source_job_id=jm.source_job_id,
        title=jm.title,
        company=jm.company,
        location=jm.location,
        country_code=jm.country_code,
        is_remote=jm.is_remote,
        apply_url=jm.apply_url,
        description_text=jm.description_text,
        posted_at=jm.posted_at,
        language=jm.language,
        tags=",".join(jm.tags),
    )
    sess.add(row)
    return row