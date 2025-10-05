from sqlalchemy import Column, String, Boolean, DateTime, Integer, Text, UniqueConstraint
from sqlalchemy.sql import func
from db.base import Base


class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False)
    source_job_id = Column(String(100), nullable=True)
    title = Column(String(300), nullable=False)
    company = Column(String(200), nullable=True)
    location = Column(String(200), nullable=True)
    country_code = Column(String(5), nullable=True)
    is_remote = Column(Boolean, default=False)
    apply_url = Column(String(1000), nullable=False)
    description_text = Column(Text, nullable=True)
    posted_at = Column(String(100), nullable=True)
    scraped_at = Column(DateTime(timezone=True), server_default=func.now())
    language = Column(String(10), nullable=True)
    tags = Column(Text, nullable=True)  # comma-separated
    __table_args__ = (
        UniqueConstraint("source", "source_job_id", name="uq_source_jobid"),
    )
