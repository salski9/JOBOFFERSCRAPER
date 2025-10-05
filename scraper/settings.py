from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_URL: str = "sqlite:///./jobs.db"
    REQUEST_TIMEOUT: float = 20.0
    USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36 JobOfferScraper/0.1"
    )
    # Keywords for internship detection and CS-related roles
    INTERNSHIP_KEYWORDS: list[str] = [
    "stage", "stagiaire", "intern", "internship", "alternance", "apprentissage"
    ]
    CS_KEYWORDS: list[str] = [
    # broad CS/SE/AI/ML/Data
    "software", "engineer", "developer", "dev", "backend", "frontend", "fullstack",
    "data", "ml", "machine learning", "ai", "artificial intelligence", "deep learning",
    "nlp", "cv", "computer vision", "mle", "sre", "devops", "cloud", "python",
    "java", "c++", "golang", "rust", "typescript", "sql"
    ]
    FRANCE_HINTS: list[str] = [
    "france","paris","lyon","lille","nantes","rennes","toulouse","bordeaux",
    "marseille","grenoble","nice","remote france","fr"
    ]


class Config:
    env_file = ".env"


settings = Settings()