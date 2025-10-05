from typing import Iterable
from scraper.models.job import JobModel


class BaseAdapter:
    source_name: str
    def discover(self) -> Iterable[JobModel]:
        raise NotImplementedError