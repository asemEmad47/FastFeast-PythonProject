from dataclasses import dataclass
from models.base import Base

@dataclass
class City(Base):
    city_id:   int
    city_name: str
    country:   str
    timezone:  str