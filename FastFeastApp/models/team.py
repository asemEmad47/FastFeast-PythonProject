from dataclasses import dataclass
from models.base import Base

@dataclass
class Team(Base):
    team_id:   int
    team_name: str