"""Team — SQLAlchemy model for dim_team (SCD Type 1)."""
from sqlalchemy import Column, Integer, String
from models.base import Base


class Team(Base):

    team_id   = Column(Integer, primary_key=True)
    team_name = Column(String(15), nullable=False)
