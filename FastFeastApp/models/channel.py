"""Channel — SQLAlchemy model for dim_channel (SCD Type 1)."""
from sqlalchemy import Column, Integer, String
from models.base import Base


class Channel(Base):

    channel_id   = Column(Integer, primary_key=True)
    channel_name = Column(String(5), nullable=False)
