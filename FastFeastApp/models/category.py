from dataclasses import dataclass
from models.base import Base

@dataclass
class Channel(Base):
    channel_id:   int
    channel_name: str