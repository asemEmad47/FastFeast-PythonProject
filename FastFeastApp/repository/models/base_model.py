"""
BaseModel — SQLAlchemy declarative base shared by all entity models.
"""
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
