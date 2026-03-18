"""
Base — SQLAlchemy declarative base shared by every model.
All model files import Base from here.
"""
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
