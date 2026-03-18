"""
DatabaseManager — Singleton Pattern.

One instance, one engine, one session factory.
All repositories call session_scope() — never manage sessions themselves.
"""
from __future__ import annotations
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from config.settings import DATABASE_URL


class DatabaseManager:

    _instance: DatabaseManager = None

    def __new__(cls) -> DatabaseManager:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._engine  = create_engine(DATABASE_URL, echo=False)
            cls._instance._Session = sessionmaker(bind=cls._instance._engine)
        return cls._instance

    def get_session(self) -> Session:
        return self._Session()

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope — commit on success, rollback on error."""
        session = self._Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
