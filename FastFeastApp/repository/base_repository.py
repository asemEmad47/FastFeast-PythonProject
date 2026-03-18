"""
BaseRepository<T> — Repository Pattern.

Generic base for all table repositories.
Provides CRUD + bulk operations via SQLAlchemy session scope.
Every concrete repository inherits this and sets __model__ = <ModelClass>.
"""
from __future__ import annotations
from contextlib import contextmanager
from typing import TypeVar, Generic, Optional, Any
from db.database_manager import DatabaseManager

T = TypeVar("T")


class BaseRepository(Generic[T]):

    __model__: type = None   # set by every concrete subclass

    def __init__(self, db_manager: DatabaseManager) -> None:
        self._db = db_manager

    @contextmanager
    def _session(self):
        with self._db.session_scope() as session:
            yield session

    # ── Write ─────────────────────────────────────────────────────────
    def add(self, entity: T) -> bool:
        try:
            with self._session() as session:
                session.add(entity)
            return True
        except Exception:
            return False

    def add_many(self, records: list[dict]) -> bool:
        """Bulk insert a list of dicts as model instances."""
        try:
            with self._session() as session:
                instances = [self.__model__(**r) for r in records]
                session.bulk_save_objects(instances)
            return True
        except Exception:
            return False

    # ── Read ──────────────────────────────────────────────────────────
    def get_by_id(self, entity_id: Any) -> Optional[T]:
        with self._session() as session:
            return session.get(self.__model__, entity_id)

    def get_all(self) -> list[T]:
        with self._session() as session:
            return session.query(self.__model__).all()

    def get_all_ids(self, id_column: str = "id") -> list:
        """Return a flat list of all PK values — used for cache warming."""
        with self._session() as session:
            col = getattr(self.__model__, id_column)
            return [row[0] for row in session.query(col).all()]

    def get_by_attribute(self, attr_name: str, attr_value: Any) -> list[T]:
        with self._session() as session:
            return session.query(self.__model__).filter(
                getattr(self.__model__, attr_name) == attr_value
            ).all()

    def get_existing_ids(self, id_column: str, ids: set) -> set:
        """Bulk IN query — one round trip. Used by LookUp components."""
        if not ids:
            return set()
        with self._session() as session:
            col    = getattr(self.__model__, id_column)
            result = session.query(col).filter(col.in_(ids)).all()
            return {row[0] for row in result}

    # ── Update / Delete ───────────────────────────────────────────────
    def update(self, entity_id: Any, **kwargs) -> bool:
        try:
            with self._session() as session:
                entity = session.get(self.__model__, entity_id)
                if entity:
                    for k, v in kwargs.items():
                        setattr(entity, k, v)
            return True
        except Exception:
            return False

    def delete_by_id(self, entity_id: Any) -> bool:
        try:
            with self._session() as session:
                entity = session.get(self.__model__, entity_id)
                if entity:
                    session.delete(entity)
            return True
        except Exception:
            return False
