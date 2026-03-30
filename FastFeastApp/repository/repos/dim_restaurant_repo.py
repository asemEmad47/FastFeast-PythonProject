"""
RestaurantsRepository — Concrete repository for RestaurantsDim.
"""
from __future__ import annotations
from repository.base_repository import BaseRepository
from db.database_manager import DatabaseManager


class RestaurantsRepository(BaseRepository):

    __table__  = "DimRestaurants"
    __pk__     = "restaurant_id"

    def __init__(self, db_manager: DatabaseManager) -> None:
        super().__init__(db_manager)

    # ── CRUD ──────────────────────────────────────────────────────────────

    def get_restaurant_by_id(self, restaurant_id: int) -> dict | None:
        """Returns a single restaurant by PK or None if not found."""
        return self.get_by_id(restaurant_id)

    def get_all_restaurants(self) -> list[dict]:
        """Returns all restaurants regardless of active status."""
        return self.get_all()

    def soft_delete(self, restaurant_id: int) -> bool:
        """
        Marks a restaurant as inactive without removing the record.
        Preserves referential integrity with FactTickets.
        """
        return self.update(restaurant_id, is_active=False)

    def upsert_restaurants(self, records: list[dict]) -> bool:
        """
        Idempotent bulk upsert for RestaurantsDim.
        Re-processing the same batch file will not duplicate restaurants.
        """
        return self.upsert_many(records, pk_column="restaurant_id")

    # ── Custom ────────────────────────────────────────────────────────────

    def get_active_restaurants(self) -> list[dict]:
        """Returns all currently active restaurants."""
        return self.get_by_attribute("is_active", True)

    def get_by_region(self, region_id: int) -> list[dict]:
        """Returns all restaurants in a specific region."""
        return self.get_by_attribute("region_id", region_id)

    def get_by_category(self, category_id: int) -> list[dict]:
        """Returns all restaurants in a specific category."""
        return self.get_by_attribute("category_id", category_id)

    def get_by_price_tier(self, price_tier: str) -> list[dict]:
        """Returns all restaurants at a specific price tier."""
        return self.get_by_attribute("price_tier", price_tier)