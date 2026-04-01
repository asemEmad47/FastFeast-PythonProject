"""
DriversRepository — Concrete repository for DriversDim.
"""
from __future__ import annotations
from repository.base_repository import BaseRepository
from db.database_manager import DatabaseManager


class DriversRepository(BaseRepository):

    def __init__(self, db_manager, registry, audit=None):
        super().__init__(db_manager, registry, "DriversDim", audit=audit)
    # ── CRUD ──────────────────────────────────────────────────────────────

    def get_driver_by_id(self, driver_id: int) -> dict | None:
        """Returns a single driver by PK or None if not found."""
        return self.get_by_id(driver_id)

    def get_all_drivers(self) -> list[dict]:
        """Returns all drivers regardless of active status."""
        return self.get_all()

    def soft_delete(self, driver_id: int) -> bool:
        """
        Marks a driver as inactive without removing the record.
        Preserves referential integrity with FactTickets.
        """
        return self.update(driver_id, is_active=False)

    def upsert_drivers(self, records: list[dict]) -> bool:
        """
        Idempotent bulk upsert for DriversDim.
        Re-processing the same batch file will not duplicate drivers.
        """
        return self.upsert_many(records, pk_column="driver_id")

    # ── Custom ────────────────────────────────────────────────────────────

    def get_active_drivers(self) -> list[dict]:
        """Returns all currently active drivers."""
        return self.get_by_attribute("is_active", True)

    def get_by_region(self, region_id: int) -> list[dict]:
        """Returns all drivers operating in a specific region."""
        return self.get_by_attribute("region_id", region_id)

    def get_by_shift(self, shift: str) -> list[dict]:
        """Returns all drivers on a specific shift."""
        return self.get_by_attribute("shift", shift)

    def get_by_vehicle_type(self, vehicle_type: str) -> list[dict]:
        """Returns all drivers with a specific vehicle type."""
        return self.get_by_attribute("vehicle_type", vehicle_type)