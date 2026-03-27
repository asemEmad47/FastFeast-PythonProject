"""
CustomersRepository — Concrete repository for CustomersDim.
"""
from __future__ import annotations
from repository.base_repository import BaseRepository
from db.database_manager import DatabaseManager


class CustomersRepository(BaseRepository):

    __table__  = "DimCustomers"
    __pk__     = "customer_id"

    def __init__(self, db_manager: DatabaseManager) -> None:
        super().__init__(db_manager)

    # ── CRUD ──────────────────────────────────────────────────────────────

    def get_customer_by_id(self, customer_id: int) -> dict | None:
        """Returns a single customer by PK or None if not found."""
        return self.get_by_id(customer_id)

    def get_all_customers(self) -> list[dict]:
        """Returns all customers regardless of active status."""
        return self.get_all()

    def soft_delete(self, customer_id: int) -> bool:
        """
        Marks a customer as inactive without removing the record.
        Preserves referential integrity with FactTickets.
        CustomersDim does not have is_active — we use a status-like
        approach by zeroing out discount and removing priority support.
        """
        return self.update(
            customer_id,
            priority_support=False,
            discount_pct=0.0
        )

    def upsert_customers(self, records: list[dict]) -> bool:
        """
        Idempotent bulk upsert for CustomersDim.
        Re-processing the same batch file will not duplicate customers.
        """
        return self.upsert_many(records, pk_column="customer_id")

    # ── Custom ────────────────────────────────────────────────────────────

    def get_by_segment(self, segment_id: int) -> list[dict]:
        """Returns all customers belonging to a specific segment."""
        return self.get_by_attribute("segment_id", segment_id)

    def get_priority_customers(self) -> list[dict]:
        """Returns all customers with priority support enabled."""
        return self.get_by_attribute("priority_support", True)

    def get_by_region(self, region_id: int) -> list[dict]:
        """Returns all customers in a specific region."""
        return self.get_by_attribute("region_id", region_id)

    def get_by_gender(self, gender: str) -> list[dict]:
        """Returns all customers of a specific gender."""
        return self.get_by_attribute("gender", gender)