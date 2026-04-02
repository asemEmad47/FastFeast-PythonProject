"""
FactTicketsRepository — Concrete repository for FactTickets.
"""
from __future__ import annotations
from repository.base_repository import BaseRepository
from db.database_manager import DatabaseManager


class FactTicketsRepository(BaseRepository):

    def __init__(self, db_manager, registry, audit=None):
        super().__init__(db_manager, registry, "FactTickets", audit=audit)

    # ── CRUD ──────────────────────────────────────────────────────────────

    def get_ticket_by_id(self, ticket_id: str) -> dict | None:
        """Returns a single ticket by PK or None if not found."""
        return self.get_by_id(ticket_id)

    def get_all_tickets(self) -> list[dict]:
        """
        Returns all tickets regardless of any filter.
        Use with caution — fact tables grow large.
        """
        return self.get_all()

    def upsert_tickets(self, records: list[dict]) -> bool:
        """
        Idempotent bulk upsert for FactTickets.
        Re-processing the same micro-batch file will not duplicate tickets.
        ticket_id and order_id are degenerate dimensions —
        they live in the fact table with no corresponding dim table.
        """
        return self.upsert_many(records)

    # ── Custom ────────────────────────────────────────────────────────────

    def get_by_customer(self, customer_id: int) -> list[dict]:
        """Returns all tickets raised by a specific customer."""
        return self.get_by_attribute("customer_id", customer_id)

    def get_by_agent(self, agent_id: int) -> list[dict]:
        """Returns all tickets handled by a specific agent."""
        return self.get_by_attribute("agent_id", agent_id)

    def get_by_driver(self, driver_id: int) -> list[dict]:
        """Returns all tickets associated with a specific driver."""
        return self.get_by_attribute("driver_id", driver_id)

    def get_by_restaurant(self, restaurant_id: int) -> list[dict]:
        """Returns all tickets associated with a specific restaurant."""
        return self.get_by_attribute("restaurant_id", restaurant_id)

    def get_breached_sla_tickets(self) -> list[dict]:
        """
        Returns all tickets where either SLA was breached.
        Used for SLA monitoring analytics.
        """
        with self._db.cursor_scope() as cursor:
            cursor.execute(
                f"""
                SELECT * FROM {self._full_table_name()}
                WHERE sla_first_response_breached = TRUE
                OR sla_resolution_breached = TRUE
                """
            )
            return cursor.fetchall()

    def get_by_date_range(self, start_date_key: int, end_date_key: int) -> list[dict]:
        """
        Returns all tickets created within a date key range.
        date_key format: YYYYMMDD as integer e.g. 20260220
        """
        with self._db.cursor_scope() as cursor:
            cursor.execute(
                f"""
                SELECT * FROM {self._full_table_name()}
                WHERE date_key_created BETWEEN %s AND %s
                """,
                (start_date_key, end_date_key)
            )
            return cursor.fetchall()