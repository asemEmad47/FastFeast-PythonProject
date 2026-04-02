"""
AgentsRepository — Concrete repository for AgentsDim.
"""
from __future__ import annotations
from repository.base_repository import BaseRepository
from db.database_manager import DatabaseManager


class AgentsRepository(BaseRepository):

    def __init__(self, db_manager: DatabaseManager, registry, audit=None) -> None:
        super().__init__(db_manager, registry, "AgentsDim", audit=audit)

    # ── CRUD ──────────────────────────────────────────────────────────────

    def get_agent_by_id(self, agent_id: int) -> dict | None:
        """Returns a single agent by PK or None if not found."""
        return self.get_by_id(agent_id)

    def get_all_agents(self) -> list[dict]:
        """Returns all agents regardless of active status."""
        return self.get_all()

    def soft_delete(self, agent_id: int) -> bool:
        """
        Marks an agent as inactive without removing the record.
        Preserves referential integrity with FactTickets.
        """
        return self.update(agent_id, is_active=False)

    def upsert_agents(self, records: list[dict]) -> bool:
        """
        Idempotent bulk upsert for AgentsDim.
        Re-processing the same batch file will not duplicate agents.
        """
        return self.upsert_many(records)

    # ── Custom ────────────────────────────────────────────────────────────

    def get_active_agents(self) -> list[dict]:
        """Returns all currently active agents."""
        return self.get_by_attribute("is_active", True)

    def get_by_team(self, team_id: int) -> list[dict]:
        """Returns all agents belonging to a specific team."""
        return self.get_by_attribute("team_id", team_id)

    def get_by_skill_level(self, skill_level: str) -> list[dict]:
        """Returns all agents at a specific skill level."""
        return self.get_by_attribute("skill_level", skill_level)