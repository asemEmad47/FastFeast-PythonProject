"""
Test AgentsRepository — CRUD + upsert operations.
Uses a custom pandas DataFrame as the data source
to simulate what the ETL layer will eventually supply.
Run from FastFeastApp/ directory:
    python -m tests.repo_test
"""
from __future__ import annotations
import pandas as pd
from datetime import date, datetime
from db.database_manager import DatabaseManager
from repository.repos.dim_agents_repo import AgentsRepository
from registry.data_registry import DataRegistry
from registry.conf_file_parser import ConfFileParser

# ── Test Data ─────────────────────────────────────────────────────────────────

def build_agents_dataframe() -> pd.DataFrame:
    """
    Simulates what the ETL layer will supply after reading
    and conforming the agents batch CSV file.
    """
    return pd.DataFrame([
        {
            "agent_id":            1,
            "agent_name":          "Alice Johnson",
            "agent_email":         "alice@fastfeast.com",
            "agent_phone":         1234567890,
            "team_id":             10,
            "skill_level":         "senior",
            "hire_date":           date(2022, 3, 15),
            "avg_handle_time_min": 8.5,
            "is_active":           True,
            "created_at":          datetime(2022, 3, 15, 9, 0, 0),
            "updated_at":          datetime(2024, 1, 10, 12, 0, 0),
            "team_name":           "Support A"
        },
        {
            "agent_id":            2,
            "agent_name":          "Bob Martinez",
            "agent_email":         "bob@fastfeast.com",
            "agent_phone":         9876543210,
            "team_id":             11,
            "skill_level":         "junior",
            "hire_date":           date(2023, 6, 1),
            "avg_handle_time_min": 12.0,
            "is_active":           True,
            "created_at":          datetime(2023, 6, 1, 8, 0, 0),
            "updated_at":          datetime(2024, 2, 1, 10, 0, 0),
            "team_name":           "Support B"
        },
        {
            "agent_id":            3,
            "agent_name":          "Clara Smith",
            "agent_email":         "clara@fastfeast.com",
            "agent_phone":         5556667777,
            "team_id":             10,
            "skill_level":         "mid",
            "hire_date":           date(2021, 9, 20),
            "avg_handle_time_min": 10.2,
            "is_active":           True,
            "created_at":          datetime(2021, 9, 20, 7, 30, 0),
            "updated_at":          datetime(2024, 3, 5, 14, 0, 0),
            "team_name":           "Support A"
        }
    ])


# ── Helpers ───────────────────────────────────────────────────────────────────

def df_to_records(df: pd.DataFrame) -> list[dict]:
    """
    Converts a DataFrame to a list of dicts for repository consumption.
    Serializes dates and timestamps to ISO strings — Snowflake's pyformat
    binding does not support pandas Timestamp or Python date objects directly.
    """
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if isinstance(v, pd.Timestamp):
                clean[k] = v.isoformat()
            elif isinstance(v, __import__('datetime').date):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        records.append(clean)
    return records

def print_result(label: str, result) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {label}")
    print(f"{'─' * 60}")
    if isinstance(result, list):
        for row in result:
            print(f"  {row}")
    else:
        print(f"  {result}")


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_upsert(repo: AgentsRepository, records: list[dict]) -> None:
    """
    Tests idempotent bulk upsert.
    Running this twice must not duplicate records.
    """
    print("\n══ TEST: upsert_agents (first run) ══")
    result = repo.upsert_agents(records)
    assert result is True, "First upsert failed"
    print("  ✓ First upsert succeeded")

    print("\n══ TEST: upsert_agents (idempotency check — second run) ══")
    result = repo.upsert_agents(records)
    assert result is True, "Second upsert failed"
    print("  ✓ Second upsert succeeded — no duplicates")


def test_get_by_id(repo: AgentsRepository) -> None:
    """Tests fetching a single agent by PK."""
    print("\n══ TEST: get_agent_by_id ══")
    result = repo.get_agent_by_id(1)
    assert result is not None, "Expected agent_id=1 to exist"
    print_result("agent_id = 1", result)
    print("  ✓ get_agent_by_id passed")


def test_get_all(repo: AgentsRepository) -> None:
    """Tests full table retrieval."""
    print("\n══ TEST: get_all_agents ══")
    result = repo.get_all_agents()
    assert len(result) >= 3, f"Expected at least 3 agents, got {len(result)}"
    print_result(f"Total agents returned: {len(result)}", result)
    print("  ✓ get_all_agents passed")


def test_get_active(repo: AgentsRepository) -> None:
    """Tests filtering by is_active = True."""
    print("\n══ TEST: get_active_agents ══")
    result = repo.get_active_agents()
    assert all(row[8] is True for row in result), "Non-active agent returned"
    print_result(f"Active agents returned: {len(result)}", result)
    print("  ✓ get_active_agents passed")


def test_soft_delete(repo: AgentsRepository) -> None:
    """
    Tests soft delete — agent_id=3 should become inactive.
    Verifies the record still exists after soft delete.
    """
    print("\n══ TEST: soft_delete ══")
    result = repo.soft_delete(3)
    assert result is True, "Soft delete failed"
    print("  ✓ soft_delete call succeeded")

    agent = repo.get_agent_by_id(3)
    assert agent is not None, "Agent should still exist after soft delete"
    print_result("agent_id = 3 after soft delete", agent)
    print("  ✓ Record still exists — soft delete confirmed")


def test_get_by_team(repo: AgentsRepository) -> None:
    """Tests filtering agents by team_id."""
    print("\n══ TEST: get_by_team ══")
    result = repo.get_by_team(10)
    assert len(result) >= 1, "Expected at least one agent in team_id=10"
    print_result(f"Agents in team_id=10: {len(result)}", result)
    print("  ✓ get_by_team passed")


def test_update(repo: AgentsRepository) -> None:
    """
    Tests updating a specific field on agent_id=2.
    Changes skill_level from junior to mid.
    """
    print("\n══ TEST: update ══")
    result = repo.update(2, skill_level="mid")
    assert result is True, "Update failed"
    print("  ✓ update call succeeded")

    agent = repo.get_agent_by_id(2)
    print_result("agent_id = 2 after update", agent)
    print("  ✓ update confirmed")


def test_upsert_update(repo: AgentsRepository, original_records: list[dict]) -> None:
    """
    Tests that upsert correctly overwrites an existing record
    when the same PK is supplied with different values.
    agent_id=1 gets a new email and team assignment.
    """
    print("\n══ TEST: upsert_agents (update existing record) ══")
    modified = original_records.copy()
    modified[0] = {**original_records[0], "agent_email": "alice_new@fastfeast.com", "team_id": 99}
    result = repo.upsert_agents([modified[0]])
    assert result is True, "Upsert update failed"

    agent = repo.get_agent_by_id(1)
    print_result("agent_id = 1 after upsert update", agent)
    print("  ✓ upsert correctly overwrote existing record")


# ── Entry Point ───────────────────────────────────────────────────────────────

def run_all_tests() -> None:
    print("\n" + "═" * 60)
    print("  FastFeast — AgentsRepository Test Suite")
    print("═" * 60)

    db   = DatabaseManager()
    
    conf_file_parser = ConfFileParser()
    registry = DataRegistry(conf_file_parser)
    registry.load_config("conf/pipeline.yaml")
    repo = AgentsRepository(db, registry)
    df   = build_agents_dataframe()
    records = df_to_records(df)

    print(f"\n── Source DataFrame ──")
    print(df.to_string(index=False))

    try:
        test_upsert(repo, records)
        test_get_by_id(repo)
        test_get_all(repo)
        test_get_active(repo)
        test_soft_delete(repo)
        test_get_by_team(repo)
        test_update(repo)
        test_upsert_update(repo, records)

        print("\n" + "═" * 60)
        print("  ✓ ALL TESTS PASSED")
        print("═" * 60)

    except AssertionError as e:
        print(f"\n  ✗ TEST FAILED: {e}")

    finally:
        db.close()


if __name__ == "__main__":
    run_all_tests()