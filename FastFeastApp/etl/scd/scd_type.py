"""
SCDType — Enumeration of supported Slowly Changing Dimension strategies.

Type 0 — Freeze:  never update; insert only for new PKs.
Type 1 — Upsert:  overwrite existing rows; no history.
Type 2 — Version: expire old row, insert new versioned row; full history.
"""
from enum import Enum


class SCDType(Enum):
    TYPE_0 = 0   # Freeze  — immutable reference data (city, priority, reason)
    TYPE_1 = 1   # Upsert  — latest value only (agent, segment, team)
    TYPE_2 = 2   # Version — full history (customer, driver, restaurant)
