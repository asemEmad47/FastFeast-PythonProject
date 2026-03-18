"""
SCDComponent — DataFlowComponent.

Applies the correct SCD strategy per table_conf['dimension']['scd_type'].

Type 0: insert new PKs only — existing rows are never touched.
Type 1: upsert — UPDATE changed rows, INSERT new rows.
Type 2: expire old version (is_current=False, valid_to=today),
         INSERT new version (is_current=True, valid_from=today, surrogate_key).

Extra columns required on Type 2 target tables:
  surrogate_key, is_current, valid_from, valid_to
"""
from __future__ import annotations
from typing import Optional
from datetime import date
import uuid
import pandas as pd

from etl.components.data_flow_component import DataFlowComponent
from etl.scd.scd_type import SCDType
from registry.data_registry import DataRegistry
from audit.audit import Audit


class SCDComponent(DataFlowComponent):

    def __init__(
        self,
        table_conf: dict,
        scd_conf:   dict,
        registry:   DataRegistry,
        audit:      Audit,
    ) -> None:
        super().__init__(audit=audit)
        self.table_conf     = table_conf
        self.scd_conf       = scd_conf          # {mode, scd_type}
        self.registry       = registry
        self._scd_type      = SCDType(scd_conf.get("scd_type", 1))
        self._primary_key   = table_conf.get("primary_key")
        self._tracked_cols  = table_conf.get("tracked_columns", [])
        self._surrogate_key = table_conf.get("surrogate_key", "surrogate_key")

    # ------------------------------------------------------------------
    def do_task(self, df: Optional[pd.DataFrame]) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        if df is None:
            return False, ["SCDComponent received None df"], None
        try:
            result_df = self.handle_scd(df)
            return True, [], result_df
        except Exception as exc:
            return False, [f"SCDComponent failed: {exc}"], df

    # ------------------------------------------------------------------
    def handle_scd(self, df: pd.DataFrame) -> pd.DataFrame:
        """Dispatch to the correct SCD strategy."""
        if self._scd_type == SCDType.TYPE_0:
            return self._apply_type_0(df)
        elif self._scd_type == SCDType.TYPE_1:
            return self._apply_type_1(df)
        elif self._scd_type == SCDType.TYPE_2:
            return self._apply_type_2(df)

    # ── Type 0 — Freeze ───────────────────────────────────────────────
    def _apply_type_0(self, df: pd.DataFrame) -> pd.DataFrame:
        """Insert only rows with PKs not yet in the DWH."""
        new_df, _ = self.split_new_and_existing_rows(df)
        self._track_scd_metrics(inserted=len(new_df))
        return new_df

    # ── Type 1 — Upsert ───────────────────────────────────────────────
    def _apply_type_1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        New PKs   → INSERT
        Existing PKs with changes → UPDATE (via repo.update())
        TODO: implement update calls against repository.
        """
        new_df, existing_df = self.split_new_and_existing_rows(df)
        changed_df = self.detect_changed_rows(existing_df)
        self._track_scd_metrics(inserted=len(new_df), updated=len(changed_df))
        # Return new rows for insertion; changed rows handled by repo.update()
        return new_df

    # ── Type 2 — Versioned History ─────────────────────────────────────
    def _apply_type_2(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        New PKs                     → INSERT with surrogate key
        Existing PKs with changes   → expire old row + INSERT new version
        Existing PKs without change → skip (no-op)
        """
        new_df, existing_df = self.split_new_and_existing_rows(df)
        changed_df          = self.detect_changed_rows(existing_df)

        # Expire current rows in DWH for changed PKs
        if not changed_df.empty:
            self.expire_current_rows(changed_df)

        # Assign surrogate keys to new + changed rows
        inserts = pd.concat([new_df, changed_df], ignore_index=True)
        inserts = self.generate_surrogate_keys(inserts)
        inserts = self.prepare_type_2_new_rows(inserts)

        skipped = len(existing_df) - len(changed_df)
        self._track_scd_metrics(
            inserted=len(inserts),
            updated=len(changed_df),
            expired=len(changed_df),
            skipped=skipped,
        )
        return inserts

    # ── Helpers ───────────────────────────────────────────────────────
    def split_new_and_existing_rows(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Fetch existing PKs from DWH via registry.
        Returns (new_rows_df, existing_rows_df).
        TODO: wire registry.get_existing_ids() with resolved table name.
        """
        incoming_ids = set(df[self._primary_key].dropna().unique())
        existing_ids: set = set()  # placeholder — implement via registry

        is_existing = df[self._primary_key].isin(existing_ids)
        return df[~is_existing].copy(), df[is_existing].copy()

    def detect_changed_rows(self, existing_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compare tracked_columns against DWH current values.
        Returns rows where at least one tracked column differs.
        TODO: fetch current DWH snapshot and compare.
        """
        if existing_df.empty or not self._tracked_cols:
            return pd.DataFrame(columns=existing_df.columns)
        # Placeholder — return all existing rows as changed
        return existing_df.copy()

    def expire_current_rows(self, changed_df: pd.DataFrame) -> None:
        """
        UPDATE existing DWH rows: set is_current=False, valid_to=today.
        TODO: call repo.update(pk, is_current=False, valid_to=date.today())
        """
        pass

    def generate_surrogate_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Assign a UUID surrogate key to every row in df."""
        df = df.copy()
        df[self._surrogate_key] = [str(uuid.uuid4()) for _ in range(len(df))]
        return df

    def prepare_type_2_new_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Stamp Type 2 metadata columns on new/changed rows."""
        df = df.copy()
        today = date.today()
        df["is_current"]  = True
        df["valid_from"]  = today
        df["valid_to"]    = None
        return df

    def _track_scd_metrics(self, inserted=0, updated=0, expired=0, skipped=0) -> None:
        self.audit.track_metrics("scd", {
            "scd_inserted": inserted,
            "scd_updated":  updated,
            "scd_expired":  expired,
            "scd_skipped":  skipped,
        })
