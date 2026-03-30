"""
utils/dataframe_parser.py

Builder Pattern — incremental DataFrame transformation pipeline.
Each method transforms the internal DataFrame and returns self.
to_records() is the terminal step that produces repository-ready list of dicts.

Usage:
    records = (
        DataFrameParser(df)
            .normalize_timestamps()
            .fill_nulls()
            .to_records()
    )
"""
from __future__ import annotations
import pandas as pd


class DataFrameParser:

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df.copy()

    # ── Building Steps ─────────────────────────────────────────────────────

    # Some of those methods are to be refactored/removed
    def normalize_timestamps(self) -> DataFrameParser:
        """
        Converts all pandas Timestamp, datetime, and date columns to ISO strings.
        Snowflake's pyformat binding does not support these types natively.
        """
        for col in self._df.columns:
            if pd.api.types.is_datetime64_any_dtype(self._df[col]):
                self._df[col] = self._df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
        return self

    def fill_nulls(self) -> DataFrameParser:
        """
        Replaces all NaN and pandas NA values with None.
        Snowflake does not understand float('nan') as a null value.
        """
        self._df = self._df.where(pd.notnull(self._df), other=None)
        return self

    def rename_columns(self, mapping: dict[str, str]) -> DataFrameParser:
        """
        Renames columns according to a mapping dict.
        Used when source file column names don't match the warehouse schema.

        Example:
            .rename_columns({"cust_id": "customer_id", "fname": "full_name"})
        """
        self._df = self._df.rename(columns=mapping)
        return self

    def drop_columns(self, columns: list[str]) -> DataFrameParser:
        """
        Drops columns that should not be passed to the repository.
        Used to strip source-only fields before loading into the warehouse.

        Example:
            .drop_columns(["internal_flag", "raw_hash"])
        """
        existing = [c for c in columns if c in self._df.columns]
        self._df  = self._df.drop(columns=existing)
        return self

    def mask_pii(self, columns: list[str]) -> DataFrameParser:
        """
        Masks PII columns by replacing their values with '***'.
        Used before loading into the analytics schema where PII must not appear.

        Example:
            .mask_pii(["email", "phone", "national_id"])
        """
        for col in columns:
            if col in self._df.columns:
                self._df[col] = "***"
        return self

    def cast_column(self, column: str, dtype) -> DataFrameParser:
        """
        Explicitly casts a single column to a given Python/pandas type.
        Used when pandas infers the wrong type on read.

        Example:
            .cast_column("customer_id", int)
            .cast_column("is_active", bool)
        """
        if column in self._df.columns:
            self._df[column] = self._df[column].astype(dtype)
        return self

    def filter_rows(self, column: str, value) -> DataFrameParser:
        """
        Keeps only rows where column equals value.
        Used to pre-filter source data before loading.

        Example:
            .filter_rows("is_active", True)
        """
        if column in self._df.columns:
            self._df = self._df[self._df[column] == value]
        return self

    # ── Terminal Step ──────────────────────────────────────────────────────

    def to_records(self) -> list[dict]:
        """
        Terminal step — converts the transformed DataFrame to a
        repository-ready list of dicts.
        Always call this last.
        """
        return self._df.to_dict(orient="records")