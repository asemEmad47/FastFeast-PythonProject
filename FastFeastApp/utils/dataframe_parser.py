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
import numpy as np


class DataFrameParser:

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    # ── Building Steps ─────────────────────────────────────────────────────

    # Some of those methods are to be refactored/removed
    def normalize_timestamps(self, date_columns: list[str] = None) -> DataFrameParser:
        cols_to_process = date_columns if date_columns is not None else self._df.columns
        for col in cols_to_process:
            if col not in self._df.columns:
                continue
            if pd.api.types.is_datetime64_any_dtype(self._df[col]):
                self._df[col] = self._df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            elif self._df[col].dtype == object:
                try:
                    self._df[col] = pd.to_datetime(self._df[col], format="mixed").dt.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError):
                    pass
        return self

    def fill_nulls(self) -> DataFrameParser:
        """
        Replaces all NaN, pandas NA, and string 'NAN' values with None.
        Snowflake requires None (which translates to NULL) to handle these correctly.
        """
        # 1. Replace actual numpy/pandas NaNs with None
        # .replace({np.nan: None}) is often more reliable than .fillna(None)
        self._df = self._df.replace({np.nan: None})

        # 2. Handle the string "NAN" (if it exists as a string)
        # Note: Replacing with None is safer than "" for numeric columns
        self._df = self._df.replace(to_replace=r'(?i)^nan$', value=None, regex=True)
        
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

    def to_df(self) -> pd.DataFrame:
        """
        Terminal step — converts the transformed DataFrame to a
        repository-ready list of dicts.
        Always call this last.
        """
        return self._df
    