"""
RowsValidator — Validator Strategy.
Validates each row: data types, nullability, value ranges.
Soft failure: bad rows are separated and returned as errors,
clean df continues. Never returns ok=False.
"""
from typing import Optional
import pandas as pd
from validation.validator import Validator


class RowsValidator(Validator):

    def validate(
        self,
        df:         Optional[pd.DataFrame],
        model,
        table_conf: dict,
    ) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        if df is None:
            return True, [], pd.DataFrame()

        errors: list[str] = []
        required = table_conf.get("required_fields", [])

        # Null check on required fields
        null_mask = pd.Series([False] * len(df), index=df.index)
        for col in required:
            if col in df.columns:
                col_nulls = df[col].isna()
                null_mask = null_mask | col_nulls

        bad_rows   = df[null_mask]
        clean_rows = df[~null_mask]

        if not bad_rows.empty:
            errors.append(f"Null rows rejected: {len(bad_rows)}")
            # TODO: route bad_rows to QuarantineWriter

        # TODO: validate_dataType(), validate_value()

        return True, errors, clean_rows
