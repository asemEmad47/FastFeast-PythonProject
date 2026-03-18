"""
SchemaValidator — Validator Strategy.
Checks required columns exist and column types match the model.
Hard failure: entire file is rejected if schema is wrong.
"""
from typing import Optional
import pandas as pd
from validation.validator import Validator


class SchemaValidator(Validator):

    def validate(
        self,
        df:         Optional[pd.DataFrame],
        model,
        table_conf: dict,
    ) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        if df is None:
            return False, ["SchemaValidator: received None df"], None

        errors = []

        # Check required columns exist
        required = table_conf.get("required_fields", [])
        missing  = [col for col in required if col not in df.columns]
        if missing:
            errors.append(f"Missing required columns: {missing}")

        # TODO: check column types against model field definitions

        if errors:
            return False, errors, df

        return True, [], df
