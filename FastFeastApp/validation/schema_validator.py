import dataclasses
import pandas as pd
from typing import Any
from validation.validator import Validator

class SchemaValidator(Validator):

    def validate(self, df: pd.DataFrame, model: Any, table_conf: dict) -> tuple[bool, list[dict[str, str]], pd.DataFrame]:
        if df is None:
            return False, None, None

        errors = []

        required = table_conf.get("required_fields", [])
        missing = [col for col in required if col not in df.columns]
        if missing:
            errors.append({
                    "model": model.__name__,
                    "reason": f"has missing columns: {missing}"
                })
            return False, errors, df

        for field in dataclasses.fields(model):
            col_name = field.name.strip().lower()
            expected_type = field.type

            if col_name not in df.columns:
                continue

            unique_values = df[col_name].dropna().unique()
            if len(unique_values) == 0:
                continue

            matched = any(self.validate_value(val, expected_type) for val in unique_values)

            if not matched:
                errors.append({
                    "model": model.__name__,
                    "reason": f"Column '{col_name}' type mismatch. Expected {expected_type.__name__}, got {df[col_name].dtype}"
                })

        return (False, errors, df) if errors else (True, [], df)