import dataclasses
from typing import Any, Optional
import pandas as pd
from validation.validator import Validator


class RowsValidator(Validator):

    def validate(self, df, model, required) -> tuple[bool, list[dict[str, str]], pd.DataFrame , dict[str, int]]:
        if df is None:
            return True, [], pd.DataFrame(), {}
        
        stats = {
            "total_rows": len(df),
            "passed_count": len(df),
            "null_count": 0,
            "quarantined_count": 0
        }

        errors = []
        
        bad_rows = df[df[required].isnull().any(axis=1)]
        clean_rows = df.drop(bad_rows.index)

        if not bad_rows.empty:
            for _, row in bad_rows.iterrows(): 
                errors.append({
                    "row" : row.to_string(),
                    "reason": f"Missing required fields: {[col for col in required if pd.isnull(row[col])]}"
                })
                stats["null_count"] += 1
                stats["passed_count"] -= 1
                
        for field in dataclasses.fields(model):
            col_name      = field.name.strip().lower()
            expected_type = field.type

            if col_name not in clean_rows.columns:
                continue
            
            curr_col = clean_rows[col_name]
            for idx, val in curr_col.dropna().items():
                if not self.validate_value(val, expected_type):
                    errors.append({
                        "row": clean_rows.loc[idx].to_string(),
                        "reason": f"Column '{col_name}' type mismatch. Expected {expected_type.__name__}, got {type(val).__name__}"
                    })
                    stats["quarantined_count"] += 1
                    stats["passed_count"] -= 1
                    clean_rows = clean_rows.drop(idx)

        return True, errors, clean_rows , stats