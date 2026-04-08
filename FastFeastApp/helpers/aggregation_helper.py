from __future__ import annotations
import pandas as pd

class AggregationHelper:

    @staticmethod
    def apply(df: pd.DataFrame, aggregated_columns: list[dict]) -> tuple[bool, list[str], pd.DataFrame]:

        errors = []

        for action in aggregated_columns:
            name = action.get("name")
            action_type = action.get("type")
            params = action.get("params")

            try:
                if action_type == "date_diff":
                    df = AggregationHelper._date_diff(df, name, params)

                elif action_type == "comparison":
                    df = AggregationHelper._comparison(df, name, params)

                elif action_type == "arithmetic":
                    df = AggregationHelper._arithmetic(df, name, params)
                    
                elif action_type == "date_to_key":
                    df = AggregationHelper._date_to_key(df, name, params)

                else:
                    errors.append(f"Unknown action type '{action_type}' for column '{name}'")
                    return False, errors, df

            except Exception as e:
                errors.append(f"Failed to compute '{name}': {str(e)}")
                return False, errors, df

        return True, errors, df

    @staticmethod
    def _date_diff(df: pd.DataFrame, name: str, params: dict) -> pd.DataFrame:
        start = params["start"]
        end = params["end"]
        unit = params.get("unit", "m")

        start_ts = pd.to_datetime(df[start], errors='coerce')
        end_ts = pd.to_datetime(df[end], errors='coerce')
        
        diff = end_ts - start_ts

        if unit == "m":
            df[name] = (diff.dt.total_seconds() / 60).round().fillna(0).astype(int)
        elif unit == "s":
            df[name] = diff.dt.total_seconds().round().fillna(0).astype(int)
        elif unit == "h":
            df[name] = (diff.dt.total_seconds() / 3600).round().fillna(0).astype(int)
        elif unit == "d":
            df[name] = diff.dt.days.fillna(0).astype(int)

        return df

    @staticmethod
    def _comparison(df: pd.DataFrame, name: str, params: dict) -> pd.DataFrame:
        left = params["left"]
        op = params["op"]
        right = params["right"]

        left_series = df[left]
        right_series = df[right]

        try:
            left_series = pd.to_datetime(left_series)
            right_series = pd.to_datetime(right_series)
        except Exception:
            try:
                left_series = pd.to_numeric(left_series)
                right_series = pd.to_numeric(right_series)
            except Exception:
                pass  

        if op == "<":
            df[name] = left_series < right_series
        elif op == ">":
            df[name] = left_series > right_series
        elif op == "<=":
            df[name] = left_series <= right_series
        elif op == ">=":
            df[name] = left_series >= right_series
        elif op == "==":
            df[name] = left_series == right_series
        elif op == "!=":
            df[name] = left_series != right_series

        return df

    @staticmethod
    def _arithmetic(df: pd.DataFrame, name: str, params: list[dict]) -> pd.DataFrame:
        result = None

        for step in params:
            column = step.get("column")
            op = step.get("op", "+")
            factor = step.get("factor", 1)
            right_column = step.get("right_column")

            left_value = pd.to_numeric(df[column], errors='coerce').fillna(0) * factor

            if result is None:
                result = left_value
                continue

            if op == "+":
                result = result + left_value
            elif op == "-":
                result = result - left_value
            elif op == "*":
                if right_column:
                    right_val = pd.to_numeric(df[right_column], errors='coerce').fillna(0) * factor
                else:
                    right_val = left_value
                result = result * right_val
            elif op == "/":
                if right_column:
                    right_val = pd.to_numeric(df[right_column], errors='coerce').fillna(0) * factor
                else:
                    right_val = left_value
                result = result / right_val.replace(0, float("nan"))

        df[name] = result
        return df

    @staticmethod
    def _date_to_key(df: pd.DataFrame, name: str, params: dict) -> pd.DataFrame:
        column = params["column"]
        df[name] = (
            pd.to_datetime(df[column], errors="coerce")
            .dt.strftime("%Y%m%d")
            .where(df[column].notna(), other=None)
            .astype("Int64")  
        )
        return df