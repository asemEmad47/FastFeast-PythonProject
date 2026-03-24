from abc import ABC, abstractmethod
from typing import Optional
import datetime
import decimal
import pandas as pd
import numpy as np


class Validator(ABC):

    @abstractmethod
    def validate(self, df, model, table_conf: dict) -> tuple[bool,  list[dict[str, str]], pd.DataFrame]:
        ...

    def validate_value(self, value: any, expected_type: type) -> bool:

        if expected_type is int:
            try:
                int(value)    
                return True
            except (ValueError, TypeError):
                return False

        if expected_type is float or expected_type is decimal.Decimal:
            try:
                float(value)   
                return True
            except (ValueError, TypeError):
                return False

        if expected_type is str:
            return isinstance(value, str)

        if expected_type is bool:
            return isinstance(value, (bool, np.bool_))

        if expected_type is datetime.date:
            if isinstance(value, (datetime.date, datetime.datetime)):
                return True
            try:
                pd.to_datetime(value) 
                return True
            except (ValueError, TypeError):
                return False

        if expected_type is datetime.datetime:
            if isinstance(value, datetime.datetime):
                return True
            try:
                pd.to_datetime(value)
                return True
            except (ValueError, TypeError):
                return False

        return isinstance(value, expected_type)