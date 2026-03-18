"""
Validator — Abstract Strategy interface.
ValidatorContext dispatches to SchemaValidator or RowsValidator via this contract.
"""
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd


class Validator(ABC):

    @abstractmethod
    def validate(
        self,
        df:         Optional[pd.DataFrame],
        model,
        table_conf: dict,
    ) -> tuple[bool, list[str], Optional[pd.DataFrame]]:
        ...
