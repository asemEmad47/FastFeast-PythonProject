from typing import Optional
import pandas as pd
from validation.validator import Validator

class ValidatorContext:

    def __init__(self) -> None:
        self._validator: Validator = None

    def set_validator(self, validator: Validator) -> None:
        self._validator = validator

    def get_validator(self) -> Validator:
        return self._validator

    def validate(self, df, model, required: list[str]) -> tuple[bool,  list[dict[str, str]], pd.DataFrame , dict[str, int]]:
        if self._validator is None:
            raise RuntimeError("ValidatorContext: no validator set")
        return self._validator.validate(df, model, required)
