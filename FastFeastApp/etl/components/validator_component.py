"""
ValidatorComponent — DataFlowComponent.

Stage 1: SchemaValidator  → hard fail (whole file rejected, chain stops).
Stage 2: RowsValidator    → soft fail (bad rows go to bad_rows, clean df continues).

Updates metrics: null_records, failed_records, passed_records.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from validation.validator import Validator
from validation.validator_context import ValidatorContext
from audit.audit import Audit
from registry.data_registry import DataRegistry


class ValidatorComponent(DataFlowComponent):

    def __init__(self, audit: Audit, registry: DataRegistry = None):
        super().__init__(audit=audit, registry=registry)
        self._validations = {}
        self._validator = ValidatorContext()

    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        
        errors = []
        dimension = data_frame_dict.get("dimension")
        source = data_frame_dict.get("source")
        
        print(source)
        print(dimension)
        if not dimension:
            errors.append("Missing 'dimension' in data_frame_dict")
            return False, errors, data_frame_dict, {}, None
        
        df = data_frame_dict.get("dataframe")
        model = self.registry.get_model(source)
        required = self.registry.get_file_required_fields(source)
        
        print("model = ", model)
        print("required = ", required)
        
        if model is None or required is None:
            errors.append(f"Model or required fields not found for dimension '{dimension}'")
            return False, errors, data_frame_dict, {}, None

        # Stage 1: Schema Validation
        self._validator.set_validator(self._validations.get("schema"))
        success, stage_errors, df, stats = self._validator.validate(df, model, required)
        
        if not success:
            return False, stage_errors, data_frame_dict, stats, None
        
        # Stage 2: Rows Validation
        self._validator.set_validator(self._validations.get("rows"))
        success, stage_errors, clean_df, stats = self._validator.validate(df, model, required)
        self.audit.track_metrics(stats) 
        
        if not success:
            return False, stage_errors, data_frame_dict, stats, None
        
        data_frame_dict["dataframe"] = clean_df
        return True, stage_errors, data_frame_dict, stats, None
    
    def register_validations(self, validations: dict[str, Validator]) -> None:
        for name, validator in validations.items():
            self._validations[name] = validator
