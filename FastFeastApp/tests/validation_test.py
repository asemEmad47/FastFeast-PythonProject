import os
import pandas as pd
import json
import dataclasses
import datetime
from typing import Optional

# Component Imports
from audit.audit import Audit
from registry.data_registry import DataRegistry
from registry.conf_file_parser import ConfFileParser
from etl.components.validator_component import ValidatorComponent
from validation.schema_validator import SchemaValidator
from validation.rows_validator import RowsValidator



if __name__ == "__main__":
    # --- Infrastructure Setup ---
    audit = Audit(mode="batch")
    audit.start_batch(batch_date="2026-04-02")
    
    # We use a dummy registry but override the methods to return our local Customer model
    parser = ConfFileParser()
    registry = DataRegistry(parser)
    
    base_dir = os.path.dirname(__file__)
    pipeline_file = os.path.abspath(os.path.join(base_dir, "..", "conf", "pipeline.yaml"))
    
    parser = ConfFileParser()
    registry = DataRegistry(parser)
    registry.load_config(pipeline_file) 
    # --- 2. Create the Corrupted Data ---
    corrupted_df = pd.DataFrame({
        "customer_id":   [None, "2", "corrupted", "4", "5"],    
        "full_name":     ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email":         ["a@a.com", "b@b.com", "c@c.com", "d@d.com", "e@e.com"],
        "phone":         ["01012345678", "01098765432", 123456, "01011112222", "01033334444"], 
        "region_id":     [1.1, 2.2, 3.3, 4.4, 5.5],
        "segment_id":    [1, 2, 3, "bad_segment", 5],         
        "signup_date":   pd.to_datetime(["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"]),
        "gender":        ["Male", "Female", "Male", 99, "Female"], 
        "created_at":    pd.to_datetime(["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"]),
        "updated_at":    pd.to_datetime(["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"]),
        "surrogate_key": ["uuid-1", "uuid-2", "uuid-3", "uuid-4", "uuid-5"],
        "is_current":    [True, False, True, "yes", False], 
        "valid_from":    pd.to_datetime(["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"]),
        "valid_to":      pd.to_datetime([None, None, "2023-01-01", None, "2023-05-01"]),
    })

    mock_input = {
        "dimension": "CustomersDim",
        "source": "customers",
        "dataframe": corrupted_df
    }

    # --- 3. Initialize and Run Component ---
    validator_comp = ValidatorComponent(audit=audit, registry=registry)
    validator_comp.register_validations({
        "schema": SchemaValidator(),
        "rows": RowsValidator()
    })

    print("=" * 60)
    print("RUNNING COMPONENT VALIDATION ON CORRUPTED DATA")
    print("=" * 60)

    success, errors, result_dict, stats, _ = validator_comp.do_task(mock_input)

    # --- 4. Verification Output ---
    if success:
        clean_df = result_dict["dataframe"]
        print("✅ Component Task Finished Successfully (Rows Validated)")
        print("-" * 30)
        print("PROCESSING STATISTICS:")
        print(json.dumps(stats, indent=4))
        print("-" * 30)

        if errors:
            print(f"Captured {len(errors)} Row-Level Warnings/Errors:")
            for e in errors:
                # Truncate row display for cleaner output
                row_preview = e.get('row', 'N/A').replace('\n', ' ')[:60]
                print(f"Row    : {row_preview}...")
                print(f"Reason : {e.get('reason')}")
                print("-" * 40)

        print(f"\nFinal Clean Row Count: {len(clean_df)}")
        if not clean_df.empty:
            print("\nFirst row of clean data:")
            print(clean_df.head(1).to_string(index=False))
        print("stats = ", stats)
    else:
        print("\n❌ SCHEMA VALIDATION FAILED (Chain Stopped)")
        for err in errors:
            # Check if the error is a dict or just a raw string
            if isinstance(err, dict):
                print(f"Reason: {err.get('reason', 'Unknown error')}")
            else:
                print(f"Reason: {err}")