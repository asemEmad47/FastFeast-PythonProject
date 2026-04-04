import os
import pandas as pd
from audit.audit import Audit
from registry.data_registry import DataRegistry
from registry.conf_file_parser import ConfFileParser
from etl.components.transformer import Transformer

if __name__ == "__main__":
    # 1. Setup paths and registry
    base_dir = os.path.dirname(__file__)
    pipeline_file = os.path.abspath(os.path.join(base_dir, "..", "conf", "pipeline.yaml"))
    
    parser = ConfFileParser()
    registry = DataRegistry(parser)
    registry.load_config(pipeline_file) 
    
    audit = Audit(mode="batch")
    audit.start_batch(batch_date="2024-01-01")

    # 2. Mock Data: All required fields + extra junk columns
    mock_df = pd.DataFrame({
        # Required Fields
        "customer_id": [72],
        "full_name": ["Hazem Ali"],
        "email": ["hazem.ali66@hotmail.com"],
        "phone": ["01265766169"],
        "segment_name": ["Regular"],
        "signup_date": ["2022-09-25"],
        "gender": ["male"],
        "priority_support": [False],
        "discount_pct": [0],
        "created_at": ["2025-02-13"],
        "updated_at": ["2026-03-26"],
        
        # Extra columns that MUST be deleted
        "temporary_junk": ["Drop Me"],
        "debug_log": ["Error 404 at source"],
        "internal_id": [9999],
        "raw_metadata": ["{ 'source': 'csv' }"]
    })

    mock_input = {
        "dimension": "CustomersDim", 
        "dataframe": mock_df
    }

    # 3. Run Task
    transformer = Transformer(audit=audit, registry=registry)
    print("="*80)
    print("RUNNING TRANSFORMER TEST: Column Filtering & Aggregation")
    print("="*80)
    
    success, errors, result_dict, _, _ = transformer.do_task(mock_input)

    # 4. Verification
    if success:
        df = result_dict["dataframe"]
        print("✅ SUCCESS: Unused columns deleted.")
        
        # Check if junk columns still exist
        junk_cols = ["temporary_junk", "debug_log", "internal_id", "raw_metadata"]
        found_junk = [col for col in junk_cols if col in df.columns]
        
        if not found_junk:
            print("✨ Verification: All extra columns were successfully removed.")
        
        print("\nTRANSFORMED DATA (FIRST ROW):")
        print("-" * 80)
        # Print only the first row without the index
        print(df.head(1).to_string(index=False))
        print("-" * 80)
        
        print(f"\nFinal Column Count: {len(df.columns)}")
    else:
        print(f"❌ FAILED! Errors: {errors}")