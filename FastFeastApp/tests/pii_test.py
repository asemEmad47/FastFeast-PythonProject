import os
import pandas as pd
from audit.audit import Audit
from registry.data_registry import DataRegistry
from registry.conf_file_parser import ConfFileParser
from etl.components.pii_mask import PIIMask

if __name__ == "__main__":
    base_dir = os.path.dirname(__file__)
    pipeline_file = os.path.abspath(os.path.join(base_dir, "..", "conf", "pipeline.yaml"))
    
    parser = ConfFileParser()
    registry = DataRegistry(parser)
    registry.load_config(pipeline_file) 
    
    audit = Audit(mode="batch")
    audit.start_batch(batch_date="2024-01-01")

    # Mocking customers data with PII
    mock_df = pd.DataFrame({
        "customer_id": [72, 438],
        "full_name": ["Hazem Ali", "Salma Shaker"],
        "email": ["hazem.ali66@hotmail.com", "salma.shaker@gmail.com"],
        "phone": ["01265766169", "01571324136"]
    })

    mock_input = {
        "source": "customers", # Matches pii_fields under 'customers' in yaml
        "dataframe": mock_df
    }

    pii_task = PIIMask(audit=audit, registry=registry)
    print("="*60)
    print("RUNNING PII MASK TEST: Hashing email and phone")
    print("="*60)
    
    success, errors, result_dict, _, _ = pii_task.do_task(mock_input)

    if success:
        df = result_dict["dataframe"]
        print("\n✅ PII MASKING VERIFICATION (FIRST ROW)")
        print("="*80)
        
        # Select only the first row
        first_row = df.head(1)
        
        # Define the columns you want to inspect
        cols_to_show = ['customer_id', 'full_name', 'email', 'phone']
        
        # Print using a clean table format
        print(first_row[cols_to_show].to_string(index=False))
        print("="*80)
        
        # Optional: Explicit check for the hash length
        email_hash = first_row['email'].values[0]
        print(f"DEBUG: Email Hash Length: {len(str(email_hash))} chars")
    else:
        print(f"❌ FAILED! Errors: {errors}")