import os

import pandas as pd
from audit.audit import Audit
from registry.data_registry import DataRegistry
from registry.conf_file_parser import ConfFileParser
from etl.components.join import Join

if __name__ == "__main__":
    

    base_dir = os.path.dirname(__file__)

    pipeline_file = os.path.abspath(os.path.join(base_dir, "..", "conf", "pipeline.yaml"))
    
    parser = ConfFileParser()
    registry = DataRegistry(parser)
    
    registry.load_config(pipeline_file) 
    
    audit = Audit(mode="batch")
    audit.start_batch(batch_date="2024-01-01")

    # 2. Mock DataFrames with your exact file columns
    # Customers Mock (based on customers.csv structure)
    mock_customers_df = pd.DataFrame({
        "customer_id": [72, 438, 391],
        "full_name": ["Hazem Ali", "Salma Shaker", "Fadi Mohamed"],
        "email": ["hazem@test.com", "salma@test.com", "fadi@test.com"],
        "phone": ["01265766169", "01571324136", "01150744867"],
        "region_id": [16.0, 2.0, 1.0],
        "segment_id": [1, 1, 2],  # Join Key
        "signup_date": ["2022-09-25", "2022-10-03", "2021-07-12"],
        "gender": ["male", "female", "male"]
    })

    # Segments Mock (based on segments.csv structure)
    mock_segments_df = pd.DataFrame({
        "segment_id": [1, 2], # Join Key
        "segment_name": ["Regular", "VIP"],
        "discount_pct": [0, 10],
        "priority_support": [False, True]
    })

    # 3. Prepare the data dictionary list
    # 'dimension' must match 'CustomersDim' in pipeline.yaml
    # 'source' must match 'customers' and 'segments' in pipeline.yaml
    mock_data_framse_dicts = [
        {
            "dimension": "CustomersDim", 
            "source": "customers", 
            "dataframe": mock_customers_df
        },
        {
            "dimension": "CustomersDim", 
            "source": "segments", 
            "dataframe": mock_segments_df
        }
    ]

    # 4. Initialize and Run Task
    join_task = Join(audit=audit, registry=registry)
    join_task.set_data_framse_dict(mock_data_framse_dicts)
    
    print("="*60)
    print("RUNNING JOIN TEST: CustomersDim (customers + segments)")
    print("="*60)
    
    success, errors, _, _, _ = join_task.do_task({})

    # 5. Verification
    if success:
        # The result is stored back into the first dictionary of the list
        result_df = mock_data_framse_dicts[0]["dataframe"]
        
        print("✅ SUCCESS!")
        print("\nColumns after join:")
        print(result_df.columns.tolist())
        
        print("\nMerged Data Snippet:")
        # We expect segment_name and discount_pct to be merged in
        cols_to_show = ['customer_id', 'full_name', 'segment_id', 'segment_name', 'discount_pct']
        print(result_df[cols_to_show])
    else:
        print(f"❌ FAILED! Errors: {errors}")