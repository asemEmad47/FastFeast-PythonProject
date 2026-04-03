import os

import pandas as pd
from audit.audit import Audit
from db.database_manager import DatabaseManager
from registry.data_registry import DataRegistry
from registry.conf_file_parser import ConfFileParser
from etl.components.join import Join
from repository.base_repository import BaseRepository

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
