import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etl.components.read_from_source_factory import ReadFromSourceFactory
from audit.audit import Audit

audit = Audit("batch")
BASE_PATH = "../FastFeast-Data/scripts/data/input/stream/2026-03-06/14"

def test_read_from_source():

    # 🔹 Step 1: list files
    files = os.listdir(BASE_PATH)

    if not files:
        print("❌ No files found in directory")
        return

    # 🔹 Step 2: filter csv/json files
    valid_files = [
        f for f in files if f.endswith(".csv") or f.endswith(".json")
    ]

    if not valid_files:
        print("❌ No CSV or JSON files found")
        return

    print(f"📂 Found {len(valid_files)} files\n")

    # 🔹 Step 3: loop over all files
    for f in valid_files:
        file_path = os.path.join(BASE_PATH, f)

        print(f"\n📂 Processing file: {file_path}")

        # 🔹 create component
        reader = ReadFromSourceFactory.create_source(file_path, audit)

        # 🔹 build input dict
        data_dict = {
            "file_path": file_path,
            "dimension": "TestDim",
            "dataframe": None,
            "source": f
        }

        # 🔹 execute
        ok, errors, result_dict, metrics, bad_rows = reader.do_task(data_dict)

        # 🔹 results
        print("✅ Success:", ok)

        if errors:
            print("❌ Errors:", errors)

        print("📊 Metrics:", metrics)

        df = result_dict.get("dataframe")

        if df is not None:
            print("📄 Data Preview:")
            print(df.head())
        else:
            print("⚠️ No dataframe returned")

        print(result_dict)
test_read_from_source()