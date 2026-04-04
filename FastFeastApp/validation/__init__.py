# from models.customer import Customer
# from validation.schema_validator import SchemaValidator
# from validation.rows_validator import RowsValidator
# from validation.validator_context import ValidatorContext
# import pandas as pd
# import json

# # corrupted data
# corrupted_df = pd.DataFrame({
#     "customer_id":   [None, "2", "corrupted", "4", "5"],    
#     "full_name":     ["Alice", "Bob", "Charlie", "Diana", "Eve"],
#     "email":         ["a@a.com", "b@b.com", "c@c.com", "d@d.com", "e@e.com"],
#     "phone":         ["01012345678", "01098765432", 123456, "01011112222", "01033334444"], 
#     "region_id":     [1.1, 2.2, 3.3, 4.4, 5.5],
#     "segment_id":    [1, 2, 3, "bad_segment", 5],         
#     "signup_date":   pd.to_datetime(["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"]),
#     "gender":        ["Male", "Female", "Male", 99, "Female"], 
#     "created_at":    pd.to_datetime(["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"]),
#     "updated_at":    pd.to_datetime(["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"]),
#     "surrogate_key": ["uuid-1", "uuid-2", "uuid-3", "uuid-4", "uuid-5"],
#     "is_current":    [True, False, True, "yes", False], 
#     "valid_from":    pd.to_datetime(["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"]),
#     "valid_to":      pd.to_datetime([None, None, "2023-01-01", None, "2023-05-01"]),
# })

# # Extract the list directly as required by your new signature
# required_fields = ["customer_id", "full_name", "email", "segment_id", "signup_date", "gender"]

# validator_ctx = ValidatorContext()
# schema_validator = SchemaValidator()
# rows_validator = RowsValidator()

# # --- TEST 1: Schema Validation ---
# validator_ctx.set_validator(schema_validator)
# print("=" * 40)
# print("TEST 1 — Schema Validation")
# print("=" * 40)

# # Unpacking 4 values now: ok, errors, df, stats
# ok, errors, df_out, stats = validator_ctx.validate(corrupted_df, Customer, required_fields)

# print(f"Schema Valid: {ok}")
# if errors:
#     for e in errors:
#         print(f"  ❌ {e['reason']}")
# else:
#     print("  ✅ Schema matches model")

# # --- TEST 2: Row Validation ---
# validator_ctx.set_validator(rows_validator)
# print("\n" + "=" * 40)
# print("TEST 2 — Row Data Validation & Stats")
# print("=" * 40)

# ok, errors, clean_df, stats = validator_ctx.validate(corrupted_df, Customer, required_fields)

# print(f"Row Validation Complete. Status: {'Success' if ok else 'Failed'}")
# print("-" * 20)
# print("PROCESSING STATS:")
# print(json.dumps(stats, indent=4))
# print("-" * 20)

# if errors:
#     print(f"Found {len(errors)} validation errors:")
#     for e in errors:
#         # Using .get() for safety since row/model keys differ between validators
#         print(f"Target : {e.get('row', e.get('model', 'N/A'))[:50]}...") 
#         print(f"Reason : {e.get('reason')}")
#         print("-" * 40)

# print(f"\nFinal Clean DataFrame Row Count: {len(clean_df)}")