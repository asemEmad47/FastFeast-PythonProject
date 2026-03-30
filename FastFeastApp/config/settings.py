"""
settings.py — All environment-level constants in one place.
Override with environment variables in production.
"""
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL             = os.getenv("FF_DATABASE_URL",   "postgresql://user:pass@localhost/fastfeast")
BATCH_SOURCE_DIR         = os.getenv("FF_BATCH_DIR",      "data/input/batch")
MICROBATCH_SOURCE_DIR    = os.getenv("FF_STREAM_DIR",      "data/input/stream")
MICROBATCH_POLL_INTERVAL_SEC = int(os.getenv("FF_POLL_SEC", "30"))
QUARANTINE_DIR           = os.getenv("FF_QUARANTINE_DIR", "data/quarantine")
ARCHIVE_DIR              = os.getenv("FF_ARCHIVE_DIR",    "data/archive")
CONF_PATH                = os.getenv("FF_CONF_PATH",      "conf/pipeline.yaml")
ALERT_SMTP_HOST          = os.getenv("FF_SMTP_HOST",      "smtp.fastfeast.com")
ALERT_SMTP_PORT          = int(os.getenv("FF_SMTP_PORT",  "587"))
ALERT_FROM_EMAIL         = os.getenv("FF_ALERT_FROM",     "pipeline@fastfeast.com")
ALERT_TO_EMAIL           = os.getenv("FF_ALERT_TO",       "data-team@fastfeast.com")
ORPHAN_RATE_THRESHOLD    = float(os.getenv("FF_ORPHAN_THRESHOLD", "0.05"))
SNOWFLAKE_ACCOUNT        = os.getenv("SNOWFLAKE_ACCOUNT")    # e.g. "xyz12345"
SNOWFLAKE_USER           = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD       = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE       = os.getenv("SNOWFLAKE_DATABASE")   # "FASTFEAST_DWH"
SNOWFLAKE_SCHEMA         = os.getenv("SNOWFLAKE_SCHEMA")     # "ANALYTICS"
SNOWFLAKE_WAREHOUSE      = os.getenv("SNOWFLAKE_WAREHOUSE")  # "FASTFEAST_ETL_WH"
SNOWFLAKE_ROLE           = os.getenv("SNOWFLAKE_ROLE")       # "FASTFEAST_ADMIN"
