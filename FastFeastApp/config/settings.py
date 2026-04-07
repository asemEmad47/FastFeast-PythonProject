"""
settings.py — All environment-level constants in one place.
Override with environment variables in production via .env file.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# DATABASE SETTINGS
# ============================================================
DATABASE_URL = os.getenv("FF_DATABASE_URL", "postgresql://user:pass@localhost/fastfeast")

# ============================================================
# Email conf
ALERT_SMTP_HOST      = os.getenv("ALERT_SMTP_HOST",      "smtp.gmail.com")
ALERT_SMTP_PORT      = int(os.getenv("ALERT_SMTP_PORT",  "587"))
ALERT_FROM_EMAIL     = os.getenv("ALERT_FROM_EMAIL",     "asememad984@gmail.com")
ALERT_TO_EMAIL       = os.getenv("ALERT_TO_EMAIL",       "asememad590@gmail.com")
ALERT_EMAIL_PASSWORD = os.getenv("ALERT_EMAIL_PASSWORD", "tpfsajouaawlqmzw")

# Snow flake part
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE",  "FASTFEAST")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",    "FASTFEASTDWH")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "FASTFEAST_WH")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE",      "FASTFEAST_ADMIN")

# ============================================================
# New Message Settings
ALERT_DEFAULT_SUBJECT = os.getenv("FF_ALERT_SUBJECT", "FastFeast Pipeline Notification")
ALERT_ORPHAN_MESSAGE  = os.getenv("FF_ORPHAN_MSG",    "High orphan rate detected in the current micro-batch!")