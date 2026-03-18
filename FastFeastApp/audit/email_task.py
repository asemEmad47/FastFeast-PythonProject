"""
EmailTask — Task implementation for async alert emails.
Called by WorkFlow._trigger_alert() on a daemon thread.
The pipeline never waits for this — it fires and forgets.
"""
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from config.settings import (
    ALERT_SMTP_HOST, ALERT_SMTP_PORT,
    ALERT_FROM_EMAIL, ALERT_TO_EMAIL,
)
from etl.task import Task


class EmailTask(Task):

    def do_task(self, message: str = "") -> tuple[bool, list[str]]:
        try:
            timestamp = datetime.utcnow().isoformat()
            body = (
                "FastFeast Pipeline Alert\n"
                "----------------------------------------\n"
                f"Time:    {timestamp}\n"
                f"Message: {message}\n"
            )
            msg = MIMEText(body)
            msg["Subject"] = f"[FastFeast Alert] {message[:60]}"
            msg["From"]    = ALERT_FROM_EMAIL
            msg["To"]      = ALERT_TO_EMAIL

            with smtplib.SMTP(ALERT_SMTP_HOST, ALERT_SMTP_PORT, timeout=10) as smtp:
                smtp.sendmail(ALERT_FROM_EMAIL, [ALERT_TO_EMAIL], msg.as_string())

            return True, []
        except Exception as exc:
            # Never propagate — email failure must not crash the pipeline
            return False, [str(exc)]
