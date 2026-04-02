import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from config.settings import (
    ALERT_SMTP_HOST, ALERT_SMTP_PORT,
    ALERT_FROM_EMAIL, ALERT_TO_EMAIL,
    ALERT_EMAIL_PASSWORD, ALERT_DEFAULT_SUBJECT, ALERT_ORPHAN_MESSAGE
)
from etl.task import Task

class EmailTask(Task):
    def __init__(self):
        self.message = ALERT_ORPHAN_MESSAGE

    def set_message(self, message: str = None):

        if message:
            self.message = message
        else:
            self.message = ALERT_ORPHAN_MESSAGE

    def do_task(self) -> tuple[bool, list[str]]:
        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            
            body = (
                "FastFeast Pipeline Alert\n"
                "----------------------------------------\n"
                f"Time:    {timestamp}\n"
                f"Message: {self.message}\n"
            )

            msg = MIMEMultipart()
            
            clean_subject = ALERT_DEFAULT_SUBJECT.replace("\n", " ").strip()
            msg["Subject"] = f"[FastFeast Alert] {clean_subject[:60]}"
            
            msg["From"] = ALERT_FROM_EMAIL
            msg["To"]   = ALERT_TO_EMAIL
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(ALERT_SMTP_HOST, ALERT_SMTP_PORT, timeout=10) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(ALERT_FROM_EMAIL, ALERT_EMAIL_PASSWORD)
                smtp.sendmail(ALERT_FROM_EMAIL, [ALERT_TO_EMAIL], msg.as_string())

            return True, []

        except Exception as exc:
            return False, [str(exc)]