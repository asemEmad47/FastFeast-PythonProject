import os
import sys

# Setup path
base_dir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(base_dir, "..")))

from etl.tasks.email_task import EmailTask
from config import settings 

if __name__ == "__main__":
    email_sender = EmailTask()

    print("TEST: Custom Message (Manual)")
    print("="*60)
    email_sender.set_message("High orphan rate detected in the current micro-batch! Please investigate immediately.")
    print(f"Current Message: {email_sender.message}")

    success, _ = email_sender.do_task()
    if success: print("✅ Custom email sent.")