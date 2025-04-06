import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Supabase credentials
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Telegram bot token
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Time to send daily notification (UTC, 24-hour format)
NOTIFICATION_TIME_UTC = os.getenv("NOTIFICATION_TIME_UTC", "12:00")

# Telegram API base URL
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Parse notification time
try:
    NOTIFICATION_HOUR, NOTIFICATION_MINUTE = map(int, NOTIFICATION_TIME_UTC.split(':'))
except ValueError:
    # Default to 12:00 UTC if parsing fails
    NOTIFICATION_HOUR, NOTIFICATION_MINUTE = 12, 0 