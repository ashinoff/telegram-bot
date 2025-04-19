import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram.ext import Updater, CommandHandler

# Авторизация Google
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    os.environ["GOOGLE_CREDENTIALS_PATH"], scope
)
client = gspread.authorize(creds)

sheet = client.open_by_key(os.environ["SHEET_ID"])
worksheet = sheet.worksheet(os.environ["SHEET_NAME"])

# Telegram команда /start
def start(update, context):
    data = worksheet.get_all_records()
    preview = data[:3]
    msg = f"Тест Google Sheets подключение:\n\n{preview}"
    update.message.reply_text(msg)

def main():
    updater = Updater(os.environ["TOKEN"], use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
