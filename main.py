import os
import threading
import pandas as pd
import telegram
from telegram.ext import Updater, MessageHandler, Filters
import requests
from io import BytesIO
from flask import Flask
import time

# Flask-приложение для Render Web Service
app = Flask(__name__)

@app.route('/')
def index():
    return "Бот работает!"

# Получаем список разрешённых Telegram ID
allowed_ids = os.environ.get("ALLOWED_IDS", "")
ALLOWED_USERS = [int(x) for x in allowed_ids.split(",") if x.strip().isdigit()]

# Excel-файл
EXCEL_URL = "https://docs.google.com/uc?export=download&id=1s2zMtwdMaHJSOCXflfw4puL5kzIbiObb"

def load_data():
    response = requests.get(EXCEL_URL)
    file_data = BytesIO(response.content)
    df = pd.read_excel(file_data, engine='openpyxl')
    return df

# Ответ на сообщения
def handle_message(update, context):
    user_id = update.effective_user.id
    if user_id not in ALLOWED_USERS:
        update.message.reply_text("Пошёл на хуй, я тебя не знаю!")
        return

    query = update.message.text.strip()
    if not query.isdigit():
        update.message.reply_text("Пожалуйста, введите номер (только цифры).")
        return

    df = load_data()
    row = df[df.iloc[:, 0] == int(query)]

    if row.empty:
        update.message.reply_text("Номер не найден.")
    else:
        row = row.iloc[0]
        response = ""
        for col in df.columns[1:]:
            response += f"{col}: {row[col]}\n"
        update.message.reply_text(response)

# Запуск Telegram-бота
def run_bot():
    token = os.environ.get("TOKEN")
    updater = Updater(token, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    updater.start_polling()
    updater.idle()

# Авто-пинг сам себя каждые 5 минут
def keep_alive():
    url = os.environ.get("SELF_URL")
    if not url:
        print("SELF_URL не указан, авто-пинг отключён")
        return

    def ping():
        while True:
            try:
                requests.get(url)
                print("Пинг отправлен")
            except Exception as e:
                print(f"Ошибка пинга: {e}")
            time.sleep(300)

    threading.Thread(target=ping).start()

# Запуск
if __name__ == "__main__":
    threading.Thread(target=run_bot).start()
    keep_alive()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
