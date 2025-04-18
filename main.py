import os
import threading
import pandas as pd
import telegram
from telegram.ext import Updater, MessageHandler, Filters
import requests
from io import BytesIO
from flask import Flask

# Flask-приложение для Render (Web Service)
app = Flask(__name__)

@app.route('/')
def index():
    return "Бот работает!"

# Список ID из переменной окружения
allowed_ids = os.environ.get("ALLOWED_IDS", "")
ALLOWED_USERS = [int(x) for x in allowed_ids.split(",") if x.strip().isdigit()]

# Ссылка на Excel-файл
EXCEL_URL = "https://docs.google.com/uc?export=download&id=1s2zMtwdMaHJSOCXflfw4puL5kzIbiObb"

def load_data():
    response = requests.get(EXCEL_URL)
    file_data = BytesIO(response.content)
    df = pd.read_excel(file_data, engine='openpyxl')
    return df

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

def run_bot():
    token = os.environ.get("TOKEN")
    updater = Updater(token, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    updater.start_polling()
    updater.idle()

# Запуск бота в фоновом потоке
threading.Thread(target=run_bot).start()

# Запуск Flask (для Render)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
