import os
import pandas as pd
import telegram
from telegram.ext import Dispatcher, MessageHandler, Filters
from flask import Flask, request
import requests
from io import BytesIO

# Инициализация Flask
app = Flask(__name__)

# Telegram Token и URL
TOKEN = os.environ.get("TOKEN")
SELF_URL = os.environ.get("SELF_URL")  # например, https://yourbot.onrender.com

bot = telegram.Bot(token=TOKEN)

# ALLOWED USERS
allowed_ids = os.environ.get("ALLOWED_IDS", "")
ALLOWED_USERS = [int(x) for x in allowed_ids.split(",") if x.strip().isdigit()]

# Excel-файл
EXCEL_URL = "https://docs.google.com/spreadsheets/d/1JWG4YLxd7ltI_K1JASKC1z6gpP-dEa8P/edit?usp=drive_link&ouid=116607731149286795427&rtpof=true&sd=true"

def load_data():
    response = requests.get(EXCEL_URL)
    file_data = BytesIO(response.content)
    df = pd.read_excel(file_data, engine='openpyxl')
    return df

# Обработка сообщений
def handle_message(update, context):
    user_id = update.effective_user.id
    if user_id not in ALLOWED_USERS:
        context.bot.send_message(chat_id=update.effective_chat.id, text="Пошёл на хуй, я тебя не знаю!")
        return

    query = update.message.text.strip()
    if not query.isdigit():
        context.bot.send_message(chat_id=update.effective_chat.id, text="Пожалуйста, введите номер счетчика (только цифры).")
        return

    df = load_data()
    row = df[df.iloc[:, 0] == int(query)]

    if row.empty:
        context.bot.send_message(chat_id=update.effective_chat.id, text="Номер не найден.")
    else:
        row = row.iloc[0]
        response = ""
        for col in df.columns[1:]:
            response += f"{col}: {row[col]}\n"
        context.bot.send_message(chat_id=update.effective_chat.id, text=response)

# Настройка dispatcher
from telegram.ext import CallbackContext
dispatcher = Dispatcher(bot, None, workers=0)
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

# Flask маршрут для Telegram webhook
@app.route(f'/{TOKEN}', methods=['POST'])
def webhook():
    update = telegram.Update.de_json(request.get_json(force=True), bot)
    dispatcher.process_update(update)
    return 'ok'

# Корневой маршрут (для проверки, что бот жив)
@app.route('/')
def index():
    return 'Бот на webhook работает!'

# Установка webhook
if __name__ == '__main__':
    webhook_url = f"{SELF_URL}/{TOKEN}"
    bot.delete_webhook()
    bot.set_webhook(url=webhook_url)
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
