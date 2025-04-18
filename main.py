import os
import pandas as pd
import telegram
from telegram.ext import Dispatcher, MessageHandler, Filters
from flask import Flask, request
import requests
from io import BytesIO

# Flask-приложение
app = Flask(__name__)

# Telegram настройки
TOKEN = os.environ.get("TOKEN")
SELF_URL = os.environ.get("SELF_URL")
bot = telegram.Bot(token=TOKEN)

# Список разрешённых ID
allowed_ids = os.environ.get("ALLOWED_IDS", "")
ALLOWED_USERS = [int(x) for x in allowed_ids.split(",") if x.strip().isdigit()]

# ССЫЛКА НА GOOGLE ТАБЛИЦУ
EXCEL_URL = "https://docs.google.com/uc?export=download&id=1JWG4YLxd7ltI_K1JASKC1z6gpP-dEa8P"

def load_data():
    try:
        response = requests.get(EXCEL_URL)
        response.raise_for_status()
        file_data = BytesIO(response.content)
        df = pd.read_excel(file_data, engine='openpyxl')
        return df
    except Exception as e:
        print(f"Ошибка загрузки Excel: {e}")
        return None

def handle_message(update, context):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    print(f"Запрос от пользователя: {user_id}")

    if user_id not in ALLOWED_USERS:
        context.bot.send_message(chat_id=chat_id, text="Пошёл на хуй, я тебя не знаю!")
        return

    query = update.message.text.strip()
    if not query.isdigit():
        context.bot.send_message(chat_id=chat_id, text="Пожалуйста, введите номер (только цифры).")
        return

    context.bot.send_message(chat_id=chat_id, text="Запрос получен. Ищу данные, подожди немного...")

    df = load_data()
    if df is None:
        context.bot.send_message(chat_id=chat_id, text="Ошибка загрузки таблицы. Попробуй позже.")
        return

    try:
        row = df[df.iloc[:, 0] == int(query)]
        if row.empty:
            context.bot.send_message(chat_id=chat_id, text="Номер не найден.")
        else:
            row = row.iloc[0]
            response = ""

            for col in df.columns[1:11]:  # первые 10 столбцов
                response += f"{col}: {row[col]}\n"

            if len(response) > 4000:
                response = response[:4000] + "\n...Обрезано по длине"

            context.bot.send_message(chat_id=chat_id, text=response)
    except Exception as e:
        print(f"Ошибка обработки данных: {e}")
        context.bot.send_message(chat_id=chat_id, text="Произошла ошибка при обработке данных.")

# Telegram webhook (обработка POST-запроса)
from telegram.ext import CallbackContext
dispatcher = Dispatcher(bot, None, workers=0)
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

@app.route(f'/{TOKEN}', methods=['POST'])
def webhook():
    update = telegram.Update.de_json(request.get_json(force=True), bot)
    dispatcher.process_update(update)
    return 'ok'

@app.route('/')
def index():
    return 'Бот на webhook работает!'

# Установка webhook при запуске
if __name__ == '__main__':
    webhook_url = f"{SELF_URL}/{TOKEN}"
    bot.delete_webhook()
    bot.set_webhook(url=webhook_url)
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
