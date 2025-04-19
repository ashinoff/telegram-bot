import os
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Updater, CommandHandler, MessageHandler, CallbackQueryHandler, Filters, CallbackContext
import threading
import time

EXCEL_URL = os.environ.get("EXCEL_URL")
REES_MAP = {p.split(":")[0]: p.split(":")[1] for p in os.environ.get("REES_MAP", "").split(",")}
TOKEN = os.environ.get("TOKEN")
SELF_URL = os.environ.get("SELF_URL")

user_data = {}
TIMEOUT = 600  # 10 минут

CATEGORIES = {
    'contract': ['ТУ', 'Номер ТУСТЕК', 'Номер ТУ', 'ЛС / ЛС СТЕК', 'Наименование договора', 'Вид потребителя', 'Субабонент'],
    'address': ['Сетевой участок', 'Населенный пункт', 'Улица', 'Дом', 'ТП'],
    'meter': ['Номер счетчика', 'Состояние ТУ', 'Максимальная мощность', 'Вид счетчика', 'Фазность',
              'Госповерка счетчика', 'Межповерочный интервал ПУ', 'Окончание срок поверки',
              'Проверка схемы дата', 'Последнее активное событие дата',
              'Первичный ток ТТ (А)', 'Госповерка ТТ (А)', 'Межповерочный интервал ТТ']
}

def load_data():
    response = requests.get(EXCEL_URL)
    df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
    return df

def start(update: Update, context: CallbackContext):
    update.message.reply_text("Введите номер счётчика:")

def handle_number(update: Update, context: CallbackContext):
    user_id = str(update.effective_user.id)
    text = update.message.text.strip()

    if not text.isdigit():
        update.message.reply_text("Введите номер счётчика (только цифры).")
        return

    access = REES_MAP.get(user_id)
    if not access:
        update.message.reply_text("Пошёл на х*й, я тебя не знаю!")
        return

    try:
        df = load_data()
    except Exception as e:
        update.message.reply_text(f"Ошибка загрузки файла: {e}")
        return

    if access != 'ALL':
        df = df[df['Сетевой участок'] == access]

    row = df[df['Номер счетчика'] == int(text)]
    if row.empty:
        update.message.reply_text("Счётчик не найден.")
        return

    user_data[user_id] = {
        'row': row.iloc[0].to_dict(),
        'time': datetime.utcnow()
    }

    buttons = [
        [InlineKeyboardButton("Информация по договору", callback_data='contract')],
        [InlineKeyboardButton("Информация по адресу подключения", callback_data='address')],
        [InlineKeyboardButton("Информация по прибору учёта", callback_data='meter')]
    ]
    update.message.reply_text("Выберите, что вас интересует:", reply_markup=InlineKeyboardMarkup(buttons))

def handle_button(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    user_id = str(query.from_user.id)

    data = user_data.get(user_id)
    if not data:
        query.edit_message_text("Сначала введите номер счётчика.")
        return

    if datetime.utcnow() - data['time'] > timedelta(seconds=TIMEOUT):
        user_data.pop(user_id, None)
        query.edit_message_text("Сессия устарела. Введите номер счётчика заново.")
        return

    category = query.data
    fields = CATEGORIES.get(category, [])
    row = data['row']
    response = "\n".join(f"{field}: {row.get(field, '—')}" for field in fields)
    query.edit_message_text(response)

def ping_self():
    while True:
        try:
            if SELF_URL:
                requests.get(SELF_URL)
        except:
            pass
        time.sleep(300)

def main():
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_number))
    dp.add_handler(CallbackQueryHandler(handle_button))

    threading.Thread(target=ping_self, daemon=True).start()

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
