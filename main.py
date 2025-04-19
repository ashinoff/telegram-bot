import os
import pandas as pd
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler
import requests
from io import BytesIO
from datetime import datetime, timedelta

EXCEL_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRC4rukIYI8jEnmgm5kIxmFa67xMcmgHNA1efWv5o87HE9-2_g9GmkNfB__JNK0Kw/pub?output=xlsx"
TOKEN = os.environ.get("TOKEN")
REES_MAP = {
    '955536270': 'ALL',
    '7960394970': 'Сочинский РЭС'
}

# Категории заголовков
CATEGORIES = {
    'contract': ['ТУ', 'Номер ТУСТЕК', 'Номер ТУ', 'ЛС / ЛС СТЕК', 'Наименование договора', 'Вид потребителя', 'Субабонент'],
    'address': ['Сетевой участок', 'Населенный пункт', 'Улица', 'Дом', 'ТП'],
    'device': ['Номер счетчика', 'Состояние ТУ', 'Максимальная мощность', 'Вид счетчика', 'Фазность',
               'Госповерка счетчика', 'Межповерочный интервал ПУ', 'Окончание срок поверки',
               'Проверка схемы дата', 'Последнее активное событие дата',
               'Первичный ток ТТ (А)', 'Госповерка ТТ (А)', 'Межповерочный интервал ТТ']
}

# Временное хранилище
user_requests = {}

def load_data():
    response = requests.get(EXCEL_URL)
    file_data = BytesIO(response.content)
    df = pd.read_excel(file_data)
    return df

def start(update, context):
    update.message.reply_text("Введите номер счётчика:")

def handle_message(update, context):
    user_id = str(update.effective_user.id)
    query = update.message.text.strip()

    if not query.isdigit():
        update.message.reply_text("Пожалуйста, введите номер (только цифры).")
        return

    df = load_data()
    access = REES_MAP.get(user_id)

    if not access:
        update.message.reply_text("Пошёл на х*й, я тебя не знаю!")
        return

    if access != 'ALL':
        df = df[df['Сетевой участок'] == access]

    row = df[df.iloc[:, 0] == int(query)]
    if row.empty:
        update.message.reply_text("Номер не найден.")
        return

    # Сохраняем номер и время последнего запроса
    user_requests[user_id] = {
        'data': row.iloc[0],
        'timestamp': datetime.utcnow()
    }

    keyboard = [
        [InlineKeyboardButton("Информация по договору", callback_data='contract')],
        [InlineKeyboardButton("Информация по адресу подключения", callback_data='address')],
        [InlineKeyboardButton("Информация по прибору учёта", callback_data='device')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text("Что именно вас интересует?", reply_markup=reply_markup)

def button_handler(update, context):
    query = update.callback_query
    user_id = str(query.from_user.id)
    query.answer()

    info = user_requests.get(user_id)
    if not info or datetime.utcnow() - info['timestamp'] > timedelta(minutes=10):
        query.edit_message_text("Сессия устарела. Введите номер счётчика заново.")
        return

    category = query.data
    row = info['data']
    fields = CATEGORIES.get(category, [])

    response = '\n'.join(f"{field}: {row.get(field, '—')}" for field in fields)
    query.edit_message_text(response)

def ping_self(context):
    try:
        requests.get(os.environ.get("SELF_URL", "https://example.com"))
    except:
        pass

def main():
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    dp.add_handler(CallbackQueryHandler(button_handler))

    # Самопинг каждые 5 минут
    job_queue = updater.job_queue
    job_queue.run_repeating(ping_self, interval=300, first=10)

    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()
