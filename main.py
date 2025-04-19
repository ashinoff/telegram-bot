import os
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext

# Глобальный контекст
user_context = {}

# Группы полей
INFO_FIELDS = {
    "Информация по договору": [
        "ТУ", "Номер ТУСТЕК", "Номер ТУ", "ЛС / ЛС СТЕК",
        "Наименование договора", "Вид потребителя", "Субабонент"
    ],
    "Информация по адресу подключения": [
        "Сетевой участок", "Населенный пункт", "Улица", "Дом", "ТП"
    ],
    "Информация по прибору учета": [
        "Номер счетчика", "Состояние ТУ", "Максимальная мощность", "Вид счетчика",
        "Фазность", "Госповерка счетчика", "Межповерочный интервал ПУ",
        "Окончание срок поверки", "Проверка схемы дата", "Последнее активное событие дата",
        "Первичный ток ТТ (А)", "Госповерка ТТ (А)", "Межповерочный интервал ТТ"
    ]
}

# Скачиваем и читаем Excel
def load_data():
    url = os.environ.get("EXCEL_URL")
    response = requests.get(url)
    file_data = BytesIO(response.content)
    df = pd.read_excel(file_data, engine="openpyxl")
    return df

# /start
def start(update: Update, context: CallbackContext):
    update.message.reply_text("Привет! Введите номер счётчика для поиска.")

# Обработка номера счётчика
def handle_number(update: Update, context: CallbackContext):
    chat_id = update.effective_user.id
    text = update.message.text.strip()

    rees_map = os.environ.get("REES_MAP", "")
    rees_dict = {int(p.split(":")[0]): p.split(":")[1] for p in rees_map.split(",") if ":" in p}
    access = rees_dict.get(chat_id)

    if not access:
        update.message.reply_text("Пошел на х*й, я тебя не знаю!")
        return

    if not text.isdigit():
        update.message.reply_text("Введите номер счётчика (только цифры).")
        return

    try:
        df = load_data()
    except Exception as e:
        update.message.reply_text(f"Ошибка загрузки файла: {e}")
        return

    if access != "ALL":
        df = df[df["Сетевой участок"] == access]

    row = df[df["Номер счетчика"] == int(text)]
    if row.empty:
        update.message.reply_text("Счётчик не найден.")
        return

    user_context[chat_id] = {
        "row": row.iloc[0].to_dict(),
        "timestamp": datetime.now()
    }

    keyboard = [["Информация по договору"],
                ["Информация по адресу подключения"],
                ["Информация по прибору учета"]]
    markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

    update.message.reply_text("Что вас интересует?", reply_markup=markup)

# Обработка кнопок
def handle_button(update: Update, context: CallbackContext):
    chat_id = update.effective_user.id
    text = update.message.text.strip()

    session = user_context.get(chat_id)
    if not session:
        update.message.reply_text("Сначала введите номер счётчика.")
        return

    if datetime.now() - session["timestamp"] > timedelta(minutes=10):
        update.message.reply_text("Сессия устарела. Введите номер счётчика снова.")
        user_context.pop(chat_id, None)
        return

    fields = INFO_FIELDS.get(text)
    if not fields:
        update.message.reply_text("Неверный выбор.")
        return

    row = session["row"]
    response = "\n".join([f"{field}: {row.get(field, '—')}" for field in fields])
    update.message.reply_text(response)

# Запуск бота
def main():
    token = os.environ.get("TOKEN")
    updater = Updater(token, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.regex(r"^\d+$"), handle_number))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_button))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
