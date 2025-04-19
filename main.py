import os
import telegram
from telegram import ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from telegram.ext import CallbackQueryHandler, ConversationHandler
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime, timedelta

# Авторизация в Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    os.environ["GOOGLE_CREDENTIALS_PATH"], scope
)
client = gspread.authorize(creds)
sheet = client.open_by_key(os.environ["SHEET_ID"]).worksheet(os.environ["SHEET_NAME"])

# Получение REES_MAP
REES_MAP = {
    int(pair.split(":")[0]): pair.split(":")[1]
    for pair in os.environ.get("REES_MAP", "").split(",")
}

# Хранилище состояний
user_context = {}

# Заголовки по категориям
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
        "Окончание срок поверки", "Проверка схемы дата",
        "Последнее активное событие дата", "Первичный ток ТТ (А)",
        "Госповерка ТТ (А)", "Межповерочный интервал ТТ"
    ]
}

# Обработка сообщений
def handle_message(update, context: CallbackContext):
    chat_id = update.effective_user.id
    user_input = update.message.text.strip()

    # Проверка доступа
    user_res = REES_MAP.get(chat_id)
    if not user_res:
        update.message.reply_text("Ты мне не знаком — до свидания.")
        return

    if not user_input.isdigit():
        update.message.reply_text("Введите номер счётчика (только цифры).")
        return

    df = pd.DataFrame(sheet.get_all_records())
    if user_res != "ALL":
        df = df[df["Сетевой участок"] == user_res]

    row = df[df["Номер счетчика"] == int(user_input)]
    if row.empty:
        update.message.reply_text("Номер счётчика не найден.")
        return

    # Сохраняем контекст
    user_context[chat_id] = {
        "last_query": row.iloc[0].to_dict(),
        "last_time": datetime.now()
    }

    keyboard = [["Информация по договору"],
                ["Информация по адресу подключения"],
                ["Информация по прибору учета"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=False, resize_keyboard=True)

    update.message.reply_text("Что именно вас интересует?", reply_markup=reply_markup)
from telegram.ext import CallbackContext

# Обработка выбора кнопки
def handle_button(update, context: CallbackContext):
    chat_id = update.effective_user.id
    text = update.message.text.strip()

    # Проверка: есть ли активный номер?
    if chat_id not in user_context:
        update.message.reply_text("Сначала введите номер счётчика.")
        return

    session = user_context[chat_id]
    elapsed = datetime.now() - session["last_time"]
    if elapsed > timedelta(minutes=10):
        update.message.reply_text("Время сессии истекло. Введите номер счётчика заново.")
        user_context.pop(chat_id, None)
        return

    row = session["last_query"]
    if text not in INFO_FIELDS:
        update.message.reply_text("Неверный выбор. Используйте кнопки.")
        return

    fields = INFO_FIELDS[text]
    response = "\n".join([f"{field}: {row.get(field, '—')}" for field in fields])
    update.message.reply_text(response)

# Команда /start
def start(update, context):
    update.message.reply_text("Привет! Введите номер счётчика для начала.")

# Запуск бота
def main():
    token = os.environ["TOKEN"]
    updater = Updater(token, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.regex(r"^\d+$"), handle_message))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_button))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
