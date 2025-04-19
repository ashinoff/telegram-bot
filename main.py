import os
import pandas as pd
import requests
from flask import Flask, request
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackContext

app = Flask(__name__)

TOKEN = os.getenv("TOKEN")
SELF_URL = os.getenv("SELF_URL")
EXCEL_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRC4rukIYI8jEnmgm5kIxmFa67xMcmgHNA1efWv5o87HE9-2_g9GmkNfB__JNK0Kw/pub?output=xlsx"

bot = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)

user_states = {}

def load_data():
    response = requests.get(EXCEL_URL)
    with open("data.xlsx", "wb") as f:
        f.write(response.content)
    df = pd.read_excel("data.xlsx")
    return df

def start(update: Update, context: CallbackContext):
    update.message.reply_text("Введите номер счётчика или ЛС")

def handle_message(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    text = update.message.text.strip()

    if text in ["Информация по договору", "Информация по адресу подключения", "Информация по прибору учёта"]:
        if user_id in user_states:
            search_value = user_states[user_id]
            send_info(update, search_value, text)
        else:
            update.message.reply_text("Пожалуйста, сначала введите номер счётчика или ЛС")
    else:
        df = load_data()
        if text in df["Номер счетчика"].astype(str).values:
            user_states[user_id] = text
            keyboard = [["Информация по договору"], ["Информация по адресу подключения"], ["Информация по прибору учёта"]]
            reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=False, resize_keyboard=True)
            update.message.reply_text("Что вас интересует?", reply_markup=reply_markup)
        else:
            update.message.reply_text("Номер не найден. Проверьте ввод.")

def send_info(update: Update, number, info_type):
    df = load_data()
    row = df[df["Номер счетчика"].astype(str) == number]
    if row.empty:
        update.message.reply_text("Данные не найдены.")
        return

    if info_type == "Информация по договору":
        fields = ["ТУ", "Номер ТУСТЕК", "Номер ТУ", "ЛС / ЛС СТЕК", "Наименование договора", "Вид потребителя", "Субабонент"]
    elif info_type == "Информация по адресу подключения":
        fields = ["Сетевой участок", "Населенный пункт", "Улица", "Дом", "ТП"]
    elif info_type == "Информация по прибору учёта":
        fields = ["Номер счетчика", "Состояние ТУ", "Максимальная мощность", "Вид счетчика", "Фазность", 
                  "Госповерка счетчика", "Межповерочный интервал ПУ", "Окончание срок поверки",
                  "Проверка схемы дата", "Последнее активное событие дата", "Первичный ток ТТ (А)", 
                  "Госповерка ТТ (А)", "Межповерочный интервал ТТ"]
    else:
        update.message.reply_text("Неизвестный запрос.")
        return

    message = "\n".join([f"{col}: {row.iloc[0].get(col, 'Нет данных')}" for col in fields])
    update.message.reply_text(message)

@app.route(f"/webhook", methods=["POST"])
def webhook():
    update = Update.de_json(request.get_json(force=True), bot)
    dispatcher.process_update(update)
    return "ok"

@app.route("/")
def index():
    return "Бот работает"

dispatcher.add_handler(CommandHandler("start", start))
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

if __name__ == "__main__":
    bot.set_webhook(f"{SELF_URL}/webhook")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
