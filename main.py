import os
import pandas as pd
import requests
from flask import Flask, request
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackContext

app = Flask(__name__)

TOKEN = os.getenv("TOKEN")
SELF_URL = os.getenv("SELF_URL")

# Мапа user_id -> регион (или ALL)
USER_REES_MAP = {
    uid.strip(): region.strip()
    for item in os.getenv("USER_REES_MAP", "").split(",") if item
    for uid, region in [item.split(":", 1)]
}

# Мапа регион -> ссылка на xlsx (export?format=xlsx)
REES_SHEETS_MAP = {
    name.strip(): url.strip()
    for item in os.getenv("REES_SHEETS_MAP", "").split(",") if item
    for name, url in [item.split("=", 1)]
}

bot = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)
user_states = {}  # хранит { user_id: {"number": "...", "region": "..."} }

def load_data(excel_url: str) -> pd.DataFrame:
    """Скачивает xlsx по ссылке и загружает в DataFrame."""
    resp = requests.get(excel_url)
    resp.raise_for_status()
    with open("data.xlsx", "wb") as f:
        f.write(resp.content)
    return pd.read_excel("data.xlsx", dtype=str)

def start(update: Update, context: CallbackContext):
    update.message.reply_text("Введите номер счётчика или ЛС")

def handle_message(update: Update, context: CallbackContext):
    user_id = str(update.message.from_user.id)
    text = update.message.text.strip()

    # Если выбран один из вариантов информации
    if text in ["Информация по договору", "Информация по адресу подключения", "Информация по прибору учёта"]:
        state = user_states.get(user_id)
        if not state:
            return update.message.reply_text("Пожалуйста, сначала введите номер счётчика или ЛС")
        # сразу переходим к выдаче
        send_info(update, state["number"], text, state["region"])
        return

    # Иначе — пытаемся воспринять это как номер
    region = USER_REES_MAP.get(user_id)
    if not region:
        return update.message.reply_text("У вас нет прав или вы не назначены ни в один РЭС.")

    # Поиск номера в одном или во всех регионах
    found_region = None
    if region == "ALL":
        for reg, url in REES_SHEETS_MAP.items():
            df = load_data(url)
            if text in df["Номер счетчика"].astype(str).values:
                found_region = reg
                break
        if not found_region:
            return update.message.reply_text("Номер не найден ни в одном регионе.")
    else:
        url = REES_SHEETS_MAP.get(region)
        if not url:
            return update.message.reply_text(f"Для вашего региона ({region}) таблица не задана.")
        df = load_data(url)
        if text not in df["Номер счетчика"].astype(str).values:
            return update.message.reply_text("Номер не найден. Проверьте ввод.")
        found_region = region

    # Сохраняем состояние и спрашиваем, что именно искать
    user_states[user_id] = {"number": text, "region": found_region}
    keyboard = [
        ["Информация по договору"],
        ["Информация по адресу подключения"],
        ["Информация по прибору учёта"],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=False, resize_keyboard=True)
    update.message.reply_text("Принял в работу. По какой из трёх вариантов поиска будем искать информацию?", reply_markup=reply_markup)

def send_info(update: Update, number: str, info_type: str, region: str):
    # Берём нужный URL по региону
    excel_url = REES_SHEETS_MAP.get(region)
    if not excel_url:
        return update.message.reply_text(f"Для региона {region} нет ссылки на данные.")
    df = load_data(excel_url)
    row = df[df["Номер счетчика"].astype(str) == number]
    if row.empty:
        return update.message.reply_text("Данные не найдены.")
    # Определяем поля
    if info_type == "Информация по договору":
        fields = ["ТУ", "Номер ТУСТЕК", "Номер ТУ", "ЛС / ЛС СТЕК", "Наименование договора", "Вид потребителя", "Субабонент"]
    elif info_type == "Информация по адресу подключения":
        fields = ["Сетевой участок", "Населенный пункт", "Улица", "Дом", "ТП"]
    elif info_type == "Информация по прибору учёта":
        fields = [
            "Номер счетчика", "Состояние ТУ", "Максимальная мощность", "Вид счетчика", "Фазность",
            "Госповерка счетчика", "Межповерочный интервал ПУ", "Окончание срок поверки",
            "Проверка схемы дата", "Последнее активное событие дата", "Первичный ток ТТ (А)",
            "Госповерка ТТ (А)", "Межповерочный интервал ТТ"
        ]
    else:
        return update.message.reply_text("Неизвестный запрос.")
    # Формируем и отправляем сообщение
    data = row.iloc[0]
    msg = "\n".join(f"{col}: {data.get(col, 'Нет данных')}" for col in fields)
    update.message.reply_text(msg)

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
