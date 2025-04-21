import os
import pandas as pd
import requests
from io import StringIO
from flask import Flask, request
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackContext

app = Flask(__name__)

# Настройки из окружения
TOKEN           = os.getenv("TOKEN")
SELF_URL        = os.getenv("SELF_URL")
ZONES_CSV_URL   = os.getenv("ZONES_CSV_URL")    # CSV‑URL таблицы зон (ID,Region)
REES_SHEETS_MAP = {
    pair.split("=", 1)[0]: pair.split("=", 1)[1]
    for pair in os.getenv("REES_SHEETS_MAP", "").split(",") if pair
}

bot        = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)
user_states = {}  # { user_id: {"number": str, "region": str} }

def load_zones_map() -> dict:
    """Скачиваем CSV‑таблицу зон и возвращаем { user_id: region }."""
    resp = requests.get(ZONES_CSV_URL)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), dtype=str)
    return dict(zip(df["ID"].str.strip(), df["Region"].str.strip()))

def load_data(excel_url: str) -> pd.DataFrame:
    """Скачиваем XLSX‑таблицу по URL и возвращаем DataFrame."""
    resp = requests.get(excel_url)
    resp.raise_for_status()
    return pd.read_excel(pd.io.common.BytesIO(resp.content), dtype=str)

def start(update: Update, context: CallbackContext):
    update.message.reply_text("Введите номер счётчика")

def handle_message(update: Update, context: CallbackContext):
    user_id = str(update.message.from_user.id)
    text = update.message.text.strip()

    # Если это выбор информации — отдаем её
    if text in ("Информация по договору",
                "Информация по адресу подключения",
                "Информация по прибору учёта"):
        st = user_states.get(user_id)
        if not st:
            return update.message.reply_text("Сначала введите номер счётчика")
        return send_info(update, st["number"], text, st["region"])

    # Иначе — пытаемся воспринять ввод как номер счётчика
    zones = load_zones_map()
    region = zones.get(user_id)
    if not region:
        return update.message.reply_text("У вас нет прав или вы не назначены ни в один РЭС.")

    norm_input = text.lstrip("0") or "0"
    found_region = None
    matched_number = None

    if region.upper() == "ALL":
        for reg, url in REES_SHEETS_MAP.items():
            df = load_data(url)
            ser = df["Номер счетчика"].astype(str)
            norm_ser = ser.str.lstrip("0").replace("", "0")
            mask = norm_ser == norm_input
            if mask.any():
                found_region = reg
                matched_number = ser[mask].iloc[0]
                break
        if not found_region:
            return update.message.reply_text("Номер не найден ни в одном регионе.")
    else:
        url = REES_SHEETS_MAP.get(region)
        if not url:
            return update.message.reply_text(f"Для региона {region} не задана таблица.")
        df = load_data(url)
        ser = df["Номер счетчика"].astype(str)
        norm_ser = ser.str.lstrip("0").replace("", "0")
        mask = norm_ser == norm_input
        if not mask.any():
            return update.message.reply_text("Номер не найден. Проверьте ввод.")
        found_region = region
        matched_number = ser[mask].iloc[0]

    # Сохраняем состояние и спрашиваем, что искать
    user_states[user_id] = {"number": matched_number, "region": found_region}
    update.message.reply_text("Принял в работу")
    keyboard = [
        ["Информация по договору"],
        ["Информация по адресу подключения"],
        ["Информация по прибору учёта"],
    ]
    reply_markup = ReplyKeyboardMarkup(
        keyboard, resize_keyboard=True, one_time_keyboard=False
    )
    update.message.reply_text("Что будем искать?", reply_markup=reply_markup)

def send_info(update: Update, number: str, info_type: str, region: str):
    df = load_data(REES_SHEETS_MAP[region])
    row = df[df["Номер счетчика"].astype(str) == number]
    if row.empty:
        return update.message.reply_text("Данные не найдены.")
    if info_type == "Информация по договору":
        fields = ["ТУ", "Номер ТУСТЕК", "Номер ТУ", "ЛС / ЛС СТЕК",
                  "Наименование договора", "Вид потребителя", "Субабонент"]
    elif info_type == "Информация по адресу подключения":
        fields = ["Сетевой участок", "Населенный пункт", "Улица", "Дом", "ТП"]
    else:
        fields = [
            "Номер счетчика", "Состояние ТУ", "Максимальная мощность", "Вид счетчика",
            "Фазность", "Госповерка счетчика", "Межповерочный интервал ПУ",
            "Окончание срок поверки", "Проверка схемы дата",
            "Последнее активное событие дата", "Первичный ток ТТ (А)",
            "Госповерка ТТ (А)", "Межповерочный интервал ТТ"
        ]
    data = row.iloc[0]
    message = "\n".join(f"{col}: {data.get(col, 'Нет данных')}" for col in fields)
    update.message.reply_text(message)

@app.route("/webhook", methods=["POST"])
def webhook():
    upd = Update.de_json(request.get_json(force=True), bot)
    dispatcher.process_update(upd)
    return "ok"

@app.route("/")
def index():
    return "Бот работает"

# Регистрируем обработчики
dispatcher.add_handler(CommandHandler("start", start))
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

if __name__ == "__main__":
    bot.set_webhook(f"{SELF_URL}/webhook")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
