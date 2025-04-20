import os
import pandas as pd
import requests
import datetime
from io import StringIO
from flask import Flask, request
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackContext

app = Flask(__name__)

# === Telegram ===
TOKEN    = os.getenv("TOKEN")
SELF_URL = os.getenv("SELF_URL")
bot      = Bot(token=TOKEN)
dp       = Dispatcher(bot, None, use_context=True)

# === Ссылки на таблицы по РЭС ===
REES_SHEETS_MAP = {
    item.split("=",1)[0]: item.split("=",1)[1]
    for item in os.getenv("REES_SHEETS_MAP", "").split(",") if item
}

# === Зоны видимости из CSV ===
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL")  # экспорт CSV: .../export?format=csv&gid=0

# === Логи через Google Form ===
LOGS_FORM_URL             = os.getenv("LOGS_FORM_URL")              # URL вида https://docs.google.com/forms/d/e/.../formResponse
LOGS_FORM_FIELD_USER      = os.getenv("LOGS_FORM_FIELD_USER")       # например entry.123456789
LOGS_FORM_FIELD_TIMESTAMP = os.getenv("LOGS_FORM_FIELD_TIMESTAMP")  # например entry.987654321
LOGS_FORM_FIELD_NUMBER    = os.getenv("LOGS_FORM_FIELD_NUMBER")     # например entry.555555555

# кеш текущих состояний
user_states = {}

def load_zones_map() -> dict:
    """Скачиваем CSV‑таблицу зон и строим {user_id: region}."""
    resp = requests.get(ZONES_CSV_URL)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), dtype=str)
    # ожидаем колонки "ID" и "Region"
    return {
        str(r).strip(): region.strip()
        for r, region in zip(df["ID"].astype(str), df["Region"].astype(str))
    }

def log_request(user_id: str, number: str):
    """POST-запросом из формы записываем в логи: ID | timestamp | number."""
    now = datetime.datetime.now().isoformat()
    data = {
        LOGS_FORM_FIELD_USER:      user_id,
        LOGS_FORM_FIELD_TIMESTAMP: now,
        LOGS_FORM_FIELD_NUMBER:    number,
    }
    try:
        requests.post(LOGS_FORM_URL, data=data)
    except Exception:
        pass  # не фатально, если лог не отправился

def load_data(excel_url: str) -> pd.DataFrame:
    """Скачиваем XLSX‑таблицу РЭС и возвращаем DataFrame."""
    r = requests.get(excel_url)
    r.raise_for_status()
    return pd.read_excel(pd.io.common.BytesIO(r.content), dtype=str)

def start(update: Update, ctx: CallbackContext):
    update.message.reply_text("Введите номер счётчика")

def handle_message(update: Update, ctx: CallbackContext):
    user_id = str(update.message.from_user.id)
    text_raw = update.message.text.strip()
    norm_input = text_raw.lstrip("0") or "0"

    # если это выбор типа инфо — отсылаем
    if text_raw in ["Информация по договору", "Информация по адресу подключения", "Информация по прибору учёта"]:
        st = user_states.get(user_id)
        if not st:
            return update.message.reply_text("Пожалуйста, сначала введите номер счётчика или ЛС")
        return send_info(update, st["number"], text_raw, st["region"])

    # иначе — пытаемся принять это за номер
    zones  = load_zones_map()
    region = zones.get(user_id)
    if not region:
        return update.message.reply_text("У вас нет прав или вы не назначены ни в один РЭС.")

    found_region = None
    matched      = None

    # ищем либо во всех, либо в одном
    if region.upper() == "ALL":
        for reg, url in REES_SHEETS_MAP.items():
            df     = load_data(url)
            ser    = df["Номер счетчика"].astype(str)
            norm   = ser.str.lstrip("0").replace("", "0")
            mask   = norm == norm_input
            if mask.any():
                found_region = reg
                matched      = ser[mask].iloc[0]
                break
        if not found_region:
            return update.message.reply_text("Номер не найден ни в одном регионе.")
    else:
        url = REES_SHEETS_MAP.get(region)
        if not url:
            return update.message.reply_text(f"Для вашего региона ({region}) таблица не задана.")
        df   = load_data(url)
        ser  = df["Номер счетчика"].astype(str)
        norm = ser.str.lstrip("0").replace("", "0")
        mask = norm == norm_input
        if not mask.any():
            return update.message.reply_text("Номер не найден. Проверьте ввод.")
        found_region = region
        matched      = ser[mask].iloc[0]

    # логируем запрос
    log_request(user_id, matched)

    # запоминаем и спрашиваем, что именно
    user_states[user_id] = {"number": matched, "region": found_region}
    kb = [
        ["Информация по договору"],
        ["Информация по адресу подключения"],
        ["Информация по прибору учёта"],
    ]
    markup = ReplyKeyboardMarkup(kb, one_time_keyboard=False, resize_keyboard=True)
    update.message.reply_text(
        "Принял в работу. По какой из трёх вариантов поиска будем искать информацию?",
        reply_markup=markup
    )

def send_info(update: Update, number: str, info_type: str, region: str):
    url = REES_SHEETS_MAP.get(region)
    df  = load_data(url)
    row = df[df["Номер счетчика"].astype(str) == number]
    if row.empty:
        return update.message.reply_text("Данные не найдены.")

    if info_type == "Информация по договору":
        cols = ["ТУ", "Номер ТУСТЕК", "Номер ТУ", "ЛС / ЛС СТЕК",
                "Наименование договора", "Вид потребителя", "Субабонент"]
    elif info_type == "Информация по адресу подключения":
        cols = ["Сетевой участок", "Населенный пункт", "Улица", "Дом", "ТП"]
    else:
        cols = ["Номер счетчика", "Состояние ТУ", "Максимальная мощность", "Вид счетчика",
                "Фазность", "Госповерка счетчика", "Межповерочный интервал ПУ",
                "Окончание срок поверки", "Проверка схемы дата", "Последнее активное событие дата",
                "Первичный ток ТТ (А)", "Госповерка ТТ (А)", "Межповерочный интервал ТТ"]

    data = row.iloc[0]
    msg  = "\n".join(f"{c}: {data.get(c, 'Нет данных')}" for c in cols)
    update.message.reply_text(msg)

@app.route("/webhook", methods=["POST"])
def webhook():
    upd = Update.de_json(request.get_json(force=True), bot)
    dp.process_update(upd)
    return "ok"

@app.route("/")
def index():
    return "Бот работает"

dp.add_handler(CommandHandler("start", start))
dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

if __name__ == "__main__":
    bot.set_webhook(f"{SELF_URL}/webhook")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",5000)))
