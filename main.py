import os
import pandas as pd
import requests
from io import StringIO
from flask import Flask, request
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackContext

app = Flask(__name__)

# === Переменные окружения ===
TOKEN           = os.getenv("TOKEN")
SELF_URL        = os.getenv("SELF_URL")
ZONES_CSV_URL   = os.getenv("ZONES_CSV_URL")   # CSV‑URL таблицы зон: ID,Region,Name
REES_SHEETS_MAP = {
    p.split("=",1)[0]: p.split("=",1)[1]
    for p in os.getenv("REES_SHEETS_MAP", "").split(",") if p
}

# === Инициализация бота ===
bot        = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)

# хранит состояние по юзеру: номер, регион, имя
user_states = {}  # { user_id: {"number": str, "region": str, "name": str} }

def load_zones_map() -> dict:
    """
    Загружает CSV‑таблицу зон (ID,Region,Name) и возвращает
      { user_id: {"region": str, "name": str} }
    """
    resp = requests.get(ZONES_CSV_URL)
    resp.raise_for_status()
    # убираем BOM, если есть
    text = resp.content.decode("utf-8-sig")
    df = pd.read_csv(StringIO(text), dtype=str)
    result = {}
    for _, row in df.iterrows():
        uid   = row["ID"].strip()
        region= row["Region"].strip()
        name  = row["Name"].strip()
        result[uid] = {"region": region, "name": name}
    return result

def load_data(excel_url: str) -> pd.DataFrame:
    """Скачивает XLSX‑таблицу РЭС по URL и возвращает DataFrame."""
    r = requests.get(excel_url)
    r.raise_for_status()
    return pd.read_excel(pd.io.common.BytesIO(r.content), dtype=str)

def start(update: Update, context: CallbackContext):
    update.message.reply_text("Введите номер счётчика")

def handle_message(update: Update, context: CallbackContext):
    user_id = str(update.message.from_user.id)
    text    = update.message.text.strip()

    # повторный выбор информации
    if text in ("Информация по договору",
                "Информация по адресу подключения",
                "Информация по прибору учёта"):
        st = user_states.get(user_id)
        if not st:
            return update.message.reply_text("Сначала введите номер счётчика")
        return send_info(update, st["number"], text, st["region"])

    # загружаем зоны и получаем регион+имя
    zones = load_zones_map()
    user_info = zones.get(user_id)
    if not user_info:
        return update.message.reply_text("У вас нет прав или вы не назначены ни в один РЭС.")
    region    = user_info["region"]
    user_name = user_info["name"]

    # нормализуем ввод
    norm_input = text.lstrip("0") or "0"
    found_reg  = None
    matched    = None

    # ищем по всем или по конкретному региону
    if region.upper() == "ALL":
        for reg, url in REES_SHEETS_MAP.items():
            df   = load_data(url)
            ser  = df["Номер счетчика"].astype(str)
            norm = ser.str.lstrip("0").replace("", "0")
            mask = norm == norm_input
            if mask.any():
                found_reg = reg
                matched   = ser[mask].iloc[0]
                break
        if not found_reg:
            return update.message.reply_text("Номер не найден ни в одном регионе.")
    else:
        excel_url = REES_SHEETS_MAP.get(region)
        if not excel_url:
            return update.message.reply_text(f"Для региона «{region}» таблица не задана.")
        df   = load_data(excel_url)
        ser  = df["Номер счетчика"].astype(str)
        norm = ser.str.lstrip("0").replace("", "0")
        mask = norm == norm_input
        if not mask.any():
            return update.message.reply_text("Номер не найден. Проверьте ввод.")
        found_reg, matched = region, ser[mask].iloc[0]

    # сохраняем состояние и обращаемся по имени
    user_states[user_id] = {
        "number": matched,
        "region": found_reg,
        "name":   user_name
    }
    update.message.reply_text(f"Принял в работу, {user_name}")

    keyboard = [
        ["Информация по договору"],
        ["Информация по адресу подключения"],
        ["Информация по прибору учёта"],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    update.message.reply_text("Что будем искать?", reply_markup=reply_markup)

def send_info(update: Update, number: str, info_type: str, region: str):
    df  = load_data(REES_SHEETS_MAP[region])
    row = df[df["Номер счетчика"].astype(str) == number]
    if row.empty:
        return update.message.reply_text("Данные не найдены.")
    if info_type == "Информация по договору":
        cols = ["ТУ","Номер ТУСТЕК","Номер ТУ","ЛС / ЛС СТЕК",
                "Наименование договора","Вид потребителя","Субабонент"]
    elif info_type == "Информация по адресу подключения":
        cols = ["Сетевой участок","Населенный пункт","Улица","Дом","ТП"]
    else:
        cols = [
            "Номер счетчика","Состояние ТУ","Максимальная мощность","Вид счетчика","Фазность",
            "Госповерка счетчика","Межповерочный интервал ПУ","Окончание срок поверки",
            "Проверка схемы дата","Последнее активное событие дата",
            "Первичный ток ТТ (А)","Госповерка ТТ (А)","Межповерочный интервал ТТ"
        ]
    data = row.iloc[0]
    message = "\n".join(f"{c}: {data.get(c,'Нет данных')}" for c in cols)
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
