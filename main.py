import os
import threading
import pandas as pd
import requests
from io import BytesIO
from flask import Flask, request
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackContext

app = Flask(__name__)

# === Переменные окружения ===
TOKEN           = os.getenv("TOKEN")
SELF_URL        = os.getenv("SELF_URL")
ZONES_CSV_URL   = os.getenv("ZONES_CSV_URL")  # CSV‑URL таблицы зон (ID,Region,Name)
REES_SHEETS_MAP = {
    pair.split("=",1)[0]: pair.split("=",1)[1]
    for pair in os.getenv("REES_SHEETS_MAP","").split(",") if pair
}

bot        = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)
user_states = {}  # { user_id: {"number": str, "region": str, "name": str} }

# === Предзагрузка всех таблиц РЭС в память ===
DATA_CACHE = {}

def refresh_cache():
    """
    Загружает все XLSX из REES_SHEETS_MAP в DATA_CACHE и
    планирует сам себя через 1 час.
    """
    for region, url in REES_SHEETS_MAP.items():
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            DATA_CACHE[region] = pd.read_excel(BytesIO(resp.content), dtype=str)
        except Exception as e:
            # Если не удалось загрузить — оставляем старые данные (если были)
            print(f"Error refreshing cache for {region}: {e}")
    # Запланировать следующий запуск через 1 час
    threading.Timer(3600, refresh_cache).start()

# Запуск предзагрузки при старте
refresh_cache()

def load_zones_map() -> dict:
    """Скачивает CSV‑таблицу зон и возвращает { user_id: {"region","name"} }."""
    resp = requests.get(ZONES_CSV_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig")
    df = pd.read_csv(BytesIO(text.encode()), dtype=str)
    # Определяем колонку с именем
    if "Name" in df.columns:
        name_col = "Name"
    elif "Имя" in df.columns:
        name_col = "Имя"
    elif len(df.columns) >= 3:
        name_col = df.columns[2]
    else:
        name_col = None

    zones = {}
    for _, row in df.iterrows():
        uid    = row["ID"].strip()
        region = row["Region"].strip()
        name   = row[name_col].strip() if name_col and pd.notna(row.get(name_col, "")) else ""
        zones[uid] = {"region": region, "name": name}
    return zones

def start(update: Update, context: CallbackContext):
    update.message.reply_text("Введите номер счётчика")

def handle_message(update: Update, context: CallbackContext):
    user_id = str(update.message.from_user.id)
    text    = update.message.text.strip()

    # Если это кнопка «Информация…», сразу показываем
    if text in ("Информация по договору",
                "Информация по адресу подключения",
                "Информация по прибору учёта"):
        st = user_states.get(user_id)
        if not st:
            return update.message.reply_text("Сначала введите номер счётчика")
        return send_info(update, st["number"], text, st["region"])

    # Иначе — вводим номер счётчика
    try:
        zones = load_zones_map()
    except Exception as e:
        return update.message.reply_text(f"Ошибка загрузки зон: {e}")
    user_info = zones.get(user_id)
    if not user_info:
        return update.message.reply_text("У вас нет прав или вы не назначены ни в один РЭС.")
    region, user_name = user_info["region"], user_info["name"]

    norm_input = text.lstrip("0") or "0"
    found_reg, matched = None, None

    # Берём данные из кэша
    if region.upper() == "ALL":
        for reg, df in DATA_CACHE.items():
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
        df = DATA_CACHE.get(region)
        if df is None:
            return update.message.reply_text(f"Таблица для региона «{region}» ещё не загружена.")
        ser  = df["Номер счетчика"].astype(str)
        norm = ser.str.lstrip("0").replace("", "0")
        mask = norm == norm_input
        if not mask.any():
            return update.message.reply_text("Номер не найден. Проверьте ввод.")
        found_reg, matched = region, ser[mask].iloc[0]

    # Сохраняем состояние и приветствуем по имени
    user_states[user_id] = {"number": matched, "region": found_reg, "name": user_name}
    greet = f"Принял в работу, {user_name}" if user_name else "Принял в работу"
    update.message.reply_text(greet)

    keyboard = [
        ["Информация по договору"],
        ["Информация по адресу подключения"],
        ["Информация по прибору учёта"],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    update.message.reply_text("Что будем искать?", reply_markup=reply_markup)

def send_info(update: Update, number: str, info_type: str, region: str):
    df = DATA_CACHE.get(region)
    if df is None:
        return update.message.reply_text(f"Таблица для региона «{region}» ещё не загружена.")
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
