import os
import threading
import re
import pandas as pd
import requests
from io import BytesIO, StringIO
from flask import Flask, request
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackContext

app = Flask(__name__)

# === Переменные окружения ===
TOKEN           = os.getenv("TOKEN")
SELF_URL        = os.getenv("SELF_URL")      # ваш URL, например https://bot.onrender.com
ZONES_CSV_URL   = os.getenv("ZONES_CSV_URL") # CSV с колонками ID,Region,Name
REES_SHEETS_MAP = {
    pair.split("=",1)[0]: pair.split("=",1)[1]
    for pair in os.getenv("REES_SHEETS_MAP","").split(",") if pair.strip()
}

# === Google Drive IDs для картинок ===
CABLE_ID       = "11LaH-BvqtUPj2wTQ31wl-1Qrs2aRGb0I"
SELECTIVITY_ID = "11q0orVtOJ_UTk5UVLEn5yGUOeCsWQkaX"
FORMULAS_ID    = "1StUq8JSdpwU1QvJJ6F3W3dHZnReF6kt8"
BASE_DRIVE_URL = "https://drive.google.com/uc?export=download&id="

IMG_CABLE_URL       = BASE_DRIVE_URL + CABLE_ID
IMG_SELECTIVITY_URL = BASE_DRIVE_URL + SELECTIVITY_ID
IMG_FORMULAS_URL    = BASE_DRIVE_URL + FORMULAS_ID

bot        = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)

# состояние пользователей и список чатов для рассылки
user_states = {}   # user_id -> {"mode","number","region","name"}
known_users = set()

# === Утилита для корректного экспорта любых Google‑Sheets URL ===
def make_export_url(url: str) -> str:
    """
    Из любой ссылки вида /d/<ID>/... или /d/e/<ID>/pubhtml
    строит ссылку вида /d/<ID>/export?format=xlsx
    """
    m = re.search(r"/d/(?:e/)?([^/]+)", url)
    if not m:
        return url
    sheet_id = m.group(1)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

# === Предзагрузка таблиц РЭС в память и автоподновление ===
DATA_CACHE = {}
def refresh_cache():
    for region, raw_url in REES_SHEETS_MAP.items():
        export_url = make_export_url(raw_url)
        try:
            r = requests.get(export_url, timeout=10); r.raise_for_status()
            DATA_CACHE[region] = pd.read_excel(BytesIO(r.content), dtype=str)
            print(f"[cache] Loaded {region}")
        except Exception as e:
            print(f"[cache] Error loading {region}: {e}")
    t = threading.Timer(3600, refresh_cache)
    t.daemon = True
    t.start()

refresh_cache()

# === Клавиатуры ===
def main_menu(region):
    items = ["Поиск", "Справочная информация"]
    if region.lower() == "admin":
        items.append("Уведомление всем")
    return ReplyKeyboardMarkup([items], resize_keyboard=True)

HELP_MENU = ReplyKeyboardMarkup([
    ["Поиск", "Справочная информация"],
    ["Сечение кабеля (ток, мощность)"],
    ["Номиналы ВА (ток, мощность)"],
    ["Формулы"],
    ["Назад"]
], resize_keyboard=True)

INFO_MENU = ReplyKeyboardMarkup([
    ["Информация по договору", "Информация по адресу подключения"],
    ["Информация по прибору учёта",   "Назад"]
], resize_keyboard=True)

# === Зона доступа и имена ===
def load_zones_map():
    r = requests.get(ZONES_CSV_URL, timeout=10); r.raise_for_status()
    text = r.content.decode("utf-8-sig")
    df = pd.read_csv(StringIO(text), dtype=str)
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
        name   = row[name_col].strip() if name_col and pd.notna(row.get(name_col)) else ""
        zones[uid] = {"region": region, "name": name}
    return zones

# === Хендлеры Telegram ===
def start(update: Update, context: CallbackContext):
    update.message.reply_text("Меню:", reply_markup=main_menu(""))

def handle_message(update: Update, context: CallbackContext):
    user_id = str(update.effective_user.id)
    text    = update.message.text.strip()

    # загрузка зоны и имени
    try:
        zones = load_zones_map()
    except Exception as e:
        return update.message.reply_text(f"Ошибка загрузки зон: {e}")
    info = zones.get(user_id)
    if not info:
        return update.message.reply_text("У вас нет прав или не назначены в РЭС.")
    region, user_name = info["region"], info["name"]
    known_users.add(user_id)
    state = user_states.get(user_id, {})

    # флаги для поиска
    is_admin = (region.lower() == "admin")
    is_all   = (region.upper() == "ALL")
    search_region = "ALL" if (is_admin or is_all) else region

    # режим broadcast (только для admin)
    if state.get("mode") == "broadcast":
        for uid in known_users:
            try: bot.send_message(chat_id=int(uid), text=text)
            except: pass
        user_states[user_id] = {}
        return update.message.reply_text("Рассылка выполнена.", reply_markup=main_menu(region))

    # главное меню
    if text == "Поиск":
        user_states[user_id] = {"mode": "search"}
        return update.message.reply_text("Введите номер счётчика", reply_markup=main_menu(region))

    if text == "Справочная информация":
        user_states[user_id] = {"mode": "help"}
        return update.message.reply_text("Справка:", reply_markup=HELP_MENU)

    if text == "Уведомление всем" and is_admin:
        user_states[user_id] = {"mode": "broadcast"}
        return update.message.reply_text("Введите текст для рассылки всем:", reply_markup=main_menu(region))

    # справочная информация
    if state.get("mode") == "help":
        if text == "Сечение кабеля (ток, мощность)":
            update.message.reply_photo(photo=IMG_CABLE_URL)
        elif text == "Номиналы ВА (ток, мощность)":
            update.message.reply_photo(photo=IMG_SELECTIVITY_URL)
        elif text == "Формулы":
            update.message.reply_photo(photo=IMG_FORMULAS_URL)
        elif text == "Назад":
            user_states[user_id] = {}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region))
        else:
            return update.message.reply_text("Выберите пункт справки:", reply_markup=HELP_MENU)
        return update.message.reply_text("Справка:", reply_markup=HELP_MENU)

    # ввод номера (search)
    if state.get("mode") == "search":
        norm = text.lstrip("0") or "0"
        found, matched = None, None
        if search_region == "ALL":
            for reg, df in DATA_CACHE.items():
                ser = df["Номер счетчика"].astype(str)
                norm_ser = ser.str.lstrip("0").replace("", "0")
                if (norm_ser == norm).any():
                    found, matched = reg, ser[norm_ser == norm].iloc[0]
                    break
            if not found:
                return update.message.reply_text("Номер не найден ни в одном регионе.", reply_markup=main_menu(region))
        else:
            df = DATA_CACHE.get(search_region)
            if df is None:
                return update.message.reply_text(f"Таблица для «{search_region}» ещё не загружена.")
            ser = df["Номер счетчика"].astype(str)
            norm_ser = ser.str.lstrip("0").replace("", "0")
            if not (norm_ser == norm).any():
                return update.message.reply_text("Номер не найден. Проверьте ввод.", reply_markup=main_menu(region))
            found, matched = search_region, ser[norm_ser == norm].iloc[0]

        user_states[user_id] = {"mode": "info", "number": matched, "region": found}
        greet = f"Принял в работу, {user_name}" if user_name else "Принял в работу"
        return update.message.reply_text(greet, reply_markup=INFO_MENU)

    # информация по счётчику (info)
    if state.get("mode") == "info":
        if text == "Назад":
            user_states[user_id] = {}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region))

        st = user_states[user_id]
        number, region_info = st["number"], st["region"]
        df = DATA_CACHE.get(region_info)
        row = df[df["Номер счетчика"].astype(str) == number]
        if row.empty:
            return update.message.reply_text("Данные не найдены.", reply_markup=INFO_MENU)
        if text == "Информация по договору":
            cols = ["ТУ","Номер ТУСТЕК","Номер ТУ","ЛС / ЛС СТЕК","Наименование договора","Вид потребителя","Субабонент"]
        elif text == "Информация по адресу подключения":
            cols = ["Сетевой участок","Населенный пункт","Улица","Дом","ТП"]
        else:
            cols = ["Номер счетчика","Состояние ТУ","Максимальная мощность","Вид счетчика","Фазность",
                    "Госповерка счетчика","Межповерочный интервал ПУ","Окончание срок поверки",
                    "Проверка схемы дата","Последнее активное событие дата",
                    "Первичный ток ТТ (А)","Госповерка ТТ (А)","Межповерочный интервал ТТ"]
        data = row.iloc[0]
        msg = "\n".join(f"{c}: {data.get(c,'Нет данных')}" for c in cols)
        return update.message.reply_text(msg, reply_markup=INFO_MENU)

    # fallback
    update.message.reply_text("Меню:", reply_markup=main_menu(region))

# === Webhook & запуск ===
@app.route("/webhook", methods=["POST"])
def webhook():
    dispatcher.process_update(Update.de_json(request.get_json(force=True), bot))
    return "ok"

@app.route("/")
def index():
    return "Бот работает"

dispatcher.add_handler(CommandHandler("start", start))
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

if __name__ == "__main__":
    # опциональный keep-awake
    def awake():
        try: requests.get(SELF_URL, timeout=5)
        except: pass
        t2 = threading.Timer(9*60, awake)
        t2.daemon = True
        t2.start()
    awake()

    bot.set_webhook(f"{SELF_URL}/webhook")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
