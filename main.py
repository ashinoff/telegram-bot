import os
import threading
import pandas as pd
import requests
from io import BytesIO, StringIO
from flask import Flask, request
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackContext

app = Flask(__name__)

# === ENV ===
TOKEN           = os.getenv("TOKEN")
SELF_URL        = os.getenv("SELF_URL")
ZONES_CSV_URL   = os.getenv("ZONES_CSV_URL")
REES_SHEETS_MAP = {
    p.split("=",1)[0]: p.split("=",1)[1]
    for p in os.getenv("REES_SHEETS_MAP","").split(",") if p
}

bot        = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)

# состояние пользователей и зарегистрированные чаты
user_states = {}
known_users = set()

# ========== ПРЕДЗАГРУЗКА РЭС-ТАБЛИЦ ==========
DATA_CACHE = {}
def refresh_cache():
    for region, url in REES_SHEETS_MAP.items():
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            DATA_CACHE[region] = pd.read_excel(BytesIO(r.content), dtype=str)
            print(f"[cache] Loaded {region}")
        except Exception as e:
            print(f"[cache] Error loading {region}: {e}")
    # планируем самовызыв через час
    t = threading.Timer(3600, refresh_cache)
    t.daemon = True
    t.start()

# сразу запускаем первый прогон
refresh_cache()

# ========== КЛАВИАТУРЫ ==========
def main_menu(region):
    items = ["Поиск", "Справочная информация"]
    if region.lower() == "admin":
        items.append("Уведомление всем")
    return ReplyKeyboardMarkup([items], resize_keyboard=True)

HELP_MENU = ReplyKeyboardMarkup([
    ["Поиск", "Справочная информация"],
    ["Сечение кабеля"],
    ["Селективность"],
    ["Формулы"],
    ["Назад"]
], resize_keyboard=True)

INFO_MENU = ReplyKeyboardMarkup([
    ["Поиск", "Справочная информация"],
    ["Информация по договору"],
    ["Информация по адресу подключения"],
    ["Информация по прибору учёта"]
], resize_keyboard=True)

# ========== ЗАГРУЗКА ЗОН ==========
def load_zones_map():
    r = requests.get(ZONES_CSV_URL, timeout=10); r.raise_for_status()
    text = r.content.decode("utf-8-sig")
    df = pd.read_csv(StringIO(text), dtype=str)
    # ищем колонку с именем
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

# ========== ХЕНДЛЕРЫ ==========
def start(update: Update, context: CallbackContext):
    update.message.reply_text("Меню:", reply_markup=main_menu(""))

def handle_message(update: Update, context: CallbackContext):
    user_id = str(update.effective_user.id)
    text    = update.message.text.strip()

    # зоны + имя
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

    # режим рассылки
    if state.get("mode") == "broadcast":
        msg = text
        for uid in known_users:
            try: bot.send_message(chat_id=int(uid), text=msg)
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

    if text == "Уведомление всем" and region.lower()=="admin":
        user_states[user_id] = {"mode": "broadcast"}
        return update.message.reply_text("Введите текст для рассылки всем:", reply_markup=main_menu(region))

    # подменю справки
    if state.get("mode") == "help":
        if text == "Сечение кабеля":
            with open("сечение.jpg","rb") as f: update.message.reply_photo(photo=f)
        elif text == "Селективность":
            with open("селективность.jpg","rb") as f: update.message.reply_photo(photo=f)
        elif text == "Формулы":
            with open("формулы.jpg","rb") as f: update.message.reply_photo(photo=f)
        elif text == "Назад":
            user_states[user_id] = {}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region))
        else:
            return update.message.reply_text("Выберите пункт справки:", reply_markup=HELP_MENU)
        return update.message.reply_text("Справка:", reply_markup=HELP_MENU)

    # ввод номера
    if text not in ("Информация по договору","Информация по адресу подключения","Информация по прибору учёта"):
        norm = text.lstrip("0") or "0"
        found, matched = None, None
        if region.upper()=="ALL":
            for reg, df in DATA_CACHE.items():
                ser = df["Номер счетчика"].astype(str)
                norm_ser = ser.str.lstrip("0").replace("","0")
                if (norm_ser==norm).any():
                    found, matched = reg, ser[norm_ser==norm].iloc[0]
                    break
            if not found:
                return update.message.reply_text("Номер не найден ни в одном регионе.", reply_markup=main_menu(region))
        else:
            df = DATA_CACHE.get(region)
            if df is None:
                return update.message.reply_text(f"Таблица для региона «{region}» ещё не загружена.")
            ser = df["Номер счетчика"].astype(str)
            norm_ser = ser.str.lstrip("0").replace("","0")
            if not (norm_ser==norm).any():
                return update.message.reply_text("Номер не найден. Проверьте ввод.", reply_markup=main_menu(region))
            found, matched = region, ser[norm_ser==norm].iloc[0]

        user_states[user_id] = {"mode":"info","number":matched,"region":found}
        greet = f"Принял в работу, {user_name}" if user_name else "Принял в работу"
        return update.message.reply_text(greet, reply_markup=INFO_MENU)

    # информация по счётчику
    st = user_states.get(user_id,{})
    number, region_info = st.get("number"), st.get("region")
    if number and region_info:
        df = DATA_CACHE.get(region_info)
        row = df[df["Номер счетчика"].astype(str)==number]
        if row.empty:
            return update.message.reply_text("Данные не найдены.", reply_markup=main_menu(region))
        if text=="Информация по договору":
            cols = ["ТУ","Номер ТУСТЕК","Номер ТУ","ЛС / ЛС СТЕК","Наименование договора","Вид потребителя","Субабонент"]
        elif text=="Информация по адресу подключения":
            cols = ["Сетевой участок","Населенный пункт","Улица","Дом","ТП"]
        else:
            cols = ["Номер счетчика","Состояние ТУ","Максимальная мощность","Вид счетчика","Фазность",
                    "Госповерка счетчика","Межповерочный интервал ПУ","Окончание срок поверки",
                    "Проверка схемы дата","Последнее активное событие дата",
                    "Первичный ток ТТ (А)","Госповерка ТТ (А)","Межповерочный интервал ТТ"]
        data = row.iloc[0]
        msg = "\n".join(f"{c}: {data.get(c,'Нет данных')}" for c in cols)
        return update.message.reply_text(msg, reply_markup=main_menu(region))

    # fallback
    update.message.reply_text("Меню:", reply_markup=main_menu(region))

# ========== WEBHOOK ==========
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
    # keep-awake (если хотите)
    def awake():
        try: requests.get(SELF_URL, timeout=5)
        except: pass
        t = threading.Timer(9*60, awake)
        t.daemon = True
        t.start()
    awake()

    bot.set_webhook(f"{SELF_URL}/webhook")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
