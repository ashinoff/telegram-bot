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

# === ENV ===
TOKEN             = os.getenv("TOKEN")
SELF_URL          = os.getenv("SELF_URL")
ZONES_CSV_URL     = os.getenv("ZONES_CSV_URL")
REES_SHEETS_MAP   = {
    p.split("=", 1)[0]: p.split("=", 1)[1]
    for p in os.getenv("REES_SHEETS_MAP", "").split(",") if p.strip()
}
VOLS_SHEETS_URL   = os.getenv("VOLS_SHEETS_URL")  # ссылка на ваш Google Sheet с ВОЛС

bot        = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)

# === утилита для превращения любых Google-ссылек в экспортную XLSX-ссылку ===
def make_export_url(raw_url: str) -> str:
    if "/pubhtml" in raw_url:
        return raw_url.split("/pubhtml")[0] + "/pub?output=xlsx"
    if "output=xlsx" in raw_url or "export?format=xlsx" in raw_url:
        return raw_url
    m = re.search(r"/d/(?:e/)?([^/]+)", raw_url)
    if m:
        sid = m.group(1)
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=xlsx"
    return raw_url

# === кеширование REES-таблиц ===
DATA_CACHE = {}
def refresh_cache():
    for region, raw_url in REES_SHEETS_MAP.items():
        try:
            url = make_export_url(raw_url)
            r   = requests.get(url, timeout=10); r.raise_for_status()
            DATA_CACHE[region] = pd.read_excel(BytesIO(r.content), dtype=str)
            print(f"[cache] Loaded {region}")
        except Exception as e:
            print(f"[cache] Error loading {region}: {e}")
    t = threading.Timer(3600, refresh_cache)
    t.daemon = True
    t.start()

# === кеширование ВОЛС - данных ===
VOLS_DF     = pd.DataFrame()
VOLS_TP_COL = None

def refresh_vols():
    global VOLS_DF, VOLS_TP_COL
    try:
        url = make_export_url(VOLS_SHEETS_URL)
        r   = requests.get(url, timeout=10); r.raise_for_status()
        df  = pd.read_excel(BytesIO(r.content), dtype=str).fillna("")
        # ищем первую колонку, где в имени встречается "тп" (рус. ТП или любая вариация)
        for c in df.columns:
            if "тп" in c.lower():
                VOLS_TP_COL = c
                break
        VOLS_DF = df
        print("[cache] Loaded VOLS")
    except Exception as e:
        print(f"[cache] Error loading VOLS: {e}")
    t = threading.Timer(3600, refresh_vols)
    t.daemon = True
    t.start()

# стартуем оба кеша
refresh_cache()
refresh_vols()

# === чтение и парсинг zones.csv с динамическим поиском колонки ТП ===
def load_zones_map():
    r = requests.get(ZONES_CSV_URL, timeout=10); r.raise_for_status()
    df = pd.read_csv(StringIO(r.content.decode("utf-8-sig")), dtype=str).fillna("")

    # определяем колонку с именем пользователя
    if "Name" in df.columns:
        name_col = "Name"
    elif "Имя" in df.columns:
        name_col = "Имя"
    elif len(df.columns) >= 3:
        name_col = df.columns[2]
    else:
        name_col = None

    # ищем колонку Region ТП (любой заголовок, содержащий "тп")
    tp_cols = [c for c in df.columns if "тп" in c.lower()]
    region_tp_col = tp_cols[0] if tp_cols else None

    zones = {}
    for _, row in df.iterrows():
        uid = str(row.get("ID", "")).strip()
        if not uid:
            continue
        region    = str(row.get("Region", "")).strip()
        name      = str(row.get(name_col, "")).strip() if name_col else ""
        region_tp = str(row.get(region_tp_col, "")).strip() if region_tp_col else ""
        zones[uid] = {
            "region":    region,
            "name":      name,
            "region_tp": region_tp
        }
    return zones

# === клавиатуры ===
def main_menu(region: str, region_tp: str):
    buttons = []
    # кнопка "Поиск" только если есть обычный регистрированный регион
    if region and region.upper() not in ("", "ALL"):
        buttons.append("Поиск")
    buttons.append("Справочная информация")
    # кнопки ВОЛС и СХЕМЫ 0,4 кВ только если есть region_tp
    if region_tp:
        buttons.append("ВОЛС")
        buttons.append("СХЕМЫ 0,4 кВ")
    # админская рассылка
    if region.lower() == "admin":
        buttons.append("Уведомление всем")
    return ReplyKeyboardMarkup([buttons], resize_keyboard=True)

HELP_MENU = ReplyKeyboardMarkup([
    ["Поиск", "Справочная информация"],
    ["Сечение кабеля (ток, мощность)"],
    ["Номиналы ВА (ток, мощность)"],
    ["Формулы"],
    ["Назад"]
], resize_keyboard=True)

INFO_MENU = ReplyKeyboardMarkup([
    ["Информация по договору", "Информация по адресу подключения"],
    ["Информация по прибору учёта", "Назад"]
], resize_keyboard=True)

# === Telegram-обработчики ===
user_states = {}   # user_id -> state dict
known_users = set()

def start(update: Update, context: CallbackContext):
    user_id = str(update.effective_user.id)
    try:
        zones = load_zones_map()
        info  = zones.get(user_id, {})
    except:
        info = {}
    region    = info.get("region", "")
    region_tp = info.get("region_tp", "")
    update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))

def handle_message(update: Update, context: CallbackContext):
    user_id = str(update.effective_user.id)
    text    = update.message.text.strip()

    # подгружаем зоны
    try:
        zones = load_zones_map()
    except Exception as e:
        return update.message.reply_text(f"Ошибка загрузки зон: {e}")
    info = zones.get(user_id)
    if not info:
        return update.message.reply_text("У вас нет прав или не назначены в РЭС.")

    region    = info["region"]
    region_tp = info["region_tp"]
    user_name = info["name"]
    known_users.add(user_id)
    state = user_states.get(user_id, {})

    is_admin   = region.lower() == "admin"
    is_all     = region.upper() == "ALL"
    search_reg = "ALL" if (is_admin or is_all) else region

    # режим рассылки (admin)
    if state.get("mode") == "broadcast":
        for uid in known_users:
            try: bot.send_message(chat_id=int(uid), text=text)
            except: pass
        user_states[user_id] = {}
        return update.message.reply_text("Рассылка выполнена.", reply_markup=main_menu(region, region_tp))

    # команды главного меню
    if text == "Поиск":
        user_states[user_id] = {"mode": "search"}
        return update.message.reply_text("Введите номер счётчика", reply_markup=main_menu(region, region_tp))

    if text == "Справочная информация":
        user_states[user_id] = {"mode": "help"}
        return update.message.reply_text("Справка:", reply_markup=HELP_MENU)

    if text == "Уведомление всем" and is_admin:
        user_states[user_id] = {"mode": "broadcast"}
        return update.message.reply_text("Введите текст для рассылки всем:", reply_markup=main_menu(region, region_tp))

    if text == "ВОЛС":
        user_states[user_id] = {"mode": "vols"}
        return update.message.reply_text(
            "Введите номер ТП (например С500 или ТП-С500):",
            reply_markup=ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True)
        )

    if text == "СХЕМЫ 0,4 кВ":
        return update.message.reply_text(
            "Функция 'СХЕМЫ 0,4 кВ' пока недоступна.",
            reply_markup=main_menu(region, region_tp)
        )

    # справка
    if state.get("mode") == "help":
        if text == "Сечение кабеля (ток, мощность)":
            update.message.reply_photo(photo=IMG_CABLE_URL)
        elif text == "Номиналы ВА (ток, мощность)":
            update.message.reply_photo(photo=IMG_SELECTIVITY_URL)
        elif text == "Формулы":
            update.message.reply_photo(photo=IMG_FORMULAS_URL)
        elif text == "Назад":
            user_states[user_id] = {}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))
        else:
            return update.message.reply_text("Выберите пункт справки:", reply_markup=HELP_MENU)
        return update.message.reply_text("Справка:", reply_markup=HELP_MENU)

    # поиск по счетчику
    if state.get("mode") == "search":
        norm = text.lstrip("0") or "0"
        found = matched = None
        if search_reg == "ALL":
            for reg, df in DATA_CACHE.items():
                ser      = df["Номер счетчика"].astype(str)
                norm_ser = ser.str.lstrip("0").replace("", "0")
                if (norm_ser == norm).any():
                    found, matched = reg, ser[norm_ser == norm].iloc[0]
                    break
            if not found:
                return update.message.reply_text("Номер не найден ни в одном регионе.", reply_markup=main_menu(region, region_tp))
        else:
            df = DATA_CACHE.get(search_reg)
            if df is None:
                return update.message.reply_text(f"Таблица для «{search_reg}» ещё не загружена.")
            ser      = df["Номер счетчика"].astype(str)
            norm_ser = ser.str.lstrip("0").replace("", "0")
            if not (norm_ser == norm).any():
                return update.message.reply_text("Номер не найден. Проверьте ввод.", reply_markup=main_menu(region, region_tp))
            found, matched = search_reg, ser[norm_ser == norm].iloc[0]

        user_states[user_id] = {
            "mode":      "info",
            "number":    matched,
            "region":    found,
            "region_tp": region_tp
        }
        greet = f"Принял в работу, {user_name}" if user_name else "Принял в работу"
        return update.message.reply_text(greet, reply_markup=INFO_MENU)

    # выдача инфо по счетчику
    if state.get("mode") == "info":
        if text == "Назад":
            user_states[user_id] = {}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))
        st   = user_states[user_id]
        num  = st["number"]
        rgn  = st["region"]
        df   = DATA_CACHE.get(rgn, pd.DataFrame())
        row  = df[df["Номер счетчика"].astype(str) == num]
        if row.empty:
            return update.message.reply_text("Данные не найдены.", reply_markup=INFO_MENU)
        if text == "Информация по договору":
            cols = ["ТУ","Номер ТУСТЕК","Номер ТУ","ЛС / ЛС СТЕК","Наименование договора","Вид потребителя","Субабонент"]
        elif text == "Информация по адресу подключения":
            cols = ["Сетевой участок","Населенный пункт","Улица","Дом","ТП"]
        else:
            cols = [
                "Номер счетчика","Состояние ТУ","Максимальная мощность","Вид счетчика","Фазность",
                "Госповерка счетчика","Межповерочный интервал ПУ","Окончание срок поверки",
                "Проверка схемы дата","Последнее активное событие дата",
                "Первичный ток ТТ (А)","Госповерка ТТ (А)","Межповерочный интервал ТТ"
            ]
        data = row.iloc[0]
        msg  = "\n".join(f"{c}: {data.get(c,'Нет данных')}" for c in cols)
        return update.message.reply_text(msg, reply_markup=INFO_MENU)

    # режим ВОЛС: ввод ТП
    if state.get("mode") == "vols":
        if text == "Назад":
            user_states[user_id] = {}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))

        tp_raw = text.strip().upper()
        tp     = tp_raw if tp_raw.startswith("ТП-") else f"ТП-{tp_raw}"

        if not VOLS_TP_COL or VOLS_TP_COL not in VOLS_DF.columns:
            return update.message.reply_text(
                "Ошибка: не найдена колонка с ТП в данных ВОЛС.",
                reply_markup=main_menu(region, region_tp)
            )

        ser  = VOLS_DF[VOLS_TP_COL].astype(str).str.upper().str.strip()
        mask = ser == tp
        if not mask.any():
            return update.message.reply_text(
                "Действующих договоров ВОЛС нет.",
                reply_markup=ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True)
            )

        df_tp = VOLS_DF[mask]
        contrs = df_tp["Наименование контрагента (собственника ВОЛС)"].astype(str).unique().tolist()
        user_states[user_id] = {
            "mode":        "vols_list",
            "tp":          tp,
            "contragents": contrs,
            "region_tp":   region_tp
        }

        msg = f"На ТП действует {len(contrs)} договор(ов):\n" + "\n".join(f"контрагент {c}" for c in contrs)
        kb  = [[c] for c in contrs] + [["Назад"]]
        return update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))

    # режим выбора контрагента ВОЛС
    if state.get("mode") == "vols_list":
        if text == "Назад":
            user_states[user_id] = {}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))

        chosen = text
        if chosen not in state["contragents"]:
            kb = [[c] for c in state["contragents"]] + [["Назад"]]
            return update.message.reply_text(
                "Выберите контрагента из списка или нажмите Назад.",
                reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True)
            )

        df_tp  = VOLS_DF[VOLS_DF[VOLS_TP_COL].astype(str).str.upper().str.strip() == state["tp"]]
        df_sel = df_tp[df_tp["Наименование контрагента (собственника ВОЛС)"] == chosen]
        for _, row in df_sel.iterrows():
            text_block = (
                f"РЭС: {row.get('РЭС','')}\n"
                f"Наименование ТП: {row.get('Наименование ТП','')}\n"
                f"Фидер: {row.get('ФИДЕР','')}\n"
                f"ВУ: {row.get('ВУ','')}\n"
                f"Опоры: {row.get('Опоры','')}"
            )
            update.message.reply_text(text_block)

        kb = [[c] for c in state["contragents"]] + [["Назад"]]
        return update.message.reply_text(
            "Можно выбрать другого контрагента или нажать Назад.",
            reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True)
        )

    # fallback: сброс состояния и меню
    user_states[user_id] = {}
    update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))

# === webhook & запуск ===
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
    # keep-alive ping
    def awake():
        try: requests.get(SELF_URL, timeout=5)
        except: pass
        t = threading.Timer(9*60, awake)
        t.daemon = True
        t.start()
    awake()
    bot.set_webhook(f"{SELF_URL}/webhook")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
