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
    p.split("=",1)[0]: p.split("=",1)[1]
    for p in os.getenv("REES_SHEETS_MAP","").split(",") if p.strip()
}
VOLS_SHEETS_URL   = os.getenv("VOLS_SHEETS_URL")

bot        = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)

# === Google Drive IDs для справки ===
BASE_DRIVE_URL       = "https://drive.google.com/uc?export=download&id="
IMG_FORMULAS_URL     = BASE_DRIVE_URL + "1StUq8JSdpwU1QvJJ6F3W3dHZnReF6kt8"
IMG_CABLE_URL        = BASE_DRIVE_URL + "11LaH-BvqtUPj2wTQ31wl-1Qrs2aRGb0I"
IMG_SELECTIVITY_URL  = BASE_DRIVE_URL + "11q0orVtOJ_UTk5UVLEn5yGUOeCsWQkaX"

# утилита для получения экспорт-URL xlsx
def make_export_url(raw_url: str) -> str:
    if "/pubhtml" in raw_url:
        return raw_url.split("/pubhtml")[0] + "/pub?output=xlsx"
    if "output=xlsx" in raw_url or "export?format=xlsx" in raw_url:
        return raw_url
    m = re.search(r"/d/(?:e/)?([^/]+)", raw_url)
    if m:
        return f"https://docs.google.com/spreadsheets/d/{m.group(1)}/export?format=xlsx"
    return raw_url

# кеш REES-таблиц
DATA_CACHE = {}
def refresh_cache():
    for region, raw_url in REES_SHEETS_MAP.items():
        try:
            url = make_export_url(raw_url)
            r   = requests.get(url, timeout=10); r.raise_for_status()
            DATA_CACHE[region] = pd.read_excel(BytesIO(r.content), dtype=str)
        except Exception as e:
            print(f"[cache] Error loading {region}: {e}")
    t = threading.Timer(3600, refresh_cache); t.daemon = True; t.start()

# кеш ВОЛС-данных
VOLS_DF     = pd.DataFrame()
VOLS_TP_COL = None
def refresh_vols():
    global VOLS_DF, VOLS_TP_COL
    try:
        url = make_export_url(VOLS_SHEETS_URL)
        r   = requests.get(url, timeout=10); r.raise_for_status()
        df  = pd.read_excel(BytesIO(r.content), dtype=str).fillna("")
        for c in df.columns:
            if "тп" in c.lower():
                VOLS_TP_COL = c
                break
        VOLS_DF = df
    except Exception as e:
        print(f"[cache] Error loading VOLS: {e}")
    t = threading.Timer(3600, refresh_vols); t.daemon = True; t.start()

refresh_cache()
refresh_vols()

# загрузка зон доступа
def load_zones_map():
    r  = requests.get(ZONES_CSV_URL, timeout=10); r.raise_for_status()
    df = pd.read_csv(StringIO(r.content.decode("utf-8-sig")), dtype=str).fillna("")

    # колонка имени
    if "Name" in df.columns:
        name_col = "Name"
    elif "Имя" in df.columns:
        name_col = "Имя"
    elif len(df.columns) >= 3:
        name_col = df.columns[2]
    else:
        name_col = None

    # колонка Region ТП
    tp_cols = [c for c in df.columns if "тп" in c.lower()]
    region_tp_col = tp_cols[0] if tp_cols else None

    zones = {}
    for _, row in df.iterrows():
        uid = str(row.get("ID","")).strip()
        if not uid:
            continue
        zones[uid] = {
            "region":    str(row.get("Region","")).strip(),
            "name":      str(row.get(name_col,"")).strip() if name_col else "",
            "region_tp": str(row.get(region_tp_col,"")).strip() if region_tp_col else ""
        }
    return zones

# клавиатуры
def main_menu(region: str, region_tp: str):
    buttons = []
    if region and region.upper() not in ("","ALL"):
        buttons.append("Поиск по прибору учета")
    buttons.append("Справка")
    if region_tp:
        buttons.append("ВОЛС")
        buttons.append("СХЕМЫ 0,4 кВ")
    if region.lower()=="admin":
        buttons.append("Уведомление всем")
    return ReplyKeyboardMarkup([[b] for b in buttons], resize_keyboard=True)

HELP_MENU = ReplyKeyboardMarkup([
    ["Сечение кабеля (ток, мощность)"],
    ["Селективность (ток, мощность)"],
    ["Формулы"],
    ["Назад"]
], resize_keyboard=True)

INFO_MENU = ReplyKeyboardMarkup([
    ["Информация по договору", "Информация по подключению"],
    ["Информация по прибору учета", "Назад"]
], resize_keyboard=True)

VOLS_MENU = ReplyKeyboardMarkup([
    ["Поиск по ТП"],
    ["Поиск по контрагенту"],
    ["Назад"]
], resize_keyboard=True)

# состояния
user_states = {}
known_users  = set()

# handlers
def start(update: Update, context: CallbackContext):
    uid = str(update.effective_user.id)
    try:
        info = load_zones_map().get(uid,{})
    except:
        info = {}
    update.message.reply_text("Меню:", reply_markup=main_menu(info.get("region",""), info.get("region_tp","")))

def handle_message(update: Update, context: CallbackContext):
    uid  = str(update.effective_user.id)
    txt  = update.message.text.strip()
    try:
        zones = load_zones_map()
    except Exception as e:
        return update.message.reply_text(f"Ошибка загрузки зон: {e}")
    info = zones.get(uid)
    if not info:
        return update.message.reply_text("У вас нет доступа.")

    region    = info["region"]
    region_tp = info["region_tp"]
    name      = info["name"]
    known_users.add(uid)
    state     = user_states.get(uid, {})

    is_admin   = region.lower()=="admin"
    is_all     = region.upper()=="ALL"
    search_reg = "ALL" if (is_admin or is_all) else region

    # рассылка
    if state.get("mode")=="broadcast":
        for u in known_users:
            try: bot.send_message(chat_id=int(u), text=txt)
            except: pass
        user_states[uid]={}
        return update.message.reply_text("Рассылка выполнена.", reply_markup=main_menu(region,region_tp))

    # Поиск по прибору учета
    if txt=="Поиск по прибору учета":
        if not region or region.upper()=="":
            return update.message.reply_text("У вас нет доступа.")
        user_states[uid]={"mode":"search"}
        return update.message.reply_text("Введите номер счетчика:", reply_markup=main_menu(region,region_tp))

    # Справка
    if txt=="Справка":
        user_states[uid]={"mode":"help"}
        return update.message.reply_text("Справка:", reply_markup=HELP_MENU)

    # Админская рассылка
    if txt=="Уведомление всем" and is_admin:
        user_states[uid]={"mode":"broadcast"}
        return update.message.reply_text("Введите текст для рассылки всем:", reply_markup=main_menu(region,region_tp))

    # ВОЛС: главное подменю
    if txt=="ВОЛС":
        if not region_tp:
            return update.message.reply_text("У вас нет доступа.", reply_markup=main_menu(region,region_tp))
        user_states[uid]={"mode":"vols_menu"}
        return update.message.reply_text("Меню ВОЛС:", reply_markup=VOLS_MENU)

    # СХЕМЫ (заглушка)
    if txt=="СХЕМЫ 0,4 кВ":
        if not region_tp:
            return update.message.reply_text("У вас нет доступа.", reply_markup=main_menu(region,region_tp))
        return update.message.reply_text("Функция пока недоступна.", reply_markup=main_menu(region,region_tp))

    # режим справки
    if state.get("mode")=="help":
        if txt=="Сечение кабеля (ток, мощность)":
            update.message.reply_photo(photo=IMG_CABLE_URL)
        elif txt=="Селективность (ток, мощность)":
            update.message.reply_photo(photo=IMG_SELECTIVITY_URL)
        elif txt=="Формулы":
            update.message.reply_photo(photo=IMG_FORMULAS_URL)
        elif txt=="Назад":
            user_states[uid]={}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region,region_tp))
        else:
            return update.message.reply_text("Выберите пункт справки:", reply_markup=HELP_MENU)
        return update.message.reply_text("Справка:", reply_markup=HELP_MENU)

    # поиск по счетчику
    if state.get("mode")=="search":
        norm = txt.strip().lstrip("0") or "0"
        found = matched = None
        if search_reg=="ALL":
            for rgn, df in DATA_CACHE.items():
                ser      = df["Номер счетчика"].astype(str)
                norm_ser = ser.str.lstrip("0").replace("","0")
                if (norm_ser==norm).any():
                    found, matched = rgn, ser[norm_ser==norm].iloc[0]
                    break
            if not found:
                return update.message.reply_text("Номер не найден ни в одном регионе.", reply_markup=main_menu(region,region_tp))
        else:
            df = DATA_CACHE.get(search_reg)
            if df is None:
                return update.message.reply_text("У вас нет доступа.")
            ser      = df["Номер счетчика"].astype(str)
            norm_ser = ser.str.lstrip("0").replace("","0")
            if not (norm_ser==norm).any():
                return update.message.reply_text("Номер не найден.", reply_markup=main_menu(region,region_tp))
            found, matched = search_reg, ser[norm_ser==norm].iloc[0]

        user_states[uid]={"mode":"info","number":matched,"region":found,"region_tp":region_tp}
        greet = f"Принял в работу, {name}" if name else "Принял в работу"
        return update.message.reply_text(greet, reply_markup=INFO_MENU)

    # выдача инфо по счетчику
    if state.get("mode")=="info":
        if txt=="Назад":
            user_states[uid]={}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region,region_tp))
        st  = user_states[uid]
        row = DATA_CACHE.get(st["region"], pd.DataFrame())
        row = row[row["Номер счетчика"].astype(str)==st["number"]]
        if row.empty:
            return update.message.reply_text("Данные не найдены.", reply_markup=INFO_MENU)
        if txt=="Информация по договору":
            cols = [
                "Номер счетчика","ТУ","Номер ТУСТЕК","Номер ТУ",
                "ЛС / ЛС СТЕК","Наименование договора","Вид потребителя","Субабонент"
            ]
        elif txt=="Информация по подключению":
            cols = [
                "Номер счетчика","Сетевой участок","Населенный пункт",
                "Улица","Дом","Подстанция","Фидер10","ТП"
            ]
        else:  # прибор учета
            cols = [
                "Номер счетчика","Максимальная мощность","Вид счетчика","Фазность",
                "Госповерка счетчика","Межповерочный интервал ПУ","Окончание срок поверки",
                "Проверка схемы дата","Последнее активное событие дата",
                "Тип ТТ","Первичный ток ТТ",
                "Заводской номер ТТ (А)","Заводской номер ТТ (В)","Заводской номер ТТ (С)",
                "Госповерка ТТ","Межповерочный интервал ТТ","Окончание срок поверки ТТ",
                "Тип ТН","Заводской номер ТН","Госповерка ТН",
                "Межповерочный интервал ТН","Окончание срок поверки ТН"
            ]
        data = row.iloc[0]
        lines = []
        for c in cols:
            v = data.get(c)
            if pd.notna(v) and str(v).strip() not in ("","nan"):
                lines.append(f"{c}: {v}")
        return update.message.reply_text("\n".join(lines), reply_markup=INFO_MENU)

    # ВОЛС: меню выбора
    if state.get("mode")=="vols_menu":
        if txt=="Назад":
            user_states[uid]={}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region,region_tp))
        if txt=="Поиск по ТП":
            user_states[uid]={"mode":"vols_tp_input","region_tp":region_tp}
            return update.message.reply_text("Введите номер ТП:", reply_markup=ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True))
        if txt=="Поиск по контрагенту":
            user_states[uid]={"mode":"vols_provider_input","region_tp":region_tp}
            return update.message.reply_text("Введите имя контрагента:", reply_markup=ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True))

    # ВОЛС: поиск по провайдерам
    if state.get("mode")=="vols_provider_input":
        if txt=="Назад":
            user_states[uid]={"mode":"vols_menu","region_tp":region_tp}
            return update.message.reply_text("Меню ВОЛС:", reply_markup=VOLS_MENU)
        provider = txt.strip().lower()
        df_p = VOLS_DF[
            VOLS_DF["Наименование контрагента (собственника ВОЛС)"]
                .fillna("").str.lower().str.contains(provider)
        ]
        if region_tp.upper()!="ALL":
            df_p = df_p[df_p["РЭС"].astype(str).str.strip()==region_tp]
        if df_p.empty:
            return update.message.reply_text("Контрагент не найден.", reply_markup=ReplyKeyboardMarkup([["Назад","Новый поиск"]], resize_keyboard=True))
        vc = df_p[VOLS_TP_COL].astype(str).value_counts()
        label_map = {f"{tp} ({cnt} шт)": tp for tp,cnt in vc.items()}
        buttons   = [[lbl] for lbl in label_map] + [["Новый поиск"],["Назад"]]
        user_states[uid] = {"mode":"vols_provider_list","label_map":label_map,"df_p":df_p,"region_tp":region_tp}
        return update.message.reply_text(f"Найдено договоров: {len(df_p)}", reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True))

    # ВОЛС: список провайдеров
    if state.get("mode")=="vols_provider_list":
        if txt=="Новый поиск":
            user_states[uid]={"mode":"vols_provider_input","region_tp":region_tp}
            return update.message.reply_text("Введите имя контрагента:", reply_markup=ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True))
        if txt=="Назад":
            user_states[uid]={"mode":"vols_menu","region_tp":region_tp}
            return update.message.reply_text("Меню ВОЛС:", reply_markup=VOLS_MENU)
        lm = state.get("label_map",{})
        if txt in lm:
            tp = lm[txt]
            df_sel = state["df_p"][state["df_p"][VOLS_TP_COL].astype(str)==tp]
            update.message.reply_text(f"{tp}: {len(df_sel)} договор(ов)")
            return update.message.reply_text("Новый поиск или Назад?", reply_markup=ReplyKeyboardMarkup([["Новый поиск"],["Назад"]], resize_keyboard=True))

    # fallback
    user_states[uid]={}
    return update.message.reply_text("Меню:", reply_markup=main_menu(region,region_tp))


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
    def awake():
        try: requests.get(SELF_URL, timeout=5)
        except: pass
        t = threading.Timer(9*60, awake); t.daemon = True; t.start()
    awake()
    bot.set_webhook(f"{SELF_URL}/webhook")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
