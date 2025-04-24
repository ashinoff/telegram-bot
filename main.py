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
TOKEN           = os.getenv("TOKEN")
SELF_URL        = os.getenv("SELF_URL")
ZONES_CSV_URL   = os.getenv("ZONES_CSV_URL")
REES_SHEETS_MAP = {
    p.split("=",1)[0]: p.split("=",1)[1]
    for p in os.getenv("REES_SHEETS_MAP","").split(",") if p.strip()
}
VOLS_SHEETS_URL = os.getenv("VOLS_SHEETS_URL")

bot        = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)

# — утилита для получение экспортной xlsx-ссылки —
def make_export_url(raw_url: str) -> str:
    if "/pubhtml" in raw_url:
        return raw_url.split("/pubhtml")[0] + "/pub?output=xlsx"
    if "output=xlsx" in raw_url or "export?format=xlsx" in raw_url:
        return raw_url
    m = re.search(r"/d/(?:e/)?([^/]+)", raw_url)
    if m:
        return f"https://docs.google.com/spreadsheets/d/{m.group(1)}/export?format=xlsx"
    return raw_url

# — кеш REES-таблиц —
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

# — кеш ВОЛС-данных —
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

# — загрузка и парсинг zones.csv —
def load_zones_map():
    r  = requests.get(ZONES_CSV_URL, timeout=10); r.raise_for_status()
    df = pd.read_csv(StringIO(r.content.decode("utf-8-sig")), dtype=str).fillna("")

    # имя пользователя
    if "Name" in df.columns:
        name_col = "Name"
    elif "Имя" in df.columns:
        name_col = "Имя"
    elif len(df.columns) >= 3:
        name_col = df.columns[2]
    else:
        name_col = None

    # колонка Region ТП (любой заголовок, содержащий "тп")
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

# — клавиатуры —
def main_menu(region: str, region_tp: str):
    buttons = []
    if region and region.upper() not in ("", "ALL"):
        buttons.append("Поиск")
    buttons.append("Справка")
    if region_tp:
        buttons.append("ВОЛС")
        buttons.append("СХЕМЫ 0,4 кВ")
    if region.lower() == "admin":
        buttons.append("Уведомление всем")
    # вертикально
    return ReplyKeyboardMarkup([[b] for b in buttons], resize_keyboard=True)

HELP_MENU = ReplyKeyboardMarkup([
    ["Сечение кабеля (ток, мощность)"],
    ["Номиналы ВА (ток, мощность)"],
    ["Формулы"],
    ["Назад"]
], resize_keyboard=True)

INFO_MENU = ReplyKeyboardMarkup([
    ["Информация по договору", "Информация по адресу подключения"],
    ["Информация по прибору учёта", "Назад"]
], resize_keyboard=True)

# — состояния пользователей —
user_states = {}
known_users = set()

# — handlers —
def start(update: Update, context: CallbackContext):
    uid = str(update.effective_user.id)
    try:
        info = load_zones_map().get(uid, {})
    except:
        info = {}
    update.message.reply_text("Меню:", reply_markup=main_menu(info.get("region",""), info.get("region_tp","")))

def handle_message(update: Update, context: CallbackContext):
    user_id = str(update.effective_user.id)
    text    = update.message.text.strip()

    try:
        zones = load_zones_map()
    except Exception as e:
        return update.message.reply_text(f"Ошибка загрузки зон: {e}")
    info = zones.get(user_id)
    if not info:
        return update.message.reply_text("У вас нет доступа.")

    region    = info["region"]
    region_tp = info["region_tp"]
    name      = info["name"]
    known_users.add(user_id)
    state = user_states.get(user_id, {})

    is_admin   = region.lower()=="admin"
    is_all     = region.upper()=="ALL"
    search_reg = "ALL" if (is_admin or is_all) else region

    # — рассылка —
    if state.get("mode")=="broadcast":
        for uid in known_users:
            try: bot.send_message(chat_id=int(uid), text=text)
            except: pass
        user_states[user_id]={}
        return update.message.reply_text("Рассылка выполнена.", reply_markup=main_menu(region, region_tp))

    # — Поиск по счётчику —
    if text=="Поиск":
        if not region or region.upper() in ("",):
            return update.message.reply_text("У вас нет доступа.")
        user_states[user_id]={"mode":"search"}
        return update.message.reply_text("Введите номер счётчика", reply_markup=main_menu(region, region_tp))

    # — Справка —
    if text=="Справка":
        user_states[user_id]={"mode":"help"}
        return update.message.reply_text("Справка:", reply_markup=HELP_MENU)

    # — Админская рассылка —
    if text=="Уведомление всем" and is_admin:
        user_states[user_id]={"mode":"broadcast"}
        return update.message.reply_text("Введите текст для рассылки всем:", reply_markup=main_menu(region, region_tp))

    # — ВОЛС —
    if text=="ВОЛС":
        if not region_tp:
            return update.message.reply_text("У вас нет доступа.", reply_markup=main_menu(region, region_tp))
        user_states[user_id]={"mode":"vols"}
        return update.message.reply_text(
            "Введите номер ТП (например С500 или ТП-С500):",
            reply_markup=ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True)
        )

    # — СХЕМЫ (заглушка) —
    if text=="СХЕМЫ 0,4 кВ":
        if not region_tp:
            return update.message.reply_text("У вас нет доступа.", reply_markup=main_menu(region, region_tp))
        return update.message.reply_text("Функция пока недоступна.", reply_markup=main_menu(region, region_tp))

    # — Режим справки —
    if state.get("mode")=="help":
        if text=="Сечение кабеля (ток, мощность)":
            update.message.reply_photo(photo=IMG_CABLE_URL)
        elif text=="Номиналы ВА (ток, мощность)":
            update.message.reply_photo(photo=IMG_SELECTIVITY_URL)
        elif text=="Формулы":
            update.message.reply_photo(photo=IMG_FORMULAS_URL)
        elif text=="Назад":
            user_states[user_id]={}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))
        else:
            return update.message.reply_text("Выберите пункт справки:", reply_markup=HELP_MENU)
        return update.message.reply_text("Справка:", reply_markup=HELP_MENU)

    # — Обработка поиска счётчика —
    if state.get("mode")=="search":
        norm = text.lstrip("0") or "0"
        found = matched = None
        if search_reg=="ALL":
            for reg, df in DATA_CACHE.items():
                ser      = df["Номер счетчика"].astype(str)
                norm_ser = ser.str.lstrip("0").replace("","0")
                if (norm_ser==norm).any():
                    found, matched = reg, ser[norm_ser==norm].iloc[0]
                    break
            if not found:
                return update.message.reply_text("Номер не найден ни в одном регионе.", reply_markup=main_menu(region, region_tp))
        else:
            df = DATA_CACHE.get(search_reg)
            if df is None:
                return update.message.reply_text("У вас нет доступа.")
            ser      = df["Номер счетчика"].astype(str)
            norm_ser = ser.str.lstrip("0").replace("","0")
            if not (norm_ser==norm).any():
                return update.message.reply_text("Номер не найден.", reply_markup=main_menu(region, region_tp))
            found, matched = search_reg, ser[norm_ser==norm].iloc[0]

        user_states[user_id]={
            "mode":"info","number":matched,"region":found,"region_tp":region_tp
        }
        greet = f"Принял в работу, {name}" if name else "Принял в работу"
        return update.message.reply_text(greet, reply_markup=INFO_MENU)

    # — Выдача инфо по счётчику —
    if state.get("mode")=="info":
        if text=="Назад":
            user_states[user_id]={}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))
        st  = user_states[user_id]
        num = st["number"]
        df  = DATA_CACHE.get(st["region"], pd.DataFrame())
        row = df[df["Номер счетчика"].astype(str)==num]
        if row.empty:
            return update.message.reply_text("Данные не найдены.", reply_markup=INFO_MENU)
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
        msg  = "\n".join(f"{c}: {data.get(c,'Нет данных')}" for c in cols)
        return update.message.reply_text(msg, reply_markup=INFO_MENU)

    # — Режим ВОЛС: ввод ТП —
    if state.get("mode")=="vols":
        if text=="Назад":
            user_states[user_id]={}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))

        tp_raw = text.strip().upper()
        tp     = tp_raw if tp_raw.startswith("ТП-") else f"ТП-{tp_raw}"

        ser_tp  = VOLS_DF[VOLS_TP_COL].astype(str).str.upper().str.strip()
        mask_tp = ser_tp==tp
        if not mask_tp.any():
            return update.message.reply_text("Договоров нет.", reply_markup=ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True))

        # фильтрация по region_tp
        if region_tp.upper()!="ALL":
            df_tp = VOLS_DF[mask_tp & (VOLS_DF["РЭС"].astype(str).str.strip()==region_tp)]
            if df_tp.empty:
                return update.message.reply_text("У вас нет доступа к этой зоне.", reply_markup=ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True))
        else:
            df_tp = VOLS_DF[mask_tp]

        # считаем количество строк-договоров по контрагентам
        vc = df_tp["Наименование контрагента (собственника ВОЛС)"].astype(str).value_counts()
        # строим map: label->name
        label_map = {f"{name} ({cnt} шт)": name for name, cnt in vc.items()}
        labels    = list(label_map.keys())

        user_states[user_id] = {
            "mode":          "vols_list",
            "tp":            tp,
            "df_tp":         df_tp,
            "contragents_map": label_map,
            "region_tp":     region_tp
        }

        msg = f"На ТП действует {len(df_tp)} договор(ов):\n" + "\n".join(labels)
        kb  = [[lbl] for lbl in labels] + [["Назад"]]
        return update.message.reply_text(msg, reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))

    # — Режим ВОЛС: выбор контрагента —
    if state.get("mode")=="vols_list":
        if text=="Назад":
            user_states[user_id]={}
            return update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))

        mapping = state["contragents_map"]
        if text not in mapping:
            kb = [[lbl] for lbl in mapping.keys()] + [["Назад"]]
            return update.message.reply_text("Выберите из списка или Назад.", reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))

        name = mapping[text]
        df_sel = state["df_tp"][state["df_tp"]["Наименование контрагента (собственника ВОЛС)"]==name]
        for _, row in df_sel.iterrows():
            text_block = (
                f"РЭС: {row.get('РЭС','')}\n"
                f"ТП: {row.get('Наименование ТП','')}\n"
                f"Фидер: {row.get('ФИДЕР','')}\n"
                f"ВУ: {row.get('ВУ','')}\n"
                f"Опоры: {row.get('Опоры','')}"
            )
            update.message.reply_text(text_block)

        kb = [[lbl] for lbl in mapping.keys()] + [["Назад"]]
        return update.message.reply_text("Можно выбрать другого контрагента или Назад.", reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))

    # fallback
    user_states[user_id]={}
    return update.message.reply_text("Меню:", reply_markup=main_menu(region, region_tp))


# — webhook & запуск —
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
