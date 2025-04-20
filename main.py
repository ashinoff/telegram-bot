import os
import csv
import datetime
import pandas as pd
import requests
from io import StringIO
from flask import Flask, request, send_file
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackContext

app = Flask(__name__)

TOKEN         = os.getenv("TOKEN")
SELF_URL      = os.getenv("SELF_URL")
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL")    # CSV‑URL таблицы зон (ID,Region)
REES_SHEETS_MAP = {
    p.split("=",1)[0]: p.split("=",1)[1]
    for p in os.getenv("REES_SHEETS_MAP","").split(",") if p
}

bot        = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)
user_states = {}

LOGS_FILE = "logs.csv"
if not os.path.exists(LOGS_FILE):
    with open(LOGS_FILE, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["user_id", "timestamp", "number"])

def load_zones_map():
    r = requests.get(ZONES_CSV_URL); r.raise_for_status()
    df = pd.read_csv(StringIO(r.text), dtype=str)
    return dict(zip(df["ID"].str.strip(), df["Region"].str.strip()))

def log_request(user_id, number):
    ts = datetime.datetime.now().isoformat()
    with open(LOGS_FILE, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([user_id, ts, number])

def load_data(excel_url):
    r = requests.get(excel_url); r.raise_for_status()
    return pd.read_excel(pd.io.common.BytesIO(r.content), dtype=str)

def start(update: Update, context: CallbackContext):
    update.message.reply_text("Введите номер счётчика или ЛС")

def handle_message(update: Update, context: CallbackContext):
    user_id  = str(update.message.from_user.id)
    text_raw = update.message.text.strip()

    # выбор типа информации
    if text_raw in ("Информация по договору", "Информация по адресу подключения", "Информация по прибору учёта"):
        st = user_states.get(user_id)
        if not st:
            return update.message.reply_text("Сначала введите номер счётчика или ЛС")
        return send_info(update, st["number"], text_raw, st["region"])

    # ввод номера
    zones  = load_zones_map()
    region = zones.get(user_id)
    if not region:
        return update.message.reply_text("У вас нет прав или вы не назначены ни в один РЭС.")

    norm_input = text_raw.lstrip("0") or "0"
    found_reg, matched = None, None

    if region.upper() == "ALL":
        for reg, url in REES_SHEETS_MAP.items():
            df  = load_data(url)
            ser = df["Номер счетчика"].astype(str)
            norm = ser.str.lstrip("0").replace("", "0")
            mask = norm == norm_input
            if mask.any():
                found_reg = reg
                matched   = ser[mask].iloc[0]
                break
        if not found_reg:
            return update.message.reply_text("Номер не найден ни в одном регионе.")
    else:
        url = REES_SHEETS_MAP.get(region)
        df  = load_data(url)
        ser = df["Номер счетчика"].astype(str)
        norm = ser.str.lstrip("0").replace("", "0")
        mask = norm == norm_input
        if not mask.any():
            return update.message.reply_text("Номер не найден. Проверьте ввод.")
        found_reg, matched = region, ser[mask].iloc[0]

    log_request(user_id, matched)

    user_states[user_id] = {"number": matched, "region": found_reg}
    kb = [
        ["Информация по договору"],
        ["Информация по адресу подключения"],
        ["Информация по прибору учёта"],
    ]
    reply_markup = ReplyKeyboardMarkup(kb, resize_keyboard=True)
    update.message.reply_text("Принял в работу. Что ищем?", reply_markup=reply_markup)

def send_info(update: Update, number: str, info_type: str, region: str):
    df  = load_data(REES_SHEETS_MAP[region])
    row = df[df["Номер счетчика"].astype(str) == number]
    if row.empty:
        return update.message.reply_text("Данные не найдены.")
    if info_type == "Информация по договору":
        cols = ["ТУ","Номер ТУСТЕК","Номер ТУ","ЛС / ЛС СТЕК","Наименование договора","Вид потребителя","Субабонент"]
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
    msg  = "\n".join(f"{c}: {data.get(c,'Нет данных')}" for c in cols)
    update.message.reply_text(msg)

@app.route("/webhook", methods=["POST"])
def webhook():
    upd = Update.de_json(request.get_json(force=True), bot)
    dispatcher.process_update(upd)
    return "ok"

@app.route("/download_logs", methods=["GET"])
def download_logs():
    return send_file(LOGS_FILE, as_attachment=True, attachment_filename="logs.csv", mimetype="text/csv")

dispatcher.add_handler(CommandHandler("start", start))
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

if __name__ == "__main__":
    bot.set_webhook(f"{SELF_URL}/webhook")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
