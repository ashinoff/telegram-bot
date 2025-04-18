import telebot
import pandas as pd
import requests
from io import BytesIO
import os

TOKEN = os.environ.get('TOKEN')
FILE_URL = 'https://docs.google.com/uc?export=download&id=1s2zMtwdMaHJSOCXflfw4puL5kzIbiObb'

bot = telebot.TeleBot(TOKEN)

def load_excel():
    response = requests.get(FILE_URL)
    return pd.read_excel(BytesIO(response.content))

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "Привет! Введи номер из первого столбца (№ п/п), и я покажу данные по этой строке.")

@bot.message_handler(func=lambda msg: msg.text.strip().isdigit())
def get_row(message):
    number = int(message.text.strip())
    df = load_excel()
    df.columns = [str(col).strip() for col in df.columns]
    first_column = df.columns[0]

    row_match = df[df[first_column] == number]

    if row_match.empty:
        bot.send_message(message.chat.id, "Такой строки не найдено.")
    else:
        row = row_match.iloc[0]
        response = ""
        for col in df.columns[1:]:
            response += f"{col}: {row[col]}
"
        bot.send_message(message.chat.id, response.strip())

bot.polling()