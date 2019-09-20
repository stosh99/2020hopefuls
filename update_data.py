import json
from textblob import TextBlob
import pandas as pd
from models import Database
import time

df_states = pd.read_csv('static/states.csv')

db = Database()


def calc_sentiment(text, full_text, q_text):
    sent = 0
    count = 1
    if len(full_text) > 5:
        sent += TextBlob(full_text).sentiment.polarity
    elif len(text) > 5:
        sent += TextBlob(text).sentiment.polarity
        count += 1
    if len(full_text) > 5:
        sent = TextBlob(q_text).sentiment.polarity
        count += 1
    avg_sent = sent / count
    return avg_sent


def get_candidates(row):
    text = row.text
    q_text = row.q_text
    if row.q_text == '' or pd.isnull(row.q_text):
        q_text = ''
    if row.text == '' or pd.isnull(row.text):
        if row.full_text == '' or pd.isnull(row.text):
            text = ''
        else:
            text = row.full_text

    row.empty = 0
    if ('bennet' in text.lower()) or ('bennet' in q_text.lower()):
        row.Bennet = 1
        row.empty = 1
    else:
        row.Bennet = 0
    if ('biden' in text.lower()) or ('biden' in q_text.lower()):
        row.Biden = 1
        row.empty = 1
    else:
        row.Biden = 0
    if ('blasio' in text.lower()) or ('blasio' in q_text.lower()):
        row.Blasio = 1
        row.empty = 1
    else:
        row.Blasio = 0
    if ('booker' in text.lower()) or ('booker' in q_text.lower()):
        row.Booker = 1
        row.empty = 1
    else:
        row.Booker = 0
    if ('buttigieg' in text.lower()) or ('mayor pete' in text.lower()) or ('buttigieg' in q_text.lower()) or (
            'mayor pete' in q_text.lower()):
        row.Buttigieg = 1
        row.empty = 1
    else:
        row.Buttigieg = 0
    if ('castro' in text.lower()) or ('castro' in q_text.lower()):
        row.Castro = 1
        row.empty = 1
    else:
        row.Castro = 0
    if ('delaney' in text.lower()) or ('delaney' in q_text.lower()):
        row.Delaney = 1
        row.empty = 1
    else:
        row.Delaney = 0
    if ('tulsi' in text.lower()) or ('gabbard' in text.lower()) or ('tulsi' in q_text.lower()) or (
            'gabbard' in q_text.lower()):
        row.Gabbard = 1
        row.empty = 1
    else:
        row.Gabbard = 0
    if ('gillibrand' in text.lower()) or ('gillibrand' in q_text.lower()):
        row.Gillibrand = 1
        row.empty = 1
    else:
        row.Gillibrand = 0
    if ('kamala' in text.lower()) or ('harris' in text.lower()) or ('kamala' in q_text.lower()) or (
            'harris' in q_text.lower()):
        row.Harris = 1
        row.empty = 1
    else:
        row.Harris = 0
    if ('hickenlooper' in text.lower()) or ('hickenlooper' in q_text.lower()):
        row.Hickenlooper = 1
        row.empty = 1
    else:
        row.Hickenlooper = 0
    if ('inslee' in text.lower()) or ('inslee' in q_text.lower()):
        row.Inslee = 1
        row.empty = 1
    else:
        row.Inslee = 0
    if ('klobuchar' in text.lower()) or ('klobuchar' in q_text.lower()):
        row.Klobuchar = 1
        row.empty = 1
    else:
        row.Klobuchar = 0
    if ('beto' in text.lower()) or ('orourke' in text.lower()) or ("o'rourke" in text.lower()) or (
            'beto' in q_text.lower()) or ('orourke' in q_text.lower()) or ("o'rourke" in q_text.lower()):
        row.ORourke = 1
        row.empty = 1
    else:
        row.ORourke = 0
    if ('ryan' in text.lower()) or ('ryan' in q_text.lower()):
        row.Ryan = 1
        row.empty = 1
    else:
        row.Ryan = 0
    if ('bernie' in text.lower()) or ('sanders' in text.lower()) or ('bernie' in q_text.lower()) or (
            'sanders' in q_text.lower()):
        row.Sanders = 1
        row.empty = 1
    else:
        row.Sanders = 0
    if ('swalwell' in text.lower()) or ('swalwell' in q_text.lower()):
        row.Swawell = 1
        row.empty = 1
    else:
        row.Swalwell = 0
    if ('tulsi' in text.lower()) or ('tulsi' in q_text.lower()):
        row.Tulsi = 1
        row.empty = 1
    else:
        row.Tulsi = 0
    if ('warren' in text.lower()) or ('warren' in q_text.lower()):
        row.Warren = 1
        row.empty = 1
    else:
        row.Warren = 0
    if ('williamson' in text.lower()) or ('williamson' in q_text.lower()):
        row.Williamson = 1
        row.empty = 1
    else:
        row.Williamson = 0
    if ('yang' in text.lower()) or ('yang' in q_text.lower()):
        row.Yang = 1
        row.empty = 1
    else:
        row.Yang = 0

    return row


while True:
    df_tweets = db.get_data()

    if len(df_tweets) > 0:
        for idx, row in df_tweets.iterrows():
            row.sent = calc_sentiment(row.text, row.full_text, row.q_text)
            row = get_candidates(row)
            row.updated = 1
            db.update_tweets_db(row)
    time.sleep(5)
    print('updated:  ', row['dt'])



