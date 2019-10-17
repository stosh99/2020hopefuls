import time
import app
import pandas as pd
import datetime as dt
from models import Database
from models import get_chicago_time

db = Database()
print(dt.datetime.now())
states, tweets = db.get_tweets()
print(dt.datetime.now())
tweets.to_csv('~/PycharmProjects/2020hopefuls/static/tweets.txt', index=None, sep=',', mode='w')
print(dt.datetime.now())
tweets2 = pd.read_csv('~/PycharmProjects/2020hopefuls/static/tweets.txt')
print(dt.datetime.now())
print(tweets.tail())
print(tweets2.tail())




"""
cols = ['state', 'sent', 'user_count', 'date', 'candidate']
df_all = pd.DataFrame(columns=cols)
print(df_all)
for candidate in candidates['candidate']:
    last_update, tweets, df = db.get_candidate_data(candidate, 0)
    df_all = df_all.append(df, ignore_index=True, sort=True)

print(df_all)
"""
