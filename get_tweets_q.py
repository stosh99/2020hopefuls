from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
import json
import re
import pandas as pd
import sqlalchemy
from datetime import datetime
import pytz
import queue
import threading
from textblob import TextBlob

ACCESS_TOKEN = "1628411605-V9MhTFQBUjDfHDpPiggFjNvvKPCk5DLfIEYARXz"
ACCESS_TOKEN_SECRET = "u9fBDP1lyptx2mfmZUK4uaqzrC6PlwGNQtdQFszLdXGu0"
CONSUMER_KEY = "TTdP7HewA3BiKAE6U6bm1cAGH"
CONSUMER_SECRET = "tQItnZCrM4WF2F8gltstM6QWgBRT4epa9UHgRkQO57iZqGwsvy"

auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = API(auth, wait_on_rate_limit=True,
          wait_on_rate_limit_notify=True)

df_states = pd.read_csv('static/states.csv')

SQLALCHEMY_DATABASE_URI = "mysql+mysqlconnector://{username}:{password}@{hostname}/{databasename}".format(
            username="admin",
            password="admin999",
            hostname="bu2019.cgh9oe6xgzbv.us-east-1.rds.amazonaws.com",
            databasename="2020hopefuls",
        )

#engine = sqlalchemy.create_engine('mysql+mysqlconnector://demouser:Anna0723$@127.0.0.1/tweets')
engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI)


def tweet_processing_thread():
    while True:
        item = tweet_queue.get()
        print(tweet_queue.qsize())
        process_tweet(item)
        tweet_queue.task_done()


def process_tweet(tweet):
    tweet_dict = get_candidates(tweet)
    tweet_dict['dt_utc'] = datetime.strftime(tweet_dict['dt_utc'], "%Y-%m-%d %H:%M:%S")
    tweet_dict['dt'] = datetime.strftime(tweet_dict['dt'], "%Y-%m-%d %H:%M:%S")
    tweet_dict['created_at'] = datetime.strftime(tweet_dict['created_at'], "%Y-%m-%d %H:%M:%S")
    tweet_dict['sent'] = calc_sentiment(tweet_dict['text'], tweet_dict['full_text'], tweet_dict['q_text'])
    if tweet_dict['coord'] is None:
        tweet_dict['coord'] = 'None'
    print(tweet_dict['sent'], tweet_dict['text'])
    print("^^^^^^^^^^^^")
    df_tweet = pd.DataFrame(data=[tweet_dict], columns=tweet_dict.keys(), index=None)
    con = engine.connect()
    try:
        df_tweet.to_sql('tweets', con=con, if_exists='append')
    except:
        print('fail')
    con.close()


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
    text = row['text']
    q_text = row['q_text']
    if row['q_text'] == '' or pd.isnull(row['q_text']):
        q_text = ''
    if row['q_text'] == '' or pd.isnull(row['text']):
        if row['full_text'] == '' or pd.isnull(row['text']):
            text = ''
        else:
            text = row['full_text']

    row['empty'] = 0
    if ('bennet' in text.lower()) or ('bennet' in q_text.lower()):
        row['Bennet'] = 1
        row['empty'] = 1
    else:
        row['Bennet'] = 0
    if ('biden' in text.lower()) or ('biden' in q_text.lower()):
        row['Biden'] = 1
        row['empty'] = 1
    else:
        row['Biden'] = 0
    if ('blasio' in text.lower()) or ('blasio' in q_text.lower()):
        row['Blasio'] = 1
        row['empty'] = 1
    else:
        row['Blasio'] = 0
    if ('booker' in text.lower()) or ('booker' in q_text.lower()):
        row['Booker'] = 1
        row['empty'] = 1
    else:
        row['Booker'] = 0
    if ('buttigieg' in text.lower()) or ('mayor pete' in text.lower()) or ('buttigieg' in q_text.lower()) or (
            'mayor pete' in q_text.lower()):
        row['Buttigieg'] = 1
        row['empty'] = 1
    else:
        row['Buttigieg'] = 0
    if ('castro' in text.lower()) or ('castro' in q_text.lower()):
        row['Castro'] = 1
        row['empty'] = 1
    else:
        row['Castro'] = 0
    if ('delaney' in text.lower()) or ('delaney' in q_text.lower()):
        row['Delaney'] = 1
        row['empty'] = 1
    else:
        row['Delaney'] = 0
    if ('tulsi' in text.lower()) or ('gabbard' in text.lower()) or ('tulsi' in q_text.lower()) or (
            'gabbard' in q_text.lower()):
        row['Gabbard'] = 1
        row['empty'] = 1
    else:
        row['Gabbard'] = 0
    if ('gillibrand' in text.lower()) or ('gillibrand' in q_text.lower()):
        row['Gillibrand'] = 1
        row['empty'] = 1
    else:
        row['Gillibrand'] = 0
    if ('kamala' in text.lower()) or ('harris' in text.lower()) or ('kamala' in q_text.lower()) or (
            'harris' in q_text.lower()):
        row['Harris'] = 1
        row['empty'] = 1
    else:
        row['Harris'] = 0
    if ('hickenlooper' in text.lower()) or ('hickenlooper' in q_text.lower()):
        row['Hickenlooper'] = 1
        row['empty'] = 1
    else:
        row['Hickenlooper'] = 0
    if ('inslee' in text.lower()) or ('inslee' in q_text.lower()):
        row['Inslee'] = 1
        row['empty'] = 1
    else:
        row['Inslee'] = 0
    if ('klobuchar' in text.lower()) or ('klobuchar' in q_text.lower()):
        row['Klobuchar'] = 1
        row['empty'] = 1
    else:
        row['Klobuchar'] = 0
    if ('beto' in text.lower()) or ('orourke' in text.lower()) or ("o'rourke" in text.lower()) or (
            'beto' in q_text.lower()) or ('orourke' in q_text.lower()) or ("o'rourke" in q_text.lower()):
        row['ORourke'] = 1
        row['empty'] = 1
    else:
        row['ORourke'] = 0
    if ('ryan' in text.lower()) or ('ryan' in q_text.lower()):
        row['Ryan'] = 1
        row['empty'] = 1
    else:
        row['Ryan'] = 0
    if ('bernie' in text.lower()) or ('sanders' in text.lower()) or ('bernie' in q_text.lower()) or (
            'sanders' in q_text.lower()):
        row['Sanders'] = 1
        row['empty'] = 1
    else:
        row['Sanders'] = 0
    if ('swalwell' in text.lower()) or ('swalwell' in q_text.lower()):
        row['Swalwell'] = 1
        row['empty'] = 1
    else:
        row['Swalwell'] = 0
    if ('tulsi' in text.lower()) or ('tulsi' in q_text.lower()):
        row['Tulsi'] = 1
        row['empty'] = 1
    else:
        row['Tulsi'] = 0
    if ('warren' in text.lower()) or ('warren' in q_text.lower()):
        row['Warren'] = 1
        row['empty'] = 1
    else:
        row['Warren'] = 0
    if ('williamson' in text.lower()) or ('williamson' in q_text.lower()):
        row['Williamson'] = 1
        row['empty'] = 1
    else:
        row['Williamson'] = 0
    if ('yang' in text.lower()) or ('yang' in q_text.lower()):
        row['Yang'] = 1
        row['empty'] = 1
    else:
        row['Yang'] = 0
    row['updated'] = 1
    return row


tweet_queue = queue.Queue()
thread = threading.Thread(target=tweet_processing_thread)
thread.Daemon = True
thread.start()


class listener(StreamListener):

    def __init__(self):
        pass

    def build_file(self, save_this, field):
        if field:
            save_this += ',' + self.clean_tweet(field)
        else:
            save_this += ',' + ''
        return save_this

    def clean_tweet(self, tweet):
        return ' '.join(re.sub("(@+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(\xF0\x9F\x91\x87\xF0\x9F)", " ", tweet).split())

    def find_state(self, loc, df_state):
        abbr = 'unk'
        for state in df_state['State']:
            if state in loc:
                abbr = df_state[df_state['State'] == state]['Abbreviation'].item()
        for a in df_state['Abbreviation']:
            if a in loc:
                abbr = a
        return abbr

    def on_data(self, data):
        try:
            tweet_data = json.loads(data)
            id = str(tweet_data['id'])
            created_at = str(tweet_data['created_at'])
            user_id = str(tweet_data['user']['id'])
            coord = tweet_data['coordinates']
            user_loc = tweet_data['user']['location']
            if tweet_data['entities']['hashtags']:
                hashtags = tweet_data['entities']['hashtags']
            else:
                hashtags = 'NULL'
            state = self.find_state(tweet_data['user']['location'], df_states)
            if tweet_data['place']:
                place = tweet_data['place']['full_name']
            else:
                place = 'NULL'
            text = tweet_data['text']
            if tweet_data['is_quote_status']:
                q_text = tweet_data['quoted_status']['text']
            else:
                q_text = 'NULL'
            if tweet_data['retweeted']:
                if 'full_text' in tweet_data['retweeted']:
                    retweet_text = tweet_data['retweeted_status']['full_text']
                else:
                    retweet_text = tweet_data['retweeted_status']['text']
            else:
                retweet_text = 'NULL'
            if 'extended_tweet' in tweet_data:
                full_text = tweet_data['extended_tweet']['full_text']
            else:
                full_text = 'NULL'

            dt_string = created_at.split(' ')[-1] + '-' + created_at.split(' ')[1] + '-' + \
                        created_at.split(' ')[2] + ' ' + created_at.split(' ')[3]
            dt_utc = datetime.strptime(dt_string, '%Y-%b-%d %H:%M:%S')
            dt_chi = dt_utc.astimezone(pytz.timezone('America/Chicago'))
            created_at = dt_utc.astimezone(pytz.timezone('America/Chicago'))

            insert_dict = {'id': id, 'dt_utc': dt_utc, 'dt': dt_chi, 'created_at': created_at, 'user_id': user_id,
                           'coord': coord, 'user_loc': user_loc, 'hashtags': hashtags, 'state': state, 'place': place,
                           'text': text, 'q_text': q_text, 'retweet_text': retweet_text, 'full_text': full_text,
                           'updated': 0}

            save_this = str(tweet_data['id'])
            save_this = self.build_file(save_this, str(tweet_data['created_at']))
            save_this = self.build_file(save_this, str(tweet_data['user']['id']))
            save_this = self.build_file(save_this, tweet_data['coordinates'])
            save_this = self.build_file(save_this, tweet_data['user']['location'])
            save_this = self.build_file(save_this, tweet_data['entities']['hashtags'])
            save_this += ',' + self.find_state(tweet_data['user']['location'], df_states)
            if tweet_data['place']:
                save_this = self.build_file(save_this, tweet_data['place']['full_name'])
            else:
                save_this += ',' + ''
            save_this = self.build_file(save_this, tweet_data['text'])
            if tweet_data['is_quote_status']:
                save_this = self.build_file(save_this, tweet_data['quoted_status']['text'])
            else:
                save_this += ',' + ''
            if tweet_data['retweeted']:
                if 'full_text' in tweet_data['retweeted']:
                    save_this = self.build_file(save_this, tweet_data['retweeted_status']['full_text'])
                else:
                    save_this = self.build_file(save_this, tweet_data['retweeted_status']['text'])
            else:
                save_this += ',' + ''
            if 'extended_tweet' in tweet_data:
                save_this = self.build_file(save_this, tweet_data['extended_tweet']['full_text'])
            else:
                save_this += ',' + ''

            tweet_queue.put(insert_dict)
        except:
            pass
        return True

    def on_timeout(self):
        print ('Timeout...')
        return True  # Don't kill the stream
        print
        "Stream restarted"

    def on_error(self, status):
        print(status)


twitterStream = Stream(auth, listener())
twitterStream.filter(languages=['en'], track=["Biden", "Bennet", "Buttigieg", "Mayor Pete", "Gillibrand", "Kamala",
                                              "Harris", "Hickenlooper", "Sanders", "Bernie", "Swalwell",
                                              "Williamson", "Yang", "Warren", "Beto", "O'Rourke", "Cory", "Booker",
                                              "Klobuchar", "Inslee", "Tulsi", "Gabbard", "Delaney", "Castro", "Ryan",
                                              "Blasio", "Castro"])




