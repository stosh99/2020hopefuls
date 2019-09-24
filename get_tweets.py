from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
import json
import re
import pandas as pd
import sqlalchemy
from datetime import datetime
import tzlocal
import pytz

ACCESS_TOKEN = "1628411605-V9MhTFQBUjDfHDpPiggFjNvvKPCk5DLfIEYARXz"
ACCESS_TOKEN_SECRET = "u9fBDP1lyptx2mfmZUK4uaqzrC6PlwGNQtdQFszLdXGu0"
CONSUMER_KEY = "TTdP7HewA3BiKAE6U6bm1cAGH"
CONSUMER_SECRET = "tQItnZCrM4WF2F8gltstM6QWgBRT4epa9UHgRkQO57iZqGwsvy"

auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = API(auth, wait_on_rate_limit=True,
          wait_on_rate_limit_notify=True)

df_states = pd.read_csv('static/states.csv')


class listener(StreamListener):

    def __init__(self):
        self.SQLALCHEMY_DATABASE_URI = "mysql+mysqlconnector://{username}:{password}@{hostname}/{databasename}".format(
            username="admin",
            password="admin999",
            hostname="bu2019.cgh9oe6xgzbv.us-east-1.rds.amazonaws.com",
            databasename="2020hopefuls",
        )
        self.engine = sqlalchemy.create_engine(self.SQLALCHEMY_DATABASE_URI)

    def build_file(self, save_this, field):
        if field:
            save_this += ',' + self.clean_tweet(field)
        else:
            save_this += ',' + ''
        return save_this

    def clean_tweet(self, tweet):
        return ' '.join(re.sub("(@+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

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

            insert_tuple = (id, dt_utc, dt_chi, created_at, user_id, coord, user_loc, hashtags, state, place,
                            text, q_text, retweet_text, full_text, 0)

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

            print(insert_tuple[10])
            print('*****')
            with open(self.jsonfile, 'a') as outfile:
                json.dump(tweet_data, outfile, sort_keys=True, indent=4)

            query = "INSERT INTO tweets " \
                    "(id, dt_utc, dt, created_at, user_id, coord, user_loc, hashtags, state, place, text, q_text, " \
                    "retweet_text, full_text, updated) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            con = self.engine.connect()
            con.execute(query, insert_tuple)
            con.close()
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




