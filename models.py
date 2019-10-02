import pandas as pd
import sqlalchemy
from datetime import datetime as dt
import datetime
import pytz


class Database:
    def __init__(self):
        #self.engine = sqlalchemy.create_engine('mysql+mysqlconnector://demouser:Anna0723$@127.0.0.1/tweets')
        self.SQLALCHEMY_DATABASE_URI = "mysql+mysqlconnector://{username}:{password}@{hostname}/{databasename}".format(
            username="admin",
            password="admin999",
            hostname="bu2019.cgh9oe6xgzbv.us-east-1.rds.amazonaws.com",
            databasename="2020hopefuls",
        )
        self.engine = sqlalchemy.create_engine(self.SQLALCHEMY_DATABASE_URI)

    def get_data(self):
        qry = 'SELECT * FROM tweets WHERE updated = 0 LIMIT 5000'
        con = self.engine.connect()
        vals = con.execute(qry)
        df = pd.DataFrame(vals.fetchall())
        if len(df) > 0:
            df.columns = vals.keys()
        con.close()
        return df

    def update_tweets_db(self, row):
        qry = "UPDATE tweets " \
              "SET sent=%s, updated=%s, Bennet=%s, Biden=%s, Blasio=%s, Booker=%s, Buttigieg=%s, Castro=%s, Delaney=%s, " \
              "Gabbard=%s, Gillibrand=%s, Harris=%s, Hickenlooper=%s, Inslee=%s, Klobuchar=%s, ORourke=%s, " \
              "Ryan=%s, Sanders=%s, Tulsi=%s, Warren=%s, Williamson=%s, Yang=%s, empty=%s " \
              "WHERE id = %s"
        update_tuple = (row.sent, 1, row.Bennet, row.Biden, row.Blasio, row.Booker, row.Buttigieg, row.Castro,
                        row.Delaney, row.Gabbard, row.Gillibrand, row.Harris, row.Hickenlooper, row.Inslee,
                        row.Klobuchar, row.ORourke, row.Ryan, row.Sanders, row.Tulsi, row.Warren, row.Williamson,
                        row.Yang, row.empty, row.id)
        con = self.engine.connect()
        con.execute(qry, update_tuple)
        con.close()

    def get_candidate_data_2(self, candidate, sent_limit):
        con = self.engine.connect()
        df = pd.read_sql('tweets', con=con)
        df = df[df['updated'] == 1]
        candidates = pd.read_sql('candidates', con=con)['candidate']
        states = pd.read_sql('states', con=con)['abbr']
        con.close()
        date_today = dt.strftime(dt.now(), '%Y-%m-%d')
        last_update = df['dt'].max()

        df_filter = filter_sent(df, sent_limit)
        df_master = create_master(df_filter, candidate, states, date_today)
        plt_df = get_plot_df(df_master, candidate)

        return last_update, plt_df

    def get_candidate_data_cur(self, candidate, sent_limit):
        qry = "SELECT * FROM tweets WHERE updated=1 and DAY(dt) = (SELECT MAX(DAY(dt)) FROM tweets)"
        con = self.engine.connect()
        vals = con.execute(qry)
        df_cand = pd.DataFrame(vals.fetchall())
        df_cand.columns = vals.keys()
        candidates = pd.read_sql('candidates', con=con)['candidate']
        states = pd.read_sql('states', con=con)['abbr']
        con.close()
        date_today = dt.strftime(get_chicago_time(), '%Y-%m-%d')
        last_update = df_cand['dt'].max()

        df_filter = filter_sent(df_cand, sent_limit)
        df_master = create_master(df_filter, candidate, states, date_today)
        plt_df = get_plot_df(df_master, candidate)
        plt_df = plt_df[plt_df['user_count'] > 0]
        tweets = plt_df['user_count'].sum()
        return last_update, tweets, plt_df

    def get_candidate_data(self, candidate, sent_limit):
        if candidate == 'All':
            qry = "SELECT * FROM tweets WHERE DATE(dt) = (SELECT MAX(DATE(dt)) FROM tweets)"
        else:
            qry = "SELECT * FROM tweets WHERE DATE(dt) = (SELECT MAX(DAtE(dt)) FROM tweets)" \
                  + " and " + candidate + "=1"
        con = self.engine.connect()
        vals = con.execute(qry)
        df_cand = pd.DataFrame(vals.fetchall())
        df_cand.columns = vals.keys()
        con.close()
        date_today = dt.strftime(get_chicago_time(), '%Y-%m-%d')
        last_update = df_cand['dt'].max()

        df_filter = filter_sent(df_cand, sent_limit)
        df_master = create_master(df_filter, candidate, date_today)
        plt_df = get_plot_df(df_master, candidate)
        plt_df = plt_df[plt_df['user_count'] > 0]
        tweets = plt_df['user_count'].sum()
        return last_update, tweets, plt_df

    def get_states(self):
        qry = "SELECT * FROM states"
        con = self.engine.connect()
        vals = con.execute(qry)
        df_states = pd.DataFrame(vals.fetchall())
        df_states.columns = vals.keys()
        return df_states

    def get_candidates(self):
        cur_date = dt.strftime(dt.now(), '%YYYY-%m-%d')
        qry = "SELECT * FROM candidates WHERE exit_dt > %s"
        con = self.engine.connect()
        vals = con.execute(qry, cur_date)
        df_cand = pd.DataFrame(vals.fetchall())
        df_cand.columns = vals.keys()
        return list(df_cand.itertuples(index=False))

    def get_scaled_sent(self):
        qry = "SELECT MAX(sent)as max_sent, MIN(sent) as min_sent " \
              "FROM tweets WHERE updated=1 and DAY(dt) = (SELECT MAX(DAY(dt)) FROM tweets)"
        con = self.engine.connect()
        vals = con.execute(qry).fetchone()
        return vals[0], vals[1]

    def get_grid_data(self):
        candidates = self.get_candidates()
        grid_list = []

        qry = "SELECT * FROM tweets WHERE updated=1 and DATE(dt) = (SELECT MAX(DATE(dt)) FROM tweets)"#" AND state != 'unk'"
        con = self.engine.connect()
        vals = con.execute(qry)
        con.close()
        df = pd.DataFrame(vals.fetchall())
        df.columns = vals.keys()
        last_update = df['dt'].max()

        for candidate in candidates:
            avg_sent = df[df[candidate[0]] == 1].groupby(by='user_id')['sent'].mean().mean()
            user_count = df[df[candidate[0]] == 1].groupby(by='user_id')['sent'].count().count()
            grid_list.append([candidate[0], candidate[2], avg_sent, user_count, candidate[3]])
        cols = ['candidate', 'fullname', 'avg_sent', 'user_count', 'jpeg']
        df = pd.DataFrame(data=grid_list, columns=cols)
        return df, last_update

    def dump_grid_data(self, df):
        con = self.engine.connect()
        df.to_sql('grid_data', con=con, if_exists='append', index=False)
        con.close()
        return

    def get_grid_data_db(self):
        qry = "SELECT * FROM grid_data WHERE dt = (SELECT MAX(dt) FROM grid_data)"
        con = self.engine.connect()
        vals = con.execute(qry)
        con.close()
        df = pd.DataFrame(vals.fetchall())
        df.columns = vals.keys()
        last_update = df['dt'].max()
        return df, last_update


def filter_sent(df, sent_limit):
    df_filter = df[(df['sent'] > sent_limit) | (df['sent'] < -sent_limit)]
    return df_filter


def create_master_2(df, candidates, states, date_today):
    df_master = pd.DataFrame(columns=['date', 'candidate', 'state', 'sent', 'user_count', ])
    for candidate in candidates:
        sent_state = df[df[candidate] == 1].groupby(by=['state', 'user_id'])['sent'].mean().groupby(by='state').mean()
        count_state = df[df[candidate] == 1].groupby(by=['state'])['user_id'].nunique()
        df_temp = pd.concat([sent_state, count_state], axis=1).reset_index()
        df_temp['date'] = date_today
        df_temp['candidate'] = candidate
        df_temp.rename(columns={'user_id': 'user_count'}, inplace=True)
        df_master = df_master.append(df_temp, ignore_index=True, sort=False)
    return df_master


def create_master(df, candidate, date_today):
    sent_state = df.groupby(by=['state', 'user_id'])['sent'].mean().groupby(by='state').mean()
    count_state = df.groupby(by=['state'])['user_id'].nunique()
    df_temp = pd.concat([sent_state, count_state], axis=1).reset_index()
    df_temp['date'] = date_today
    df_temp['candidate'] = candidate
    df_temp.rename(columns={'user_id': 'user_count'}, inplace=True)
    return df_temp


def get_plot_df(df_master, candidate='All'):
    df_plot = df_master[(df_master['candidate'] == candidate) & (df_master['state'] != 'unk')]
    return df_plot


def get_scaled_sent():
    qry = "SELECT MAX(sent)as max_sent, MIN(sent) as min_sent " \
          "FROM tweets WHERE updated=1 and DAY(dt) = (SELECT MAX(DAY(dt)) FROM tweets)"


def get_chicago_time():
    utcmoment_naive = dt.utcnow()
    utcmoment = utcmoment_naive.replace(tzinfo=pytz.utc)
    tz = 'America/Chicago'
    return utcmoment.astimezone(pytz.timezone(tz))





