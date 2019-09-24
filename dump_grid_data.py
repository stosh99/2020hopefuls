import time
from models import Database
from datetime import datetime as dt

db = Database()

while True:
    df, last_update = db.get_grid_data()
    df['dt'] = db.get_chicago_time()
    db.dump_grid_data(df)
    print(df)
    time.sleep(5)

