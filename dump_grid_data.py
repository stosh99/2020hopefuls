import time
from models import Database
from models import get_chicago_time

db = Database()

while True:
    df, last_update = db.get_grid_data()
    df['dt'] = get_chicago_time()
    db.dump_grid_data(df)
    print(df)
    time.sleep(120)

