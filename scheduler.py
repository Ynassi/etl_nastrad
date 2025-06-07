import time
from datetime import datetime
import pytz
from api.tasks import run_all_pipelines

def is_market_open_time():
    now_est = datetime.now(pytz.timezone("US/Eastern"))
    return now_est.hour == 11 and now_est.minute == 30

while True:
    if is_market_open_time():
        print("🕤 Lancement automatique des pipelines à 9h30 EST")
        run_all_pipelines()
        time.sleep(60)  # évite de relancer plusieurs fois
    time.sleep(30)
