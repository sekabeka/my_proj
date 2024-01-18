import schedule
import time

from main import time_zone, t
from AUCHAN.result import job



schedule.every().days.at(t, time_zone).do(job)


while True:
    schedule.run_pending()
    time.sleep(1)
