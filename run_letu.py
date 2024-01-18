import schedule
import time

from main import time_zone, t
from LETU.result import job



schedule.every().second.do(job)


while True:
    schedule.run_pending()
    time.sleep(1)
