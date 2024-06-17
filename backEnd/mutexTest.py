import threading
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
# 创建一个scheduler实例
# scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
scheduler = BlockingScheduler()
tradeNum = {}
mutex = threading.Lock()

def job1():
    global tradeNum
    mutex.acquire()
    tradeNum[str(datetime.datetime.now())] = len(tradeNum)
    mutex.release()
    print('job1',tradeNum)
    


def job2():
    global tradeNum
    mutex.acquire()
    if len(tradeNum) !=0:
        tradeNum.popitem()
    mutex.release()
    print('job2',tradeNum)

if __name__ == '__main__':
    print("yes")
    scheduler.add_job(job1,'cron',second='*/20',minute='*', day_of_week ='0-4')
    scheduler.add_job(job2,'cron',minute='*/1', day_of_week ='0-4')
    scheduler.start()