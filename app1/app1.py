import time  # Thread: Sleep
import datetime
import schedule  # Scheduler
import requests

def task_1(text:str):
    print(text)
#     url = 'https://16b336b235e7f458c2cff4a428ef7780.m.pipedream.net'
#     data = {
#         "message": text
#     }

#     response = requests.post(url, json=data)
#     print("response= ", response.json())

schedule.every().minute.at(":00").do(task_1, "App1: Hello World!!!")
# schedule.run_all()  # run

dt = datetime.datetime.now()
task_1(f"Application started at {dt.strftime('%Y-%m-%d %H:%M:%S')}")

while True:
    schedule.run_pending()
    time.sleep(1)