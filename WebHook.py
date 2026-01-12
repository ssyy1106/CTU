import requests
import json
import pyodbc
import schedule
import time
from types import SimpleNamespace
import os
#import certifi

class API:
    def __init__(self, description: str, url: str, status: int, interval: int, timeout: int, calls: int, 
                 fails: int, errorTitle: str, errorMessage: str, timeoutMessage: str) -> None:
        self.description = description
        self.url = url
        self.status = status
        self.interval = interval
        self.timeout = timeout
        self.calls = calls
        self.fails = fails
        self.errorTitle = errorTitle
        self.errorMessage = errorMessage
        self.timeoutMessage = timeoutMessage

class DB:
    def __init__(self, description: str, host: str, user: str, pw: int, db: int, interval: int, errorTitle: str, errorMessage: str) -> None:
        self.description = description
        self.host = host
        self.user = user
        self.pw = pw
        self.db = db
        self.interval = interval
        self.errorTitle = errorTitle
        self.errorMessage = errorMessage

class Directory:
    def __init__(self, description: str, path: str, errorTitle: str, errorMessage: str) -> None:
        self.description = description
        self.path = path
        self.errorTitle = errorTitle
        self.errorMessage = errorMessage

class Config:
    def __init__(self, api: list, DB: list, Directory: list) -> None:
        self.api = api
        self.DB = DB
        self.Directory = Directory

def sendNotification(title: str, message: dict):
    #print(certifi.where())
    webhook_url = "https://btrust.webhook.office.com/webhookb2/40fb9799-2d58-4cec-a448-ee6876dba01d@f95378be-1a4d-4296-b98e-75abc1bd706e/IncomingWebhook/d74c4b828be6416daf9b60756d99da40/a21343a9-07d1-49be-953d-2b6922bff8fd"
    message = {
        "text": message,
        "title": title
    }
    try:
        payload = json.dumps(message)
        response = requests.post(webhook_url, data = payload, headers = {"Content-Type": "application/json"})
        if response.status_code == 200:
            print("Send notification success")
        else:
            print("Send notification fail")
    except Exception as e:
        print(f"Send notification error: {e}")

def jobWeb(api: API):
    try:
        response = requests.get(api.url, timeout = api.timeout)
        if response.status_code == api.status:
            print(f"caling {api.url} success")
        else:
            sendNotification(api.errorTitle, api.errorMessage)
            print(f"caling {api.url} fail response status: {response.status_code}")
    except requests.exceptions.Timeout:
        sendNotification(api.errorTitle, api.timeoutMessage)
    
def jobDB(db: DB):
    conn = None
    try:
        connectionString = f'DRIVER={{SQL Server}};SERVER={db.host};DATABASE={db.db};UID={db.user};PWD={db.pw}'
        conn = pyodbc.connect(connectionString)
        print(f"Connect DB {db.description}-{db.host}-{db.db} success.")
    except:
        print(f"Connect DB {db.description}-{db.host}-{db.db} fail.")
        sendNotification(db.errorTitle, db.errorMessage)
    finally:
        if conn:
            conn.close()

def jobPath(path: Directory):
    if os.path.isdir(path.path):
        print(f"Path {path.path} {path.description} check success.")
    else:
        print(f"Path {path.path} {path.description} check fail.")
        sendNotification(path.errorTitle, path.errorMessage)


def createJobs(apis: list, DBs: list, paths: list):
    for api in apis:
        schedule.every(api.interval).minutes.do(jobWeb, api = api)
        
    for db in DBs:
        schedule.every(db.interval).minutes.do(jobDB, db = db)

    for path in paths:
        schedule.every(path.interval).minutes.do(jobPath, path = path)

if __name__ == "__main__":
    # 读取配置文件，得到url，间隔时间，超时时间，返回code，最近n次检测连续m次失败发送报警消息(滑动窗口实现)，
    with open('webhook_config.json') as j:
        cfg = json.load(j, object_hook=lambda d: SimpleNamespace(**d))
        print(f"Config file: {cfg}")
        apis = cfg.api
        DBs = cfg.DB
        paths = cfg.Directory
        createJobs(apis, DBs, paths)
        while True:
            try:
                schedule.run_pending()
            except Exception as e:
                print(f"run pending error: {e}")
            time.sleep(5)

