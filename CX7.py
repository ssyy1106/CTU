import json
import datetime
import requests
import logging
import time
from helper import DBContext

def get_token(url: str, apiKey: str, apiSecret: str, certification: str) -> str:
    timeStamp = datetime.datetime.now().isoformat()
    payload={'header[nameSpace]': 'authorize.token',
    'header[nameAction]': 'token',
    'header[version]': '1.0',
    'header[requestId]': 'f1becc28-ad01-b5b2-7cef-392eb1526f39',
    'header[timestamp]': timeStamp,
    'payload[api_key]': apiKey,
    'payload[api_secret]': apiSecret}
    files=[]
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload, files=files, verify = certification)
    json_object = json.loads(response.text)
    print(json_object)
    if json_object['payload'] and json_object['payload']['token']:
        return json_object['payload']['token']

    return ""

def get_punches(url: str, page: int, token: str, beginTime: str, endTime: str, certification: str) -> str:
    timeStamp = datetime.datetime.now().isoformat()
    payload={'header[nameSpace]': 'attendance.record',
    'header[nameAction]': 'getrecord',
    'header[version]': '1.0',
    'header[requestId]': 'f1becc28-ad01-b5b3-7cef-392eb1526f39',
    'header[timestamp]': timeStamp,
    'authorize[type]': 'token',
    'authorize[token]': token,
    'payload[begin_time]': beginTime,
    'payload[end_time]': endTime,
    'payload[workno]': '',
    'payload[order]': 'asc',
    'payload[page]': str(page),
    'payload[per_page]': '10'}
    files=[]
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload, files=files, verify = certification)
    #print(f"response: {response.text}")
    return response.text

def store_punches(punches: list, store: str):
    if not punches:
        return
    with DBContext() as conn:
        with conn.cursor() as cursor:
            for punch in punches:
                checkTime = datetime.datetime.fromisoformat(punch['checktime'])
                BtrustID = punch['employee']['workno']
                # set time zone 
                sgtTimeDelta = datetime.timedelta(hours=-5)
                sgtTZObject = datetime.timezone(sgtTimeDelta, name="US/Eastern")
                localTime = checkTime.astimezone(sgtTZObject)

                year = localTime.strftime("%Y")
                month = localTime.strftime("%m")
                day = localTime.strftime("%d")
                hour = localTime.strftime("%H")
                minute = localTime.strftime("%M")
                punchDate = year+'-'+month+'-'+day
                sql = f"Merge into SysPunch as t using (select '{BtrustID}' as BtrustId, '{year}' as year, '{month}' as month, '{day}' as day, '{hour}' as hour, '{minute}' as minute) as s on s.btrustid = t.btrustid and s.year = t.year and s.month = t.month and s.day = t.day and s.hour = t.hour and s.minute = t.minute when not matched then insert values('{BtrustID}', '{year}', '{month}', '{day}', '{hour}', '{minute}', '0', '{store}', '0', '1', '0', 'CX7', '{punchDate}', null, null);"
                #print(f"year: {year} month: {month} day: {day} hour: {hour} minute: {minute}")
                #sql = f"Merge into SysPunchFile as t using (select '{fileName}' as FileName, '{items}' as Items) as s on s.FileName = t.FileName when not matched then insert values( '{fileName}', '{fileModify}', {items}, '{firstYear}', '{firstMonth}', '{firstDay}', '{firstHour}', '{firstMinute}') when matched then update set ModifyTime = '{fileModify}', items={items}, firstYear='{firstYear}', firstMonth='{firstMonth}', firstDay='{firstDay}', firstHour='{firstHour}', firstMinute='{firstMinute}';"
                cursor.execute(sql)
            print(f"commit punches")


def read_from_cx(url: str, token: str, beginTime: str, endTime: str, certification: str, store: str):
    page = 1
    pageCount = 0
    responseText = get_punches(url, page, token, beginTime, endTime, certification)
    
    json_object = json.loads(responseText)
    payLoad = json_object['payload']
    if payLoad:
        page = int(payLoad['page'])
        pageCount = int(payLoad['pageCount'])
        punches = payLoad['list']
        store_punches(punches, store)

    while payLoad and pageCount > page:
        # 调用的api增加了限流，每分钟一次，所以等待一会
        time.sleep(70)
        page += 1
        responseText = get_punches(url, page, token, beginTime, endTime, certification)
        json_object = json.loads(responseText)
        payLoad = json_object['payload']
        if payLoad:
            pageCount = int(payLoad['pageCount'])
            punches = payLoad['list']
            store_punches(punches, store)

def read_cx7(config):
    # get token firstly
    # then read recent one week data
    try:
        certification = '.\FGT80FTK22079924.crt'
        if 'certification' in config:
            certification = config['certification']['file']
            print(certification)
        url = "https://api.us.crosschexcloud.com"
        api_pairs = []
        if 'CX7' in config:
            keys = [value.strip() for value in config['CX7'].get('api_keys', '').split(',') if value.strip()]
            secrets = [value.strip() for value in config['CX7'].get('api_secrets', '').split(',') if value.strip()]
            stores = [value.strip() for value in config['CX7'].get('store', '').split(',') if value.strip()]
            if keys and secrets and stores and len(keys) == len(secrets) == len(stores):
                api_pairs = list(zip(keys, secrets, stores))
        if not api_pairs:
            raise ValueError("Missing CX7 api_keys/api_secrets/store in config.ini or counts do not match")

        for index, (apiKey, apiSecret, store) in enumerate(api_pairs, start=1):
            token = get_token(url, apiKey, apiSecret, certification)
            if token:
                endTime = datetime.datetime.now(datetime.timezone.utc).isoformat()
                beginTime = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=-2)).isoformat()
                read_from_cx(url, token, beginTime, endTime, certification, store)
                logging.info(f"Read CX7 data finish (key {index}, store {store})")
            else:
                logging.warning(f"CX7 token is empty (key {index})")
    except Exception as err:
        print(f"error: {err}")
