import csv
import datetime
from helper import DBContext


class CSV:
    def __init__(self, fileName: str):
        self.fileName = fileName
        self.data = []

    def read_from_file(self):
        pass

    def write_to_db(self):
        if not self.data:
            return
        with DBContext() as conn:
            with conn.cursor() as cursor:
                for punch in self.data:
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
                    sql = f"Merge into SysPunch as t using (select '{BtrustID}' as BtrustId, '{year}' as year, '{month}' as month, '{day}' as day, '{hour}' as hour, '{minute}' as minute) as s on s.btrustid = t.btrustid and s.year = t.year and s.month = t.month and s.day = t.day and s.hour = t.hour and s.minute = t.minute when not matched then insert values('{BtrustID}', '{year}', '{month}', '{day}', '{hour}', '{minute}', '0', '10', '0', '1', '0', 'CX7', '{punchDate}', null, null);"
                    cursor.execute(sql)
                print("commit punches")

