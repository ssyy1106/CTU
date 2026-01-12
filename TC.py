import pathlib
import logging
from helper import DBContext

def read_punch():
    files = read_from_files()
    for i, file in enumerate(files):
        [data, fileName, fileModify] = file
        insert_db(data, fileName, fileModify)
    logging.info('Read enterprise data finish')

def read_from_files() -> None:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            fileNames = []
            for i in range(1, 32):
                fileNames.append("TC1000" + str(i).zfill(2) + ".txt")
            files = []
            for fileName in fileNames:
                # check file modify time
                # filePath = "C:/WangPeng/Ctu/Enterprise/Data/" + fileName
                filePath = "//172.16.20.21/Enterprise Suite/Enterprise/Data/" + \
                    fileName
                fileModify = str(pathlib.Path(filePath).stat().st_mtime)
                print(filePath)
                print(fileModify)
                # search file in DB to check whether import that file already
                cursor.execute(f"select * from SysPunchFile where FileName="
                            f"'{fileName}'and ModifyTime='{fileModify}'")
                row = cursor.fetchone()
                if row:
                    continue
                with open(filePath, "r") as f:
                    print(f"open file: {fileName}")
                    data = f.readlines()
                    files.append([data, fileName, fileModify])
    return files

def insert_db(lines: list[str], fileName: str, fileModify) -> None:
    items = len(lines)
    if not items:
        return
    data = lines[0].strip('\n').split(",")
    firstYear, firstMonth, firstDay, firstHour, firstMinute = data[10].strip('\n'), data[8].strip(), data[9].strip(), data[6].strip(), data[7].strip()
    with DBContext() as conn:
        with conn.cursor() as cursor:
            # check whether import this file already
            cursor.execute(f"select * from SysPunchFile where FileName='{fileName}' and Items={items} and FirstYear='{firstYear}' and FirstMonth='{firstMonth}' and FirstDay='{firstDay}' and FirstHour='{firstHour}' and FirstMinute='{firstMinute}'")
            row = cursor.fetchone()
            if row:
                return
            logging.info(f"insert puncher into db filename: {fileName} fileModify: {fileModify} row: {str(row)}")
            logging.info(f"select * from SysPunchFile where FileName='{fileName}' and Items={items} and FirstYear='{firstYear}' and FirstMonth='{firstMonth}' and FirstDay='{firstDay}' and FirstHour='{firstHour}' and FirstMinute='{firstMinute}'")
            cursor.execute(f"select * from SysPunchFile where FileName='{fileName}'")
            row2 = cursor.fetchone()
            logging.info(f"insert puncher into db filename: {fileName} fileModify: {fileModify} oldrow: {str(row2)}")
            for i, line in enumerate(lines):
                line = line.strip('\n')
                data = line.split(",")
                if i == 0:
                    firstYear, firstMonth, firstDay, firstHour, firstMinute = data[10].strip('\n'), data[8].strip(), data[9].strip(), data[6].strip(), data[7].strip()
                data = data + [data[10].strip('\n')+"-"+data[8].strip()+"-"+data[9].strip()]
                sql = "INSERT INTO SysPunch(HandRec, SiteNumber, ClockNumber, ClockCode, BtrustId, WorkCode, Hour, Minute, Month, Day, Year, PunchDate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.execute(sql, *data)
            # insert in to DB of file modify time
            sql = f"Merge into SysPunchFile as t using (select '{fileName}' as FileName, '{items}' as Items) as s on s.FileName = t.FileName when not matched then insert values( '{fileName}', '{fileModify}', {items}, '{firstYear}', '{firstMonth}', '{firstDay}', '{firstHour}', '{firstMinute}') when matched then update set ModifyTime = '{fileModify}', items={items}, firstYear='{firstYear}', firstMonth='{firstMonth}', firstDay='{firstDay}', firstHour='{firstHour}', firstMinute='{firstMinute}';"
            cursor.execute(sql)

# Backward-compatible aliases
