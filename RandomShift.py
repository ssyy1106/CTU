import datetime
import functools
import sys
import random
import logging
from snowflake import Snowflake
from helper import DBContext
from UserType import UserType
from SendHREmail import send_hr_random_shift

@functools.cache
def get_hour(userType) -> int:
    try:
        for type in UserType:
            if int(type.name) == int(userType):
                return type.value.description
    except:
        return sys.maxsize
    return sys.maxsize


def get_employees(department: int = 0) -> dict:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            res = {}
            cursor.execute(f"select id, type from sysuser")
            rows = cursor.fetchall()
            for (id, type) in rows:
                hour = get_hour(type)
                res[id] = (type, hour)
            return res


def get_monday(d, weekday = 0):
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0: # Target day already happened this week
        days_ahead += 7
    return (d + datetime.timedelta(days_ahead - 28)).strftime("%Y-%m-%d")

def get_departments()-> list:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select id from sysdepartment where baseisdelete = 0")
            departments = cursor.fetchall()
            return departments

def insert_shift_department(monday, department, sf):
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select id from sysshiftRandom where periodBegin='{monday}' and departmentId = {department}")
            row = cursor.fetchone()
            if not row:
                shiftId = sf.next_id()
                sql = f"insert into sysshiftRandom(id, periodBegin, departmentId) values({shiftId}, '{monday}', {department})"
                cursor.execute(sql)
            else:
                shiftId = row[0]
            return shiftId

def insert_shift_detail(shift, shiftId, sf):
    (userId, totalHours, _, MondayBegin, MondayEnd, MondayTotalHours, 
             TuesdayBegin, TuesdayEnd, TuesdayTotalHours,
             WednesdayBegin, WednesdayEnd, WednesdayTotalHours,
             ThursdayBegin, ThursdayEnd, ThursdayTotalHours,
             FridayBegin, FridayEnd, FridayTotalHours,
             SaturdayBegin, SaturdayEnd, SaturdayTotalHours,
             SundayBegin, SundayEnd, SundayTotalHours, _, _, _) = shift
    with DBContext() as conn:
        with conn.cursor() as cursor:
            newId = sf.next_id()
            sql = (f"insert into sysshiftdetailRandom(id, shiftid, userid, mondaybegin, mondayend, tuesdaybegin, tuesdayend,"
                f" wednesdaybegin, wednesdayend, thursdaybegin, thursdayend, fridaybegin, fridayend, saturdaybegin, saturdayend,"
                f" sundaybegin, sundayend, mondaytotalhours, tuesdaytotalhours, wednesdaytotalhours, thursdaytotalhours, fridaytotalhours, saturdaytotalhours, sundaytotalhours, totalhours)"
                f" values({newId}, {shiftId}, {userId}, '{MondayBegin}', '{MondayEnd}', '{TuesdayBegin}', '{TuesdayEnd}', '{WednesdayBegin}',"
                f" '{WednesdayEnd}', '{ThursdayBegin}', '{ThursdayEnd}', '{FridayBegin}', '{FridayEnd}', '{SaturdayBegin}', '{SaturdayEnd}', '{SundayBegin}', '{SundayEnd}'"
                f" , '{MondayTotalHours}', '{TuesdayTotalHours}', '{WednesdayTotalHours}', '{ThursdayTotalHours}', '{FridayTotalHours}', '{SaturdayTotalHours}', '{SundayTotalHours}', '{totalHours}')")
            cursor.execute(sql)

def insert_shift(shift, department, monday, sf):
    #if not checkShift(monday, department):
    shiftId = insert_shift_department(monday, department, sf)
    insert_shift_detail(shift, shiftId, sf)
    
    
def set_random_shift(monday: str, department: int, employees, sf):
    # 读取one month ago排班表 和对应的员工类型 判断时长，如果超时了，那么处理这个shiftdetail 减去超时的小时数
    with DBContext() as conn:
        with conn.cursor() as cursor:
            sql = (f"select userid, SysShiftDetail.totalHours as totalHours, shiftId, MondayBegin, MondayEnd, MondayTotalHours,"
                f"TuesdayBegin, TuesdayEnd, TuesdayTotalHours,"
                f"WednesdayBegin, WednesdayEnd, WednesdayTotalHours,"
                f"ThursdayBegin, ThursdayEnd, ThursdayTotalHours,"
                f"FridayBegin, FridayEnd, FridayTotalHours,"
                f"SaturdayBegin, SaturdayEnd, SaturdayTotalHours,"
                f"SundayBegin, SundayEnd, SundayTotalHours,"
                f"RealName, departmentName, btrustId"
                f"  from SysShift inner join SysShiftDetail on SysShift.id = SysShiftDetail.ShiftId "
                f"  inner join sysuser on sysuser.id=sysshiftdetail.userid "
                f"  inner join sysdepartment on SysShift.departmentid=sysdepartment.id "
                f"where PeriodBegin='{monday}' and SysShift.departmentid={department}")
            cursor.execute(sql)
            rows = cursor.fetchall()
            res = []
            for shift in rows:
                userId = shift[0]
                totalHours = 0
                if shift[1]:
                    totalHours = float(shift[1])
                # 超时了，需要减去某些小时数 random 选择一天 把当天小时数清空，再计算，如果还超，就继续循环处理
                days = []
                while totalHours and totalHours > employees[userId][1]:
                    day = random.randint(0, 6)
                    days.append(day)
                    index = 5 + day * 3
                    if shift[index]:
                        totalHours -= float(shift[index])
                    shift[index] = '0'
                    shift[index - 1] = "0:00"
                    shift[index - 2] = "0:00"
                shift[1] = str(totalHours)
                insert_shift(shift, department, monday, sf)
                dic = {"DepartmentId": department, "BtrustId": shift[26], "DepartmentName": shift[25], "RealName": shift[24], "TotalHours": shift[1], 
                    "MondayBegin": shift[3], "MondayEnd": shift[4], "MondayTotalHours": shift[5],
                    "TuesdayBegin": shift[6], "TuesdayEnd": shift[7], "TuesdayTotalHours": shift[8],
                    "WednesdayBegin": shift[9], "WednesdayEnd": shift[10], "WednesdayTotalHours": shift[11],
                    "ThursdayBegin": shift[12], "ThursdayEnd": shift[13], "ThursdayTotalHours": shift[14],
                    "FridayBegin": shift[15], "FridayEnd": shift[16], "FridayTotalHours": shift[17],
                    "SaturdayBegin": shift[18], "SaturdayEnd": shift[19], "SaturdayTotalHours": shift[20],
                    "SundayBegin": shift[21], "SundayEnd": shift[22], "SundayTotalHours": shift[23]}
                res.append(dic)
            return res


def check_shift_random(monday: str, department: int) -> bool:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select id from sysshiftRandom where periodBegin='{monday}' and departmentId = {department}")
            row = cursor.fetchone()
            if not row:
                return False
            return True

def send_hr_email_shift(config):
    try:
        # 读取所有员工 和类型 c类的不处理 存在dict里面
        employees = get_employees()
        monday = get_monday(datetime.datetime.now())
        sf = Snowflake(1, 1)
        shifts = []
        # 存入专门数据库表random表里，并给HR发送邮件
        for department in get_departments():
            department = department[0]
            if not check_shift_random(monday, department):
                shifts.extend(set_random_shift(monday, department, employees, sf))
        if shifts:
            send_hr_random_shift(shifts, config, monday)
        return 0
    except Exception as e:
        logging.info(f"send HR email shift error {e}")
        return 0

# Backward-compatible aliases
