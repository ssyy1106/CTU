import datetime
from snowflake import Snowflake
from helper import DBContext
from btrust_common.types import Shift, Punch, PunchProblem
from btrust_common.core import get_person_hours, get_department_hours


def get_all_department_ids() -> dict:
    res = {}
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select id, parentid from sysdepartment")
            items = cursor.fetchall()
            for (id, parentId) in items:
                res[id] = parentId
            return res
        
def get_hq(HQName: str) -> int:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select id from sysdepartment where parentId=0")
            row = cursor.fetchone()
            if not row:
                return 0

            cursor.execute(f"select id from sysdepartment where parentId={row[0]} and departmentName='{HQName}'")
            row = cursor.fetchone()
            if not row:
                return 0
            return row[0]


def get_department_id_by_name(HQName: str) -> int:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select id from sysdepartment where departmentName='{HQName}'")
            row = cursor.fetchone()
            if not row:
                return 0
            return row[0]


def get_departments(headQuarter: int)-> list:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            res = []
            cursor.execute(f"select id, parentId from sysdepartment")
            dic = {}
            departments = cursor.fetchall()
            for (departmentId, parentId) in departments:
                dic[departmentId] = parentId
            for (departmentId, parentId) in departments:
                father = dic[departmentId]
                while father != 0 and father != headQuarter:
                    father = dic[father]
                if father == headQuarter:
                    res.append(departmentId)
            return res

def check_shift(monday: str, department: int) -> bool:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select id from sysshift where periodBegin='{monday}' and departmentId = {department}")
            row = cursor.fetchone()
            if not row:
                return False
            return True
        
def next_weekday(d, weekday = 0):
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0: # Target day already happened this week
        days_ahead += 7
    return d + datetime.timedelta(days_ahead)

def get_employees(department: int) -> list:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select id from sysuser where baseisdelete=0 and departmentId = {department} and (isshift is null or isshift=1) and (btruststatus is null or btruststatus=0)")
            rows = cursor.fetchall()
            return [str(item[0]) for item in rows]

def get_all_employees() -> list:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select btrustid from sysuser")
            rows = cursor.fetchall()
            return [str(item[0]) for item in rows]

def get_shift_employees(monday: str, employeeIds: list) -> list:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            sql = f"select userid from SysShift inner join SysShiftDetail on sysshift.id = SysShiftDetail.ShiftId where PeriodBegin='{monday}' and SysShiftDetail.UserId in ({",".join(employeeIds)})"
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [str(item[0]) for item in rows]

def insert_shift_db(monday, department, ids, sf):
    with DBContext() as conn:
        with conn.cursor() as cursor:
            shiftId = sf.next_id()
            sql = f"insert into sysshift(id, periodBegin, departmentId) values({shiftId}, '{monday}', {department})"
            cursor.execute(sql)
            for userId in ids:
                newId = sf.next_id()
                workbegin = "09:00"
                workend = "18:00"
                workhours = "8"
                weekendbegin = "00:00"
                weekendend = "00:00"
                weekendhours = "0"
                sql = (f"insert into sysshiftdetail(id, shiftid, userid, mondaybegin, mondayend, tuesdaybegin, tuesdayend,"
                f" wednesdaybegin, wednesdayend, thursdaybegin, thursdayend, fridaybegin, fridayend, saturdaybegin, saturdayend,"
                f" sundaybegin, sundayend, mondaytotalhours, tuesdaytotalhours, wednesdaytotalhours, thursdaytotalhours, fridaytotalhours, saturdaytotalhours, sundaytotalhours, totalhours)"
                f" values({newId}, {shiftId}, {userId}, '{workbegin}', '{workend}', '{workbegin}', '{workend}', '{workbegin}',"
                f" '{workend}', '{workbegin}', '{workend}', '{workbegin}', '{workend}', '{weekendbegin}', '{weekendend}', '{weekendbegin}', '{weekendend}'"
                f" , '{workhours}', '{workhours}', '{workhours}', '{workhours}', '{workhours}', '{weekendhours}', '{weekendhours}', '40')")
                cursor.execute(sql)
    
def insert_shift(monday: str, department: int, sf):
    employeeIds = get_employees(department)
    #print(f"employees: {employeeIds}")
    if employeeIds:
        shiftEmployeeIds = get_shift_employees(monday, employeeIds)
        ids = set(employeeIds).difference(set(shiftEmployeeIds))
        #print(f"ids: {ids}")
        if ids:
            insert_shift_db(monday, department, ids, sf)
            
def set_hq_shift(config):
    if 'HQ' in config:
        HQName = config['HQ']['department']
        headQuarter = get_hq(HQName)
        departments = get_departments(headQuarter)
        #print(f"departments: {departments}")
        monday = next_weekday(datetime.datetime.now()).strftime("%Y-%m-%d")
        #print(f"monday is: {monday}")
        sf = Snowflake(1, 1)
        for department in departments:
             if not check_shift(monday, department):
                 #print(f"insert begin department: {department}")
                 insert_shift(monday, department, sf)

def get_work_days(first_day):
    # 根据first_day计算工作日
    current_day = first_day
    while current_day < datetime.datetime.now().date() - datetime.timedelta(days=14):
        yield current_day
        current_day += datetime.timedelta(days=1)

def has_calculated_hours(conn, eachday):
    with conn.cursor() as cursor:
        sql = f"select count(*) from workhour_calc_status where WorkDate='{eachday}'"
        cursor.execute(sql)
        result = cursor.fetchone()
        return result[0] > 0

def insert_workhour_calc_status(conn, eachday):
    with conn.cursor() as cursor:
        sql = f"insert into workhour_calc_status (WorkDate, status) values ('{eachday}', 'processing')"
        cursor.execute(sql)

def insert_employee_day_hours(conn, eachday, hours, departmentId):
    with conn.cursor() as cursor:
        sql = """
        INSERT INTO SysEmployeeDayHours (WorkDate, Btrustid, Hours, DepartmentId)
        VALUES (?, ?, ?, ?)
        """
        for btrust_id, hour in hours.items():
            cursor.execute(sql, eachday, btrust_id, hour, departmentId)

def calculate_employee_day_hours(config):
    if 'EmployeeDayHours' in config:
        first_day = config['EmployeeDayHours']['firstday']
        with DBContext() as conn:
            # 计算员工每日工时的逻辑
            for eachday in get_work_days(datetime.date.fromisoformat(first_day)):
                # 判断日志，是否该天计算过，并且计算成功
                # 如果计算过且成功，则跳过
                if has_calculated_hours(conn, eachday):
                    continue
                dic_dept = get_all_department_ids()
                for dept_id, v in get_department_hours(conn, dic_dept.keys(), eachday.strftime("%Y-%m-%d"), eachday.strftime("%Y-%m-%d")).items():
                    dept_hours = dict(v["persons"])
                    insert_employee_day_hours(conn, eachday, dept_hours, dept_id)
                # 写入数据库日志表，并且写入SysEmployeeDayHours表
                insert_workhour_calc_status(conn, eachday)
