import csv
import datetime
from datetime import date
import calendar
import functools
import logging
import os
from collections import defaultdict
from io import StringIO, BytesIO
from helper import DBContext
from SendingEmail import send_email, send_email_items, send_missing_punch_email
from btrust_common.types import Shift, Punch, PunchProblem
from btrust_common.core import get_person_hours
from HQShift import get_department_id_by_name, get_departments
from jinja2 import Environment, FileSystemLoader, select_autoescape
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
import pathlib


def get_hr_emails(HRrole: str) -> list:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select email from SysUser inner join SysUserBelong on SysUser.id=SysUserBelong.UserId inner join SysRole on SysRole.Id=SysUserBelong.BelongId where RoleName='{HRrole}'")
            return cursor.fetchall()

@functools.cache
def get_all_department_ids() -> dict:
    res = {}
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select id, parentid from sysdepartment")
            items = cursor.fetchall()
            for (id, parentId) in items:
                res[id] = parentId
            return res

@functools.cache
def get_store_name(id: str) -> str:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select departmentName from sysdepartment where id = " + str(id))
            name = cursor.fetchone()
            if name:
                return name[0]
            return ""

@functools.cache
def get_store(departmentId: int) -> str:
    try:
        deps = get_all_department_ids()
        son = departmentId
        store = deps[departmentId]
        father = deps[store]
        grandFather = deps[father]
        while grandFather != 0:
            son = store
            store = father
            father = grandFather
            grandFather = deps[father]
        return son
    except Exception as e:
        return son

def get_visa_expiration() -> list:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select WorkingVisaexpiration,BtrustId,RealName, departmentName, departmentId from SysUser inner join sysdepartment on sysuser.departmentid=sysdepartment.id" +
                        " where WorkingVisaexpiration is not null and LEN(WorkingVisaexpiration) > 0 and SysUser.BaseIsDelete=0 and (BtrustStatus is null or BtrustStatus <> 2)")
            return cursor.fetchall()

def get_sin_expiration() -> list:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select sinnumberexpiration,BtrustId,RealName, departmentName, departmentId from SysUser inner join sysdepartment on sysuser.departmentid=sysdepartment.id" +
                        " where sinnumberexpiration is not null and LEN(sinnumberexpiration) > 0 and SysUser.BaseIsDelete=0 and (BtrustStatus is null or BtrustStatus <> 2)")
            return cursor.fetchall()

def get_done_persons(period) -> set:
    # 读取所有在职员工，同时没发送过提醒的，放到dic里面，读取这些员工的打卡记录，排班记录和打卡问题记录，计算上班时间，第一次会比较慢
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select btrustid from sysBenefitNotificationlog where hours='{period}'")
            return set(person[0] for person in cursor.fetchall())

def get_hire_done_persons(period) -> set:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select btrustid from sysHireNotificationlog where days={period}")
            return set(person[0] for person in cursor.fetchall())

def generate_message(period: str, BtrustId: str, realName: str)-> str:
    return f"Employee {realName} ({BtrustId}) sin number expiration is {period}."

def add_log(period: str, BtrustId: str, type = 'SinExpirationNotification', mp_type = 1, mp_email_content = '')-> bool:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            if type == 'SinExpirationNotification':
                cursor.execute(f"select * from syssinexpirationnotificationlog where period='{period}' and btrustid='{BtrustId}'")
                row = cursor.fetchone()
                if row:
                    return False
                cursor.execute(f"insert into syssinexpirationnotificationlog(BtrustId, period) values('{BtrustId}', '{period}')")
            elif type == 'VisaExpirationNotification':
                cursor.execute(f"select * from sysvisaexpirationnotificationlog where period='{period}' and btrustid='{BtrustId}'")
                row = cursor.fetchone()
                if row:
                    return False
                cursor.execute(f"insert into sysvisaexpirationnotificationlog(BtrustId, period) values('{BtrustId}', '{period}')")
            elif type == 'hirelog':
                cursor.execute(f"select * from syshirenotificationlog where days={period} and btrustid='{BtrustId}'")
                row = cursor.fetchone()
                if row:
                    return False
                cursor.execute(f"insert into syshirenotificationlog(BtrustId, days) values('{BtrustId}', {period})")
            elif type == 'mp':
                cursor.execute(f"select * from SysMPNotificationLog where date='{period}' and btrustid='{BtrustId}'")
                row = cursor.fetchone()
                if row:
                    return False
                cursor.execute(f"insert into SysMPNotificationLog(BtrustId, date, type,emailcontent,senddate) values('{BtrustId}', '{period}', {mp_type}, '{mp_email_content}', '{datetime.datetime.now().strftime('%Y-%m-%d')}')")
            else:
                cursor.execute(f"select * from SysBenefitNotificationLog where hours={period} and btrustid='{BtrustId}'")
                row = cursor.fetchone()
                if row:
                    return False
                cursor.execute(f"insert into SysBenefitNotificationLog(BtrustId, hours) values('{BtrustId}', {period})")
            return True

def send_email_to_hr(templateDIR: str, message: list, HREmails: list, fileName = "Btrust_alert", type = 1):
    for email in HREmails:
        # send_email("\n".join(message), email)
        send_email_items(templateDIR, message, email, fileName, type)

def send_hr_email_working_visa(config):
    try:
        if 'VisaEmail' in config and 'Visaperiods' in config and 'Template' in config:
            templateDIR = config['Template']['dir']
            # HRrole = config['HRrole']['role']
            # HREmails = [item[0] for item in get_hr_emails(HRrole) if len(item[0]) > 0]
            HREmails = config['VisaEmail']['email'].split(',')
            if not HREmails:
                exit
            periods = config['Visaperiods']['periods']
            # 读取所有working expiration date 过期日在 当天加上 periods之前的员工，同时查看日志，是否发送过，发送过不再发送，例如45天发一次，15天发一次，先是发45天那次，发完写日志userid，period，senddate，
            visaExpirations = get_visa_expiration()
            message = []
            sendFlag = False
            visaExpirations.sort(key = lambda x: (x[0], x[3], x[1]))
            for exp in visaExpirations:
                expirationDate, BtrustId, realName, departmentName, departmentId = exp
                store = get_store_name(get_store(departmentId))
                for period in periods.split(','):
                    if  datetime.datetime.now() + datetime.timedelta(days=int(period)) >= datetime.datetime.strptime(expirationDate, '%Y-%m-%d'):
                        p = datetime.datetime.strptime(expirationDate, '%Y-%m-%d')
                        if add_log(p.strftime("%Y-%m-%d"), BtrustId, 'VisaExpirationNotification'):
                            sendFlag = True
                        # message.append(generate_message(p.strftime("%Y-%m-%d"), BtrustId, realName))
                        dic = {"Date": p.strftime("%Y-%m-%d"), "BtrustId": BtrustId, "RealName": realName, "DepartmentName": departmentName, "Store": store}
                        message.append(dic)
            # print(f"sendFlag: {sendFlag} message: {message} HREmails: {HREmails}")
            if sendFlag and message and HREmails:
                send_email_to_hr(templateDIR, message, HREmails, 'Working Visa ', 5)
    except Exception as e:
        logging.info(f"send HR email error {e}")

def send_hr_email_sin(config):
    try:
        if 'SINEmail' in config and 'SINperiods' in config and 'Template' in config:
            templateDIR = config['Template']['dir']
            # HRrole = config['HRrole']['role']
            # HREmails = [item[0] for item in get_hr_emails(HRrole) if len(item[0]) > 0]
            HREmails = config['SINEmail']['email'].split(',')
            if not HREmails:
                exit
            periods = config['SINperiods']['periods']
            # 读取所有sin expiration date 过期日在 当天加上 periods之前的员工，同时查看日志，是否发送过，发送过不再发送，例如45天发一次，15天发一次，先是发45天那次，发完写日志userid，period，senddate，
            sinExpirations = get_sin_expiration()
            message = []
            sendFlag = False
            sinExpirations.sort(key = lambda x: (x[0], x[3], x[1]))
            for exp in sinExpirations:
                expirationDate, BtrustId, realName, departmentName, departmentId = exp
                store = get_store_name(get_store(departmentId))
                for period in periods.split(','):
                    if  datetime.datetime.now() + datetime.timedelta(days=int(period)) >= datetime.datetime.strptime(expirationDate, '%Y-%m-%d'):
                        p = datetime.datetime.strptime(expirationDate, '%Y-%m-%d')
                        if add_log(p.strftime("%Y-%m-%d"), BtrustId):
                            sendFlag = True
                        # message.append(generate_message(p.strftime("%Y-%m-%d"), BtrustId, realName))
                        dic = {"Date": p.strftime("%Y-%m-%d"), "BtrustId": BtrustId, "RealName": realName, "DepartmentName": departmentName, "Store": store}
                        message.append(dic)
            # print(f"sendFlag: {sendFlag} message: {message} HREmails: {HREmails}")
            if sendFlag and message and HREmails:
                send_email_to_hr(templateDIR, message, HREmails)
    except Exception as e:
        logging.info(f"send HR email error {e}")

def get_all_persons(non_departments: list) -> set:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            if not non_departments:
                cursor.execute(f"select BtrustId from sysuser where baseisdelete=0 and (btruststatus is null or btruststatus=0)")
            else:
                departments = '(' + ",".join(str(d) for d in non_departments) + ')'
                cursor.execute(f"select BtrustId from sysuser where baseisdelete=0 and (btruststatus is null or btruststatus=0) and departmentid not in {departments}")
            rows = cursor.fetchall()
            return set(item[0] for item in rows)

def get_hire_persons(period) -> set:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            period = (datetime.datetime.now() + datetime.timedelta(days=-int(period))).strftime("%Y-%m-%d")
            cursor.execute(f"select BtrustId from sysuser where baseisdelete=0 and (btruststatus is null or btruststatus=0) and hiredate is not null and hiredate <> '' and hiredate <= '{period}'")
            rows = cursor.fetchall()
            return set(item[0] for item in rows)

def get_date(periodBegin, count) -> str:
    monday = datetime.datetime.strptime(periodBegin, '%Y-%m-%d')
    return (monday + datetime.timedelta(days=count)).strftime("%Y-%m-%d")

def get_shifts(employees):
    with DBContext() as conn:
        with conn.cursor() as cursor:
            dic = defaultdict(list)
            if not employees:
                return dic
            btrustids = "','".join(employees)
            sql = f"select btrustid, periodbegin, MondayBegin, mondayend, TuesdayBegin, tuesdayend, wednesdayBegin, wednesdayend, thursdayBegin, thursdayend, fridayBegin, fridayend, saturdayBegin, saturdayend, sundayBegin, sundayend, lunchminute from sysshift inner join sysdepartment on departmentid = sysdepartment.id inner join SysShiftDetail on sysshift.id=SysShiftDetail.shiftid inner join sysuser on sysuser.id = userid where btrustid in ('{btrustids}')"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                for i in range(7):
                    date = get_date(row[1], i)
                    shift = Shift(date, row[(i + 1) * 2], row[(i + 1) * 2 + 1], row[-1])
                    dic[row[0]].append(shift)
            return dic

def get_time(row) -> str:
    hour, minute = row[2] if row[2] else "00", row[3] if row[3] else "00"
    if len(hour) == 1:
        hour = '0' + hour
    if len(minute) == 1:
        minute = '0' + minute
    return hour + ':' + minute

def get_punches(employees):
    with DBContext() as conn:
        with conn.cursor() as cursor:
            dic = defaultdict(list)
            if not employees:
                return dic
            btrustids = "','".join(employees)
            sql = f"select btrustid, punchdate, hour, minute from syspunch where btrustid in ('{btrustids}')"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                date = row[1]
                punch = Punch(date, get_time(row))
                dic[row[0]].append(punch)
            return dic

def get_punch_problems(employees):
    with DBContext() as conn:
        with conn.cursor() as cursor:
            dic = defaultdict(list)
            if not employees:
                return dic
            btrustids = "','".join(employees)
            sql = f"select btrustid, punchdate, realtotalhours from syspunchproblem where btrustid in ('{btrustids}')"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                punch = PunchProblem(row[1], row[2])
                dic[row[0]].append(punch)
            return dic

def get_minutes(s: str) -> int:
    if len(s) != 5 and s[2] != ':':
        return -1
    return int(s[:2]) * 60 + int(s[3:])

def calculate(punchBegin, punchEnd, shiftBegin, shiftEnd) -> int:
    # 判断开始时间 大于shiftbegin 10分钟以内，算等于
    # 没排班的话，规整到后面的15分钟整数
    if punchBegin <= shiftBegin + 10 and punchBegin > shiftBegin:
        punchBegin = shiftBegin
    elif punchBegin >= shiftBegin - 30 and punchBegin <= shiftBegin:
        punchBegin = shiftBegin
    else:
        remain = punchBegin % 15
        punchBegin += (15 - remain) % 15
    if punchEnd >= shiftEnd - 5 and punchEnd <= shiftEnd:
        punchEnd = shiftEnd
    elif punchEnd > shiftEnd:
        giveIn = (punchEnd + 5) // 30
        punchEnd = giveIn * 30
    else:
        remain = punchEnd % 15
        punchEnd -= remain
    return punchEnd - punchBegin

def check_lunch_time(minutes, lunchMinute) -> int:
    if lunchMinute == 60:
        if minutes >= 5.5 * 60:
            return minutes - 60
    elif lunchMinute == 30:
        if minutes >= 5.5 * 60 and minutes < 10 * 60:
            return minutes - 30
        elif minutes >= 10 * 60:
            return minutes - 60
    return minutes


def get_total_hours(punches: list[Punch], shift = None) -> int:
    lunchMinute = 30
    if shift:
        lunchMinute = shift.lunchMinute
    punchBegin, punchEnd = -1, -1
    for punch in punches:
        time = get_minutes(punch.time)
        if time != -1 and (punchBegin == -1 or punchBegin > time):
            punchBegin = time
        if time != -1 and (punchEnd == -1 or punchEnd < time):
            punchEnd = time
    if punchBegin == -1 or punchEnd == -1:
        return 0
    minutes = punchEnd - punchBegin
    if shift:
        minutes = calculate(punchBegin, punchEnd, get_minutes(shift.begin), get_minutes(shift.end))
    minutes = check_lunch_time(minutes, lunchMinute)
    return round(minutes / 60, 2)

def calculate_hours(shifts: list, punches: list, punchProblems: list) -> int:
    dic_shift = {}
    dic_punch = defaultdict(list)
    dic_punch_problem = {}
    dates = set()
    totalHours = 0
    for shift in shifts:
        dic_shift[shift.date] = shift
    for punch in punches:
        dic_punch[punch.date].append(punch)
        dates.add(punch.date)
    for punch_problem in punchProblems:
        dic_punch_problem[punch_problem.date] = punch_problem
        dates.add(punch_problem.date)
    for date in dates:
        if date in dic_punch_problem:
            totalHours += dic_punch_problem[date].totalHours
        elif date in dic_punch:
            punches = dic_punch[date]
            if date in dic_shift:
                totalHours += get_total_hours(punches, dic_shift[date])
            else:
                totalHours += get_total_hours(punches)
    return totalHours

@functools.cache
def get_user_info(btrustId) -> tuple:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            sql = f"select departmentid, realName, hiredate from sysuser where btrustid = '{btrustId}' "
            cursor.execute(sql)
            row = cursor.fetchone()
            if row:
                return (row[1], row[0], row[2])
            return (0, 0, 0)

def second_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """
    返回 year-month 里第 2 个 weekday 的日期
    weekday: 1-7 表示 周一-周日 (跟 date.isoweekday 一致)
    """
    cal = calendar.Calendar()
    count = 0
    for d in cal.itermonthdates(year, month):
        if d.month == month and d.isoweekday() == weekday:
            count += 1
            if count == 2:
                return d
    raise ValueError("This should not happen.")


def month_start_end(year: int, month: int):
    """给出某年某月的第一天和最后一天"""
    first = date(year, month, 1)
    _, days = calendar.monthrange(year, month)
    last = date(year, month, days)
    return first, last


def month_iter(start_year: int, start_month: int, end_year: int, end_month: int):
    """从起始年月一路迭代到结束年月（包含）"""
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1

def get_months(first_month: str, send_day: int):
    # --- 把字符串转成 date 对象 ---
    first_dt = datetime.datetime.strptime(first_month, "%Y-%m-%d").date()
    today = date.today()
    current_year, current_month = today.year, today.month

    # 当月第二个 send_day
    second_day_this_month = second_weekday_of_month(current_year, current_month, send_day)

    # 上一个月
    if current_month == 1:
        prev_year = current_year - 1
        prev_month = 12
    else:
        prev_year = current_year
        prev_month = current_month - 1

    # 决定截止月份
    if today >= second_day_this_month:
        end_year, end_month = prev_year, prev_month
    else:
        if prev_month == 1:
            end_year = prev_year - 1
            end_month = 12
        else:
            end_year = prev_year
            end_month = prev_month - 1

    if (end_year, end_month) < (first_dt.year, first_dt.month):
        return

    for y, m in month_iter(first_dt.year, first_dt.month, end_year, end_month):
        first_day, last_day = month_start_end(y, m)
        yield first_day, last_day

def mp_send_times(btrust_id) -> int:
    with DBContext() as conn:
        with conn.cursor() as cursor:
            sql = f"select count(1) from SysMPNotificationLog where btrustid= '{btrust_id}'"
            cursor.execute(sql)
            row = cursor.fetchone()
            if row:
                return row[0]
            return 0

def get_mp(first_month, send_day: int):
    with DBContext() as conn:
        with conn.cursor() as cursor:
            for period_start, period_end in get_months(first_month, send_day):
                dic_person = defaultdict(list)
                sql = (
                    f"select syspunchproblem.btrustid, punchdate, syspunchproblem.Remark from syspunchproblem inner join sysuser"
                    f" on sysuser.id=syspunchproblem.userid where syspunchproblem.DailyReason=4 and syspunchproblem.status=9 "
                    f" and punchdate >= '{period_start.strftime('%Y-%m-%d')}' and punchdate <= '{period_end.strftime('%Y-%m-%d')}'"
                    f" and sysuser.baseisdelete=0 and sysuser.btruststatus<>2"
                )
                cursor.execute(sql)
                rows = cursor.fetchall()
                for row in rows:
                    dic_person[row[0]].append(row)
                for btrust_id, items in dic_person.items():
                    if len(items) < 3:
                        continue
                    send_time = min(3, mp_send_times(btrust_id)+ 1)
                    punch_dates = []
                    for item in items:
                        punch_date = item[1]
                        if hasattr(punch_date, "strftime"):
                            punch_dates.append(punch_date.strftime('%Y-%m-%d'))
                        else:
                            punch_dates.append(str(punch_date))
                    punch_dates_str = ";".join(sorted(set(punch_dates)))
                    if not add_log(period_start.strftime('%Y-%m-%d'), btrust_id, 'mp', send_time, punch_dates_str):
                        continue
                    real_name, department_id, _ = get_user_info(btrust_id)
                    department = get_store_name(department_id)
                    store = get_store_name(get_store(department_id))
                    incidents = []
                    warning_level = "Written Reminder"
                    if send_time == 2:
                        warning_level = "Written Warning"
                    elif send_time >= 3:
                        warning_level = "Final Warning"
                    for item in items:
                        dic = {
                            "BtrustID": btrust_id,
                            "RealName": real_name,
                            "WarningLevel": warning_level,
                            "Date": item[1],
                            "Store": store,
                            "Department": department,
                            "Note": item[2],
                        }
                        incidents.append(dic)
                    context = {
                        'period_start': period_start.strftime('%Y-%m-%d'),
                        'period_end': period_end.strftime('%Y-%m-%d'),
                        'name': real_name,
                        'real_name': real_name,
                        'btrust_id': btrust_id,
                        'store': store,
                        'department': department,
                        'action_url': f"http://172.16.20.50:8025/"
                    }
                    yield incidents, context, send_time

def get_benefit_notification(period, non_departments) -> list[dict]:
    message = []
    employees = get_all_persons(non_departments) - get_done_persons(period)
    if not employees:
        return message
    with DBContext() as conn:
        dic_hours = get_person_hours(conn, employees=list(employees), periodBegin='1999-01-01', periodEnd='2099-12-31')
        for btrustId in employees:
            hours = dic_hours.get(btrustId, 0)
            if hours >= int(period):
                real_name, department_id, _ = get_user_info(btrustId)
                if not real_name or not department_id:
                    continue
                department = get_store_name(department_id)
                store = get_store_name(get_store(department_id))
                
                dic = {"BtrustId": btrustId, "WorkingHours": hours, "RealName": real_name, "Store": store, "DepartmentName": department}
                message.append(dic)
                add_log(int(period), btrustId, 'benefitlog')
        return message

def get_hire_notification(period) -> list[dict]:
    try:
        message = []
        employees = get_hire_persons(period) - get_hire_done_persons(period)
        for btrustId in employees:
            real_name, department_id, hireDate = get_user_info(btrustId)
            department = get_store_name(department_id)
            store = get_store_name(get_store(department_id))
            dic = {"BtrustId": btrustId, "HiringDate": hireDate, "RealName": real_name, "Store": store, "DepartmentName": department}
            message.append(dic)
            add_log(int(period), btrustId, 'hirelog')
        return message
    except Exception as e:
        
        print(e)

def send_hr_email_benefit(config):
    # 先读取所有在职员工，判断是否发送过提醒，留下没发送过提醒的员工列表
    # 读取这些人的排班表，punch记录，punch problem处理记录
    # 配置文件配置 达到多少小时发送提醒
    try:
        if 'BenefitEmail' in config and 'Benefit' in config and 'Template' in config:
            nonDepartments = config['Benefit']['nondepartment'].split(',')
            non_departments = []
            for d in nonDepartments:
                department_id = get_department_id_by_name(d)
                non_departments.extend(get_departments(department_id))
            templateDIR = config['Template']['dir']
            periods = config['Benefit']['periods']
            # HRrole = config['HRrole']['role']
            # HREmails = [item[0] for item in get_hr_emails(HRrole) if len(item[0]) > 0]
            HREmails = config['BenefitEmail']['email'].split(',')
            if not HREmails or not periods:
                exit
            # 读取所有工作了超过period 时间的员工，同时查看日志，是否发送过，发送过不再发送，例如840hours发一次，15hours发一次，发完写日志userid，period，senddate，
            
            for period in periods.split(','):
                message = []
                message.extend(get_benefit_notification(period, non_departments))
                if message:
                    send_email_to_hr(templateDIR, message, HREmails, 'Benefit hours ' + period, 3)
    except Exception as e:
        logging.info(f"send HR Benefit email error {e}")

def send_hr_email_hire(config):
    # 读取所有在职员工hire date超过period并且没有发送过提醒的人
    try:
        if 'HireEmail' in config and 'Hire' in config and 'Template' in config:
            templateDIR = config['Template']['dir']
            periods = config['Hire']['periods']
            # HRrole = config['HRrole']['role']
            # HREmails = [item[0] for item in get_hr_emails(HRrole) if len(item[0]) > 0]
            HREmails = config['HireEmail']['email'].split(',')
            if not HREmails or not periods:
                exit
            # 读取所有在职员工hire date超过period并且没有发送过提醒的人，发完写日志userid，period，senddate，
            
            for period in periods.split(','):
                message = []
                message.extend(get_hire_notification(period))
                if message:
                    send_email_to_hr(templateDIR, message, HREmails, 'Hiring Date '+ period, 4)
    except Exception as e:
        print(f"error: {e}")
        logging.info(f"send HR Hire email error {e}")

def _sanitize_filename(name: str) -> str:
    cleaned = []
    for ch in name:
        if ch.isalnum() or ch in ('-', '_'):
            cleaned.append(ch)
        else:
            cleaned.append('_')
    result = "".join(cleaned).strip('_')
    return result or "attachment"

def _render_missing_punch_html(templateDIR: str, template_file: str, incidents: list, context: dict) -> str:
    env = Environment(
        loader=FileSystemLoader(templateDIR),
        autoescape=select_autoescape(['html', 'xml'])
    )
    template = env.get_template(template_file)
    snap_time = datetime.datetime.now()
    merged_ctx = {
        "snap_time": snap_time,
        "work_items": incidents,
        "incidents": incidents,
        "item_count": len(incidents),
        "templateDIR": templateDIR,
    }
    merged_ctx.update(context or {})
    return template.render(**merged_ctx)


def _html_to_pdf_bytes(html_content: str, base_url: str | None = None) -> bytes:
    try:
        from weasyprint import HTML
        pdf_bytes = HTML(string=html_content, base_url=base_url).write_pdf()
        return pdf_bytes
    except Exception:
        try:
            from xhtml2pdf import pisa
            pdf_buffer = BytesIO()
            pisa.CreatePDF(src=html_content, dest=pdf_buffer, encoding='utf-8')
            return pdf_buffer.getvalue()
        except Exception:
            pass
    except Exception:
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, title="Missing Punch Notice")
        styles = getSampleStyleSheet()
        elements = [Paragraph("Missing Punch Notice", styles['Title'])]
        elements.append(Spacer(1, 0.2 * inch))
        import re
        text = re.sub(r"<[^>]+>", " ", html_content)
        elements.append(Paragraph(text, styles['BodyText']))
        doc.build(elements)
        return buffer.getvalue()


def build_missing_punch_pdf(templateDIR: str, incidents: list, context: dict, mp_type: int) -> dict:
    """
    Render missing punch notice to PDF using the same HTML templates as email body.
    """
    template_file = 'missingpunch.html'
    if mp_type == 2:
        template_file = 'missingpunch2.html'
    elif mp_type == 3:
        template_file = 'missingpunch3.html'

    store_name = context.get('store') or (incidents[0].get("Store", "") if incidents else "")
    brand = 'TERRA' if store_name.lower() == 'terra' else 'BTRUST'
    watermark_file = context.get('watermark_file')
    if not watermark_file:
        fname = 'Terrawatermark.png' if brand == 'TERRA' else 'Btrustwatermark.png'
        # full_path = os.path.abspath(os.path.join(templateDIR, fname)).replace("\\", "/")
        # watermark_file = f"file:///{full_path}"
        full_path = os.path.abspath(os.path.join(templateDIR, fname))
        watermark_file = pathlib.Path(full_path).as_uri()
    context = dict(context)
    context.setdefault('brand', brand)
    context['watermark_file'] = watermark_file
    context.setdefault('templateDIR', templateDIR)
    store_normalized = (store_name or "").strip().lower()
    logo_file = 'logoOTHER.png'
    if store_normalized == 'hq':
        logo_file = 'logoHQ.png'
    elif store_normalized == 'terra':
        logo_file = 'logoTERRA.jpg'
    elif store_normalized == 'montreal':
        logo_file = 'logoMTL.png'
    try:
        logo_path = pathlib.Path(templateDIR, logo_file).resolve().as_uri()
    except Exception:
        logo_path = None
    context['logo_path'] = logo_path

    html_content = _render_missing_punch_html(templateDIR, template_file, incidents, context)
    pdf_bytes = _html_to_pdf_bytes(html_content, base_url=templateDIR)

    name = context.get('name') or (incidents[0].get("BtrustID", "Employee") if incidents else "Employee")
    period_start = context.get('period_start') or ""
    label = _sanitize_filename(f"{name}_{period_start}")
    return {"name": f"Missing_Punch_{label}.pdf", "content": pdf_bytes}

def send_hr_email_mp(config):
    try:
        if 'MissingPunchEmail' in config and 'MissingPunchMonday' in config and 'Template' in config:
            templateDIR = config['Template']['dir']
            # mondays = config['MissingPunchMonday']['monday'].split(',')
            first_month = config['MissingPunchMonday']['firstmonth']
            send_day = config['MissingPunchMonday']['sendday']
            HREmails = config['MissingPunchEmail']['email'].split(',')
            #payrolls = config['MissingPunchMonday']['payroll'].split(',')
            if not HREmails:
                exit
            # 读取 monday之后的所有 missing punch，按店+月份汇总发送，一人一份 PDF 附件
            store_month_batches = defaultdict(list)
            for incidents, context, type in get_mp(first_month, int(send_day)):
                ctx = context or {}
                store_key = ctx.get('store') or 'Unknown'
                period_key = (ctx.get('period_start') or first_month)[:7]
                store_month_batches[(store_key, period_key)].append((incidents, ctx, type))

            for (store_name, period_key), items in store_month_batches.items():
                if not items:
                    continue
                attachments = []
                combined_incidents = []
                max_type = 1
                employee_summary = {}
                period_start = items[0][1].get('period_start')
                period_end = items[0][1].get('period_end')
                for incidents, context, mp_type in items:
                    combined_incidents.extend(incidents)
                    max_type = max(max_type, mp_type)
                    attachments.append(build_missing_punch_pdf(templateDIR, incidents, context, mp_type))
                    period_start = period_start or context.get('period_start')
                    period_end = period_end or context.get('period_end')
                    emp_key = context.get("btrust_id") or (incidents[0].get("BtrustID") if incidents else None)
                    if emp_key:
                        employee_summary[emp_key] = {
                            "BtrustID": emp_key,
                            "RealName": context.get("real_name") or context.get("name"),
                            "WarningLevel": mp_type,
                        }
                if not combined_incidents or not attachments:
                    continue
                store_context = {
                    "period_start": period_start,
                    "period_end": period_end,
                    "name": store_name,
                    "store": store_name,
                    "department": None,
                    "manager_name": None,
                    "manager_email": None,
                    "action_url": None,
                    "employee_notices": list(employee_summary.values()),
                    "max_warning_level": max_type,
                }
                file_stub = _sanitize_filename(f"{period_key}_{store_name}")
                for email in HREmails:
                    send_missing_punch_email(
                        templateDIR,
                        combined_incidents,
                        email,
                        f"Missing_Punch_{file_stub}",
                        store_context,
                        False,
                        4,
                        list(attachments),
                    )
    except Exception as e:
        print(f"error: {e}")
        logging.info(f"send HR MP email error {e}")

def send_hr_random_shift(shifts, config, monday: str):
    if 'HRrole' in config and 'SINperiods' in config:
        HRrole = config['HRrole']['role']
        HREmails = [item[0] for item in get_hr_emails(HRrole) if len(item[0]) > 0]
        if not HREmails:
            exit
        for shift in shifts:
            store = get_store_name(get_store(shift["DepartmentId"]))
            shift["Store"] = store
        shifts.sort(key = lambda x: (x["Store"], x["DepartmentId"], x["BtrustId"]))
        send_email_to_hr(shifts, HREmails, monday, 2)

# Backward-compatible aliases (camelCase)
