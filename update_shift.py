import logging
import random
import datetime

from helper import DBContextUpdater, _init, get_config_file
import configparser
from snowflake import Snowflake

# 目标部门 ID 常量
TARGET_DEPARTMENTS = (
    665084813002149888,
    665084858908807168,
    665084905167785984,
    665084945512796160,
    665084992929402880,
    665085036705353728,
    665085120281055232,
    665085166259015680,
    665085207908454400,
    665085254498783232,
    665085293866520576,
    665085328926707712,
    668706409361182720
)

# 员工类型与小时数限制映射 (A 类)
A_TYPE_LIMITS = {
    '1': 30,    # A
    '19': 30,   # APlus
    '2': 15,    # A15
    '22': 15,   # A15Plus
    '3': 24,    # A24
    '23': 24,   # A24Plus
    '125': 25,  # A25
    '4': 30,    # A30
    '24': 30,   # A30Plus
    '30': 30,   # A30PlusQuestion
    '135': 35,  # A35
    '5': 36,    # A36
    '25': 36,   # A36Plus
    '6': 40,    # A40
    '26': 40,   # A40Plus
    '7': 44,    # A44
    '27': 44,   # A44Plus
    '31': 20    # A20Plus
}

# 需要删除人员及排班的 C 类员工类型
C_TYPES_TO_DELETE = (10)  # C, 

# 需要仅删除排班信息的类型 (C+, C+?, M 类)
TYPES_TO_CLEAR_SHIFTS = (11, 12, 14, 33) # C+, C+?, M, MStar

def update_punches(cursor, btrustid, target_date, begin_time, end_time):
    """
    更新指定日期的打卡记录：删除旧记录，按排班时间生成带随机偏移的新记录。
    """
    year_str = target_date.strftime("%Y")
    month_padded = target_date.strftime("%m")
    month_raw = str(target_date.month)
    day_padded = target_date.strftime("%d")
    day_raw = str(target_date.day)

    # 1. 删除当天该员工的所有打卡记录 (兼容 '05' 和 '5' 这种格式)
    cursor.execute(
        "DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
        (btrustid, year_str, month_padded, month_raw, day_padded, day_raw)
    )

    # 2. 解析排班时间并计算随机偏移
    try:
        bh, bm = map(int, begin_time.split(':'))
        eh, em = map(int, end_time.split(':'))
        
        # 上班：随机提前 0-5 分钟
        b_dt = target_date.replace(hour=bh, minute=bm) - datetime.timedelta(minutes=random.randint(0, 5))
        # 下班：随机推迟 0-5 分钟
        e_dt = target_date.replace(hour=eh, minute=em) + datetime.timedelta(minutes=random.randint(0, 5))

        for dt in [b_dt, e_dt]:
            h = str(dt.hour).zfill(2)
            m = str(dt.minute).zfill(2)
            y = str(dt.year)
            mo = str(dt.month).zfill(2)
            d = str(dt.day).zfill(2)
            p_date = dt.strftime("%Y-%m-%d")
            
            sql = """
                INSERT INTO SysPunch(HandRec, SiteNumber, ClockNumber, ClockCode, BtrustId, 
                                     WorkCode, Hour, Minute, Month, Day, Year, PunchDate) 
                VALUES ('0', '3', '0', '1', ?, '0', ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(sql, (btrustid, h, m, mo, d, y, p_date))
    except Exception as e:
        print(f"Error generating punches for {btrustid} on {year_str}-{month_raw}-{day_raw}: {e}")
        logging.error(f"Failed to generate punches for {btrustid} on {year_str}-{month_raw}-{day_raw}: {e}")

def adjust_shift_hours(detail_id, current_total, limit, daily_data):
    """
    调整排班小时数，确保总数等于 limit，且单天工作不少于 3 小时，不多于 12 小时。
    daily_data: [(total_h_idx, end_time_idx, begin_time_val, end_time_val, total_h_val), ...]
    """
    MAX_DAILY_HOURS = 12.0
    MIN_DAILY_HOURS = 3.0

    # 转换为可操作的字典列表
    days = []
    for d in daily_data:
        days.append({
            'total_idx': d[0], 'end_idx': d[1],
            'begin_str': str(d[2]).strip() if d[2] else "",
            'val': float(d[4]), 'modified': False
        })

    # 1. 强制将所有超过 12 小时的天数限制在 12 小时
    for d in days:
        if d['val'] > MAX_DAILY_HOURS:
            d['val'] = MAX_DAILY_HOURS
            d['modified'] = True

    # 2. 计算当前总和与目标的差距
    current_sum = sum(d['val'] for d in days)
    diff = current_sum - limit  # 正数: 需减少; 负数: 需增加
    new_daily_values = {}

    if round(diff, 2) > 0:
        # 减少工时：从最长的日子开始扣
        for d in sorted(days, key=lambda x: x['val'], reverse=True):
            if diff <= 0:
                break
            can_reduce = d['val'] - MIN_DAILY_HOURS
            if can_reduce > 0:
                reduce_amount = min(diff, can_reduce)
                d['val'] -= reduce_amount
                diff -= reduce_amount
                d['modified'] = True
    elif round(diff, 2) < 0:
        # 增加工时：在不超过 12h 的前提下，平摊到现有工作日
        to_increase = abs(diff)
        for d in sorted(days, key=lambda x: x['val'], reverse=True):
            if to_increase <= 0:
                break
            can_add = MAX_DAILY_HOURS - d['val']
            if can_add > 0:
                addition = min(to_increase, can_add)
                d['val'] += addition
                to_increase -= addition
                d['modified'] = True

    if not any(d['modified'] for d in days) and round(current_total, 2) == round(limit, 2):
        return None

    # 3. 重新计算结束时间并构造返回字典
    for d in days:
        if d['modified']:
            try:
                h_start, m_start = map(int, d['begin_str'].split(':'))
                duration = d['val'] + (0.5 if d['val'] >= 5 else 0)
                end_total_minutes = (h_start * 60 + m_start) + int(duration * 60)
                new_end_time = f"{(end_total_minutes // 60) % 24:02d}:{end_total_minutes % 60:02d}"
                new_daily_values[d['total_idx']] = str(round(d['val'], 2))
                new_daily_values[d['end_idx']] = new_end_time
            except:
                continue

    final_total = sum(d['val'] for d in days)
    return new_daily_values, str(round(final_total, 2))

def cleanup_specific_holidays(cursor):
    """
    清理特定人员在特定节假日的打卡和排班数据
    """
    target_ids = ('20125', '10822', '40100', '20270', '20322') 
    holidays = [
        {"date": "2025-12-25", "monday": "2025-12-22", "prefix": "thursday"},
        {"date": "2025-12-26", "monday": "2025-12-22", "prefix": "friday"},
        {"date": "2026-01-01", "monday": "2025-12-29", "prefix": "thursday"},
        {"date": "2026-02-16", "monday": "2026-02-16", "prefix": "monday"},
    ]
    
    for btrustid in target_ids:
        cursor.execute("SELECT id FROM sysuser WHERE btrustid = ?", (btrustid,))
        user_row = cursor.fetchone()
        if not user_row:
            continue
        userid = user_row[0]
        
        for holiday in holidays:
            h_date = datetime.datetime.strptime(holiday["date"], "%Y-%m-%d")
            y, m, d = h_date.strftime("%Y"), h_date.strftime("%m"), h_date.strftime("%d")
            m_raw, d_raw = str(h_date.month), str(h_date.day)

            # 1. 删除打卡记录
            cursor.execute(
                "DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
                (btrustid, y, m, m_raw, d, d_raw)
            )

            # 2. 修改排班明细
            prefix = holiday["prefix"]
            monday = holiday["monday"]
            sql_select = f"""
                SELECT sd.id, sd.{prefix}totalhours, sd.totalhours 
                FROM sysshiftdetail sd
                INNER JOIN sysshift s ON sd.shiftid = s.id
                WHERE sd.userid = ? AND s.periodBegin = ?
            """
            cursor.execute(sql_select, (userid, monday))
            detail = cursor.fetchone()
            
            if detail:
                detail_id, daily_h_str, total_h_str = detail
                daily_h = float(str(daily_h_str).strip()) if daily_h_str else 0.0
                total_h = float(str(total_h_str).strip()) if total_h_str else 0.0
                
                new_total = max(0, total_h - daily_h)
                sql_update = f"UPDATE sysshiftdetail SET {prefix}begin='00:00', {prefix}end='00:00', {prefix}totalhours='0', totalhours=? WHERE id=?"
                cursor.execute(sql_update, (str(round(new_total, 2)), detail_id))

        # 3. 每周排班时间不能超过44小时，超过的话调整为每周随机6天，每天7小时（排班7.5小时）
        cursor.execute("""
            SELECT sd.id, sd.totalhours, s.periodBegin 
            FROM sysshiftdetail sd
            INNER JOIN sysshift s ON sd.shiftid = s.id
            WHERE sd.userid = ?
        """, (userid,))
        shift_rows = cursor.fetchall()
        
        day_prefixes = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        
        for s_row in shift_rows:
            detail_id, total_h_str, period_begin = s_row
            p_begin_str = str(period_begin).strip()
            try:
                total_h = float(str(total_h_str).strip()) if total_h_str else 0.0
            except ValueError:
                total_h = 0.0
            
            if total_h > 44 and period_begin:
                # 确定该周包含的节假日索引，并从可选工作日中剔除
                holiday_indices = [day_prefixes.index(h["prefix"]) for h in holidays if h["monday"] == p_begin_str]
                available_indices = [i for i in range(7) if i not in holiday_indices]
                work_day_indices = random.sample(available_indices, min(6, len(available_indices)))
                
                monday_date = datetime.datetime.strptime(p_begin_str, "%Y-%m-%d")
                
                new_weekly_total = 0.0
                updates = []
                
                for i in range(7):
                    prefix = day_prefixes[i]
                    target_date = monday_date + datetime.timedelta(days=i)
                    y, m, d = target_date.strftime("%Y"), target_date.strftime("%m"), target_date.strftime("%d")
                    m_raw, d_raw = str(target_date.month), str(target_date.day)

                    if i in work_day_indices:
                        # 随机起始时间：9, 10, 11, 12 点
                        start_h = random.choice([9, 10, 11, 12])
                        start_time = f"{str(start_h).zfill(2)}:00"
                        
                        # 排班 7.5 小时，扣除 0.5 小时休息，实际 7 小时
                        # 结束时间计算：例如 09:00 + 7.5h = 16:30
                        end_h = start_h + 7
                        end_m = 30
                        end_time = f"{str(end_h).zfill(2)}:{str(end_m).zfill(2)}"
                        
                        updates.append(f"{prefix}begin='{start_time}'")
                        updates.append(f"{prefix}end='{end_time}'")
                        updates.append(f"{prefix}totalhours='7'")
                        new_weekly_total += 7.0
                        
                        # 同步打卡数据 (update_punches 内部会处理删除和带偏移的插入)
                        update_punches(cursor, btrustid, target_date, start_time, end_time)
                    else:
                        # 第 7 天休息
                        updates.append(f"{prefix}begin='00:00'")
                        updates.append(f"{prefix}end='00:00'")
                        updates.append(f"{prefix}totalhours='0'")
                        
                        # 删除该休息日的打卡记录
                        cursor.execute(
                            "DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
                            (btrustid, y, m, m_raw, d, d_raw)
                        )
                
                updates.append(f"totalhours='{str(round(new_weekly_total, 2))}'")
                cursor.execute(f"UPDATE sysshiftdetail SET {', '.join(updates)} WHERE id=?", (detail_id,))
        logging.info(f"Finished holiday cleanup and 44h limit check for btrustid: {btrustid}")

def adjust_user_20194(cursor):
    """
    针对 20194 员工在 2026-01-19 之前的排班和打卡调整
    """
    btrustid = '20194'
    cursor.execute("SELECT id FROM sysuser WHERE btrustid = ?", (btrustid,))
    user_row = cursor.fetchone()
    if not user_row:
        return
    userid = user_row[0]

    # 1. 节假日清理 (仅限 2026-01-19 之前的)
    holidays = [
        {"date": "2025-12-25", "monday": "2025-12-22", "prefix": "thursday"},
        {"date": "2025-12-26", "monday": "2025-12-22", "prefix": "friday"},
        {"date": "2026-01-01", "monday": "2025-12-29", "prefix": "thursday"},
    ]

    for holiday in holidays:
        h_date = datetime.datetime.strptime(holiday["date"], "%Y-%m-%d")
        y, m, d = h_date.strftime("%Y"), h_date.strftime("%m"), h_date.strftime("%d")
        m_raw, d_raw = str(h_date.month), str(h_date.day)

        # 删除打卡记录
        cursor.execute(
            "DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
            (btrustid, y, m, m_raw, d, d_raw)
        )

        # 修改排班明细
        prefix = holiday["prefix"]
        monday = holiday["monday"]
        sql_select = f"""
            SELECT sd.id, sd.{prefix}totalhours, sd.totalhours 
            FROM sysshiftdetail sd
            INNER JOIN sysshift s ON sd.shiftid = s.id
            WHERE sd.userid = ? AND s.periodBegin = ?
        """
        cursor.execute(sql_select, (userid, monday))
        detail = cursor.fetchone()
        if detail:
            detail_id, daily_h_str, total_h_str = detail
            daily_h = float(str(daily_h_str).strip() or 0)
            total_h = float(str(total_h_str).strip() or 0)
            new_total = max(0, total_h - daily_h)
            sql_update = f"UPDATE sysshiftdetail SET {prefix}begin='00:00', {prefix}end='00:00', {prefix}totalhours='0', totalhours=? WHERE id=?"
            cursor.execute(sql_update, (str(round(new_total, 2)), detail_id))

    # 2. 44小时限制调整 (仅限 2026-01-19 之前的周)
    cursor.execute("""
        SELECT sd.id, sd.totalhours, s.periodBegin 
        FROM sysshiftdetail sd
        INNER JOIN sysshift s ON sd.shiftid = s.id
        WHERE sd.userid = ? AND s.periodBegin < '2026-01-19'
    """, (userid,))
    shift_rows = cursor.fetchall()

    day_prefixes = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    for s_row in shift_rows:
        detail_id, total_h_str, period_begin = s_row
        p_begin_str = str(period_begin).strip()
        total_h = float(str(total_h_str).strip() or 0)
        if total_h > 44 and period_begin:
            # 确定该周包含的节假日索引，并从可选工作日中剔除
            holiday_indices = [day_prefixes.index(h["prefix"]) for h in holidays if h["monday"] == p_begin_str]
            available_indices = [i for i in range(7) if i not in holiday_indices]
            work_indices = random.sample(available_indices, min(6, len(available_indices)))

            monday_date = datetime.datetime.strptime(p_begin_str, "%Y-%m-%d")
            new_weekly_total = 0.0
            updates = []
            for i in range(7):
                prefix = day_prefixes[i]
                target_date = monday_date + datetime.timedelta(days=i)
                if i in work_indices:
                    sh = random.choice([9, 10, 11, 12])
                    s_time, e_time = f"{sh:02d}:00", f"{sh+7:02d}:30"
                    updates.extend([f"{prefix}begin='{s_time}'", f"{prefix}end='{e_time}'", f"{prefix}totalhours='7'"])
                    new_weekly_total += 7.0
                    update_punches(cursor, btrustid, target_date, s_time, e_time)
                else:
                    updates.extend([f"{prefix}begin='00:00'", f"{prefix}end='00:00'", f"{prefix}totalhours='0'"])
                    y, m, d = target_date.strftime("%Y"), target_date.strftime("%m"), target_date.strftime("%d")
                    cursor.execute("DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
                                   (btrustid, y, m, str(target_date.month), d, str(target_date.day)))
            updates.append(f"totalhours='{str(round(new_weekly_total, 2))}'")
            cursor.execute(f"UPDATE sysshiftdetail SET {', '.join(updates)} WHERE id=?", (detail_id,))
    logging.info("Finished adjustments for btrustid 20194 before 2026-01-19")

def adjust_users_20300_20278_20252(cursor):
    """
    针对 20300, 20278, 20252 员工在 2025-12-15 到 2026-01-11 之间的排班和打卡调整
    """
    target_ids = ('20300', '20278', '20252')
    holidays = [
        {"date": "2025-12-25", "monday": "2025-12-22", "prefix": "thursday"},
        {"date": "2025-12-26", "monday": "2025-12-22", "prefix": "friday"},
        {"date": "2026-01-01", "monday": "2025-12-29", "prefix": "thursday"},
    ]

    for btrustid in target_ids:
        cursor.execute("SELECT id FROM sysuser WHERE btrustid = ?", (btrustid,))
        user_row = cursor.fetchone()
        if not user_row:
            continue
        userid = user_row[0]

        # 1. 节假日清理
        for holiday in holidays:
            h_date = datetime.datetime.strptime(holiday["date"], "%Y-%m-%d")
            y, m, d = h_date.strftime("%Y"), h_date.strftime("%m"), h_date.strftime("%d")
            # 清理打卡
            cursor.execute(
                "DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
                (btrustid, y, m, str(h_date.month), d, str(h_date.day))
            )
            # 清理排班
            prefix = holiday["prefix"]
            monday = holiday["monday"]
            cursor.execute(f"""
                SELECT sd.id, sd.{prefix}totalhours, sd.totalhours 
                FROM sysshiftdetail sd
                INNER JOIN sysshift s ON sd.shiftid = s.id
                WHERE sd.userid = ? AND s.periodBegin = ?
            """, (userid, monday))
            detail = cursor.fetchone()
            if detail:
                detail_id, daily_h, total_h = detail[0], float(str(detail[1]).strip() or 0), float(str(detail[2]).strip() or 0)
                cursor.execute(f"UPDATE sysshiftdetail SET {prefix}begin='00:00', {prefix}end='00:00', {prefix}totalhours='0', totalhours=? WHERE id=?",
                               (str(round(max(0, total_h - daily_h), 2)), detail_id))

        # 2. 44小时限制调整 (2025-12-15 至 2026-01-11)
        cursor.execute("""
            SELECT sd.id, sd.totalhours, s.periodBegin 
            FROM sysshiftdetail sd
            INNER JOIN sysshift s ON sd.shiftid = s.id
            WHERE sd.userid = ? AND s.periodBegin >= '2025-12-15' AND s.periodBegin < '2026-01-11'
        """, (userid,))
        shift_rows = cursor.fetchall()

        day_prefixes = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        for s_row in shift_rows:
            detail_id, total_h_str, period_begin = s_row
            p_begin_str = str(period_begin).strip()
            total_h = float(str(total_h_str).strip() or 0)
            if total_h > 44:
                # 确定该周包含的节假日索引，并从可选工作日中剔除
                holiday_indices = [day_prefixes.index(h["prefix"]) for h in holidays if h["monday"] == p_begin_str]
                available_indices = [i for i in range(7) if i not in holiday_indices]
                work_indices = random.sample(available_indices, min(6, len(available_indices)))

                monday_date = datetime.datetime.strptime(p_begin_str, "%Y-%m-%d")
                updates = []
                new_weekly_total = 0.0
                for i in range(7):
                    prefix, target_date = day_prefixes[i], monday_date + datetime.timedelta(days=i)
                    if i in work_indices:
                        sh = random.choice([9, 10, 11, 12])
                        s_t, e_t = f"{sh:02d}:00", f"{sh+7:02d}:30"
                        updates.extend([f"{prefix}begin='{s_t}'", f"{prefix}end='{e_t}'", f"{prefix}totalhours='7'"])
                        new_weekly_total += 7.0
                        update_punches(cursor, btrustid, target_date, s_t, e_t)
                    else:
                        updates.extend([f"{prefix}begin='00:00'", f"{prefix}end='00:00'", f"{prefix}totalhours='0'"])
                        cursor.execute("DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
                                       (btrustid, target_date.strftime("%Y"), target_date.strftime("%m"), str(target_date.month), target_date.strftime("%d"), str(target_date.day)))
                updates.append(f"totalhours='{str(round(new_weekly_total, 2))}'")
                cursor.execute(f"UPDATE sysshiftdetail SET {', '.join(updates)} WHERE id=?", (detail_id,))
    logging.info("Finished adjustments for 20300, 20278, 20252 between 2025-12-15 and 2026-01-11")

def adjust_user_20277(cursor):
    """
    针对 20277 员工的特殊排班和打卡调整
    """
    btrustid = '20277'
    cursor.execute("SELECT id, departmentid FROM sysuser WHERE btrustid = ?", (btrustid,))
    user_row = cursor.fetchone()
    if not user_row:
        return
    userid, deptid = user_row
    day_prefixes = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    # 1. 2025-12-01 这周：同步打卡记录
    cursor.execute("""
        SELECT sd.* FROM sysshiftdetail sd 
        INNER JOIN sysshift s ON sd.shiftid = s.id 
        WHERE sd.userid = ? AND s.periodBegin = '2025-12-01'
    """, (userid,))
    shift_01 = cursor.fetchone()
    if shift_01:
        col_names = [column[0].lower() for column in cursor.description]
        monday_dt = datetime.datetime(2025, 12, 1)
        for i in range(7):
            prefix = day_prefixes[i]
            begin_val = shift_01[col_names.index(f"{prefix}begin")]
            end_val = shift_01[col_names.index(f"{prefix}end")]
            t_val = float(str(shift_01[col_names.index(f"{prefix}totalhours")]).strip() or 0)
            if t_val > 0:
                update_punches(cursor, btrustid, monday_dt + datetime.timedelta(days=i), str(begin_val).strip(), str(end_val).strip())

    # 2. 2025-12-08 和 2025-12-15 周：调整为 42 小时
    for week_start in ['2025-12-08', '2025-12-15']:
        cursor.execute("""
            SELECT sd.id FROM sysshiftdetail sd 
            INNER JOIN sysshift s ON sd.shiftid = s.id 
            WHERE sd.userid = ? AND s.periodBegin = ?
        """, (userid, week_start))
        res = cursor.fetchone()
        if res:
            detail_id = res[0]
            work_days = random.sample(range(7), 6)
            monday_dt = datetime.datetime.strptime(week_start, "%Y-%m-%d")
            updates = []
            for i in range(7):
                prefix = day_prefixes[i]
                target_date = monday_dt + datetime.timedelta(days=i)
                if i in work_days:
                    sh = random.choice([9, 10, 11, 12])
                    s_time, e_time = f"{sh:02d}:00", f"{sh+7:02d}:30"
                    updates.extend([f"{prefix}begin='{s_time}'", f"{prefix}end='{e_time}'", f"{prefix}totalhours='7'"])
                    update_punches(cursor, btrustid, target_date, s_time, e_time)
                else:
                    updates.extend([f"{prefix}begin='00:00'", f"{prefix}end='00:00'", f"{prefix}totalhours='0'"])
                    y, m, d = target_date.strftime("%Y"), target_date.strftime("%m"), target_date.strftime("%d")
                    cursor.execute("DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)", 
                                   (btrustid, y, m, str(target_date.month), d, str(target_date.day)))
            updates.append("totalhours='42'")
            cursor.execute(f"UPDATE sysshiftdetail SET {', '.join(updates)} WHERE id=?", (detail_id,))

    # 3. 2025-12-22 周：删除周三周四，调整周日下班时间，并确保总计 36 小时
    cursor.execute("""
        SELECT sd.id, sd.sundaybegin, sd.sundaytotalhours, sd.totalhours
        FROM sysshiftdetail sd INNER JOIN sysshift s ON sd.shiftid = s.id 
        WHERE sd.userid = ? AND s.periodBegin = '2025-12-22'
    """, (userid,))
    row_22 = cursor.fetchone()
    if row_22:
        did_22, sun_begin, sun_old_h, old_total = row_22
        sun_begin = str(sun_begin).strip()

        # 调整周日 (12-28) 下班时间为 19:00
        new_sun_h = float(str(sun_old_h).strip() or 0)
        if sun_begin and sun_begin != "00:00":
            try:
                bh, bm = map(int, sun_begin.split(':'))
                diff = (19 * 60) - (bh * 60 + bm)
                new_sun_h = round(diff / 60.0 - (0.5 if diff >= 300 else 0), 2)
                update_punches(cursor, btrustid, datetime.datetime(2025, 12, 28), sun_begin, "19:00")
            except: pass

        # 清除本周三周四，调整周日，并强制周总小时数为 36
        cursor.execute("""
            UPDATE sysshiftdetail SET wednesdaybegin='00:00', wednesdayend='00:00', wednesdaytotalhours='0',
            thursdaybegin='00:00', thursdayend='00:00', thursdaytotalhours='0',
            sundayend='19:00', sundaytotalhours=?, totalhours='36' WHERE id=?
        """, (str(new_sun_h), did_22))
        
        for d_off in [2, 3]: # 周三周四
            dt = datetime.datetime(2025, 12, 22) + datetime.timedelta(days=d_off)
            cursor.execute("DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
                           (btrustid, dt.strftime("%Y"), dt.strftime("%m"), str(dt.month), dt.strftime("%d"), str(dt.day)))

    # 4. 2025-12-29 周：周一周二排班 9 小时，清除周三到周日，并确保总计 18 小时
    cursor.execute("""
        SELECT sd.id FROM sysshiftdetail sd 
        INNER JOIN sysshift s ON sd.shiftid = s.id 
        WHERE sd.userid = ? AND s.periodBegin = '2025-12-29'
    """, (userid,))
    row_29 = cursor.fetchone()
    
    # 设定 9 小时排班 (09:00 - 18:30)
    m_data, t_data = ('09:00', '18:30', '9'), ('09:00', '18:30', '9')

    if row_29:
        did_29 = row_29[0]
        cursor.execute("""
            UPDATE sysshiftdetail SET mondaybegin=?, mondayend=?, mondaytotalhours=?,
            tuesdaybegin=?, tuesdayend=?, tuesdaytotalhours=?, 
            wednesdaybegin='00:00', wednesdayend='00:00', wednesdaytotalhours='0',
            thursdaybegin='00:00', thursdayend='00:00', thursdaytotalhours='0',
            fridaybegin='00:00', fridayend='00:00', fridaytotalhours='0',
            saturdaybegin='00:00', saturdayend='00:00', saturdaytotalhours='0',
            sundaybegin='00:00', sundayend='00:00', sundaytotalhours='0',
            totalhours='18' WHERE id=?
        """, (m_data[0], m_data[1], m_data[2], t_data[0], t_data[1], t_data[2], did_29))
        
        # 删除 12-29 那周周三到周日的打卡记录
        for d_off in range(2, 7):
            dt = datetime.datetime(2025, 12, 29) + datetime.timedelta(days=d_off)
            cursor.execute("DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
                           (btrustid, dt.strftime("%Y"), dt.strftime("%m"), str(dt.month), dt.strftime("%d"), str(dt.day)))
    else:
        cursor.execute("SELECT id FROM sysshift WHERE periodBegin = '2025-12-29' AND departmentId = ?", (deptid,))
        sid_row = cursor.fetchone()
        if sid_row:
            sf = Snowflake(1, 1)
            new_id = sf.next_id()
            cursor.execute("""
                INSERT INTO sysshiftdetail (id, shiftid, userid, mondaybegin, mondayend, mondaytotalhours, 
                tuesdaybegin, tuesdayend, tuesdaytotalhours, totalhours)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '18')
            """, (new_id, sid_row[0], userid, m_data[0], m_data[1], m_data[2], t_data[0], t_data[1], t_data[2]))
    
    # 更新打卡记录
    update_punches(cursor, btrustid, datetime.datetime(2025, 12, 29), m_data[0], m_data[1])
    update_punches(cursor, btrustid, datetime.datetime(2025, 12, 30), t_data[0], t_data[1])

    logging.info("Finished special adjustments for btrustid: 20277")

def cleanup_punch_problems(cursor):
    """
    清理 syspunchproblem 表：
    1. 除了 B 开头和 S 开头类型的人员，其余人员在目标部门的记录全部删除。
    2. 针对特定人员在曾为 D 类期间的记录进行清理。
    """
    # B 类 (8, 9, 29, 34, 35, 36) 和 S 类 (0, 16, 17)
    bs_types = (0, 8, 9, 16, 17, 29, 34, 35, 36)
    
    # 1. 通用清理：非 B/S 类型的员工
    sql_general = f"""
        DELETE FROM syspunchproblem 
        WHERE userid IN (
            SELECT id FROM sysuser 
            WHERE departmentid IN {TARGET_DEPARTMENTS} 
            AND type NOT IN {bs_types}
        )
    """
    cursor.execute(sql_general)
    
    # 2. 特殊人员处理（曾为 D 类的时间段）
    special_cases = [
        ('20277', '2025-12-01', '2026-01-04'),
        ('20194', '2025-11-24', '2026-01-18'),
        ('20300', '2025-12-15', '2026-01-11'),
        ('20278', '2025-12-15', '2026-01-11'),
        ('20252', '2025-12-15', '2026-01-11'),
    ]
    
    for btrustid, start, end in special_cases:
        cursor.execute(
            "DELETE FROM syspunchproblem WHERE btrustid = ? AND punchdate >= ? AND punchdate <= ?",
            (btrustid, start, end)
        )
    logging.info("Finished cleaning up syspunchproblem records.")

def adjust_specific_a_users_holidays(cursor):
    """
    针对特定 A 类员工在特定节假日的排班和打卡进行精确调整。
    """
    # 定义人员及其对应日期的目标小时数 (h: 圣诞/元旦期间, f: 家庭日)
    overrides = {
        '20128': {'h': 6.0, 'f': 4.0},
        '20308': {'h': 6.0, 'f': 4.0},
        '10515': {'h': 6.0, 'f': 0.0},
        '10537': {'h': 6.0, 'f': 6.0},
        '10529': {'h': 6.0, 'f': 4.0},
        '11069': {'h': 6.0, 'f': 7.0},
        '10202': {'h': 6.0, 'f': 6.0},
        '20066': {'h': 6.0, 'f': 6.0},
        '10508': {'h': 0.0, 'f': 0.0},
    }
    
    # 目标日期配置
    h_period_dates = [
        ("2025-12-25", "2025-12-22", "thursday"),
        ("2025-12-26", "2025-12-22", "friday"),
        ("2026-01-01", "2025-12-29", "thursday")
    ]
    f_date_cfg = ("2026-02-16", "2026-02-16", "monday")

    for btrustid, hours_cfg in overrides.items():
        cursor.execute("SELECT id FROM sysuser WHERE btrustid = ?", (btrustid,))
        user_row = cursor.fetchone()
        if not user_row:
            continue
        userid = user_row[0]

        # --- 1. 处理圣诞/元旦三天的累计排班 (h) ---
        target_h_h = hours_cfg['h']
        if target_h_h > 0:
            # 随机选择其中一天排班，其余两天为0
            lucky_day_idx = random.randint(0, 2)
            for idx, (d_str, m_str, prefix) in enumerate(h_period_dates):
                daily_h = target_h_h if idx == lucky_day_idx else 0.0
                apply_day_adjustment(cursor, userid, btrustid, d_str, m_str, prefix, daily_h)
        else:
            # 全都清零
            for d_str, m_str, prefix in h_period_dates:
                apply_day_adjustment(cursor, userid, btrustid, d_str, m_str, prefix, 0.0)

        # --- 2. 处理 Family Day 单日排班 (f) ---
        apply_day_adjustment(cursor, userid, btrustid, f_date_cfg[0], f_date_cfg[1], f_date_cfg[2], hours_cfg['f'])

    logging.info("Finished specific A-class user adjustments for holidays.")

def apply_day_adjustment(cursor, userid, btrustid, date_str, monday, prefix, target_h):
    """
    辅助函数：应用单日的排班和打卡调整逻辑
    """
    t_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    sql_select = f"SELECT sd.id, sd.{prefix}totalhours, sd.totalhours FROM sysshiftdetail sd INNER JOIN sysshift s ON sd.shiftid = s.id WHERE sd.userid = ? AND s.periodBegin = ?"
    cursor.execute(sql_select, (userid, monday))
    detail = cursor.fetchone()
    
    if target_h == 0:
        if detail:
            detail_id, old_daily_h_str, old_total_h_str = detail
            old_daily_h = float(str(old_daily_h_str).strip() or 0)
            old_total_h = float(str(old_total_h_str).strip() or 0)
            new_total = max(0, old_total_h - old_daily_h)
            sql_update = f"UPDATE sysshiftdetail SET {prefix}begin='00:00', {prefix}end='00:00', {prefix}totalhours='0', totalhours=? WHERE id=?"
            cursor.execute(sql_update, (str(round(new_total, 2)), detail_id))
        
        cursor.execute("DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
                       (btrustid, t_date.strftime("%Y"), t_date.strftime("%m"), str(t_date.month), t_date.strftime("%d"), str(t_date.day)))
        return

    if detail and target_h > 0:
        detail_id, old_daily_h_str, old_total_h_str = detail
        old_daily_h = float(str(old_daily_h_str).strip() or 0)
        old_total_h = float(str(old_total_h_str).strip() or 0)

        start_h = random.choice([9, 10, 11, 12])
        start_time = f"{start_h:02d}:00"
        # 计算结束时间：如果是5小时以上，通常加0.5小时休息时间
        duration = target_h + (0.5 if target_h >= 5 else 0)
        end_total_minutes = start_h * 60 + int(duration * 60)
        end_time = f"{(end_total_minutes // 60) % 24:02d}:{end_total_minutes % 60:02d}"
        
        new_total = old_total_h - old_daily_h + target_h
        sql_update = f"UPDATE sysshiftdetail SET {prefix}begin=?, {prefix}end=?, {prefix}totalhours=?, totalhours=? WHERE id=?"
        cursor.execute(sql_update, (start_time, end_time, str(target_h), str(round(new_total, 2)), detail_id))
        
        update_punches(cursor, btrustid, t_date, start_time, end_time)

def apply_manual_adjustments(cursor):
    """
    根据用户需求手动调整特定人员的打卡记录和排班。
    """
    day_prefixes = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    # 1. 删除打卡数据及对应的排班
    delete_tasks = [
        ('11069', '2025-12-17'),
        ('11069', '2025-12-23'),
        ('20026', '2026-01-04'),
        ('20128', '2025-12-22'),
        ('20308', '2025-12-23'),
        ('20066', '2025-12-24'),
        ('40100', '2025-12-24'),
    ]
    
    for btrustid, date_str in delete_tasks:
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        monday_dt = dt - datetime.timedelta(days=dt.weekday())
        monday_str = monday_dt.strftime("%Y-%m-%d")
        prefix = day_prefixes[dt.weekday()]
        
        cursor.execute("SELECT id FROM sysuser WHERE btrustid = ?", (btrustid,))
        user_row = cursor.fetchone()
        if user_row:
            apply_day_adjustment(cursor, user_row[0], btrustid, date_str, monday_str, prefix, 0.0)
            logging.info(f"Removed punches and set shift to 0 for {btrustid} on {date_str}")

    # 2. 修改打卡时间 (调整下班时间)
    modify_tasks = [
        ('20327', '2026-01-01', '17:00'),
        ('20327', '2025-12-31', '13:00'),
        ('20327', '2026-01-04', '17:00'),
        ('10508', '2025-12-24', '15:30'),
    ]
    
    for btrustid, date_str, new_end in modify_tasks:
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        monday_dt = dt - datetime.timedelta(days=dt.weekday())
        monday_str = monday_dt.strftime("%Y-%m-%d")
        prefix = day_prefixes[dt.weekday()]

        cursor.execute("SELECT id FROM sysuser WHERE btrustid = ?", (btrustid,))
        user_row = cursor.fetchone()
        if not user_row:
            continue
        userid = user_row[0]

        # 获取当天的排班明细，确保存在上班时间
        sql = f"""
            SELECT sd.id, sd.{prefix}begin, sd.{prefix}totalhours, sd.totalhours 
            FROM sysshiftdetail sd 
            INNER JOIN sysshift s ON sd.shiftid = s.id 
            WHERE sd.userid = ? AND s.periodBegin = ?
        """
        cursor.execute(sql, (userid, monday_str))
        detail = cursor.fetchone()
        
        punch_updated = False
        if detail:
            did, begin_time, old_d_str, old_t_str = detail
            begin_time = str(begin_time).strip()
            
            if begin_time and begin_time != "00:00":
                try:
                    bh, bm = map(int, begin_time.split(':'))
                    eh, em = map(int, new_end.split(':'))
                    diff_minutes = (eh * 60 + em) - (bh * 60 + bm)
                    
                    # 模拟系统扣除休息时间逻辑 (5小时以上扣0.5h)
                    target_h = round(diff_minutes / 60.0 - (0.5 if diff_minutes >= 300 else 0), 2)
                    
                    old_d = float(str(old_d_str).strip() or 0)
                    old_t = float(str(old_t_str).strip() or 0)
                    new_t = round(old_t - old_d + target_h, 2)
                    
                    # 更新排班
                    sql_upd = f"UPDATE sysshiftdetail SET {prefix}end=?, {prefix}totalhours=?, totalhours=? WHERE id=?"
                    cursor.execute(sql_upd, (new_end, str(target_h), str(new_t), did))
                    
                    # 同步更新打卡数据 (带随机偏移)
                    update_punches(cursor, btrustid, dt, begin_time, new_end)
                    punch_updated = True
                    logging.info(f"Modified punch out for {btrustid} on {date_str} to {new_end} (Hours: {target_h})")
                except Exception as e:
                    logging.error(f"Manual adjustment error for {btrustid} on {date_str}: {e}")

        # 如果没有排班或者排班没上班时间，但确实需要修改打卡时间 (针对 20327 等情况)
        if not punch_updated:
            cursor.execute("SELECT MIN(CAST(hour AS INT) * 60 + CAST(minute AS INT)) FROM syspunch WHERE btrustid=? AND punchdate=?", (btrustid, date_str))
            min_min = cursor.fetchone()[0]
            if min_min is not None:
                actual_begin = f"{min_min // 60:02d}:{min_min % 60:02d}"
                update_punches(cursor, btrustid, dt, actual_begin, new_end)
                logging.info(f"Forced punch modification for {btrustid} on {date_str} to {new_end} (No shift found, using earliest punch {actual_begin})")

def run_update():
    with DBContextUpdater() as conn:
        with conn.cursor() as cursor:
            logging.info("Starting shift update script...")
            
            # 1.1 处理特定员工 10544：彻底删除所有数据
            cursor.execute("SELECT id FROM sysuser WHERE btrustid = '10544'")
            u10544 = cursor.fetchone()
            if u10544:
                uid = u10544[0]
                cursor.execute(f"DELETE FROM sysshiftdetail WHERE userid = {uid}")
                cursor.execute("DELETE FROM syspunch WHERE btrustid = '10544'")
                cursor.execute("DELETE FROM syspunchproblem WHERE btrustid = '10544'")
                cursor.execute(f"DELETE FROM sysuser WHERE id = {uid}")
                logging.info("Fully deleted user 10544 and all related records.")

            # 1.2 处理 C 类员工：删除排班明细和人员
            cursor.execute(f"SELECT id, btrustid FROM sysuser WHERE departmentid IN {TARGET_DEPARTMENTS} AND type = {C_TYPES_TO_DELETE}")
            c_users = cursor.fetchall()
            if c_users:
                u_ids = [str(row[0]) for row in c_users]
                b_ids = ["'" + str(row[1]) + "'" for row in c_users if row[1]]
                
                cursor.execute(f"DELETE FROM sysshiftdetail WHERE userid IN ({','.join(u_ids)})")
                if b_ids:
                    cursor.execute(f"DELETE FROM syspunch WHERE btrustid IN ({','.join(b_ids)})")
                cursor.execute(f"DELETE FROM sysuser WHERE id IN ({','.join(u_ids)})")
                logging.info(f"Deleted {len(c_users)} C-class users, their shifts and punches.")

            # 2. 处理 M 类（及 C+）员工：仅删除排班明细
            cursor.execute(f"SELECT id, btrustid FROM sysuser WHERE departmentid IN {TARGET_DEPARTMENTS} AND type IN {TYPES_TO_CLEAR_SHIFTS}")
            dm_users = cursor.fetchall()
            if dm_users:
                u_ids = [str(row[0]) for row in dm_users]
                b_ids = ["'" + str(row[1]) + "'" for row in dm_users if row[1]]

                cursor.execute(f"DELETE FROM sysshiftdetail WHERE userid IN ({','.join(u_ids)})")
                if b_ids:
                    cursor.execute(f"DELETE FROM syspunch WHERE btrustid IN ({','.join(b_ids)})")
                logging.info(f"Cleared shifts and punches for {len(dm_users)} D/M class users.")

            # 3. 处理 A 类员工：调整超标排班
            a_types = tuple(A_TYPE_LIMITS.keys())
            cursor.execute(f"""
                SELECT sd.id, 
                       sd.totalhours, 
                       u.type, 
                       u.btrustid, 
                       s.periodBegin,
                       sd.mondaybegin, sd.mondayend, sd.mondaytotalhours,
                       sd.tuesdaybegin, sd.tuesdayend, sd.tuesdaytotalhours,
                       sd.wednesdaybegin, sd.wednesdayend, sd.wednesdaytotalhours,
                       sd.thursdaybegin, sd.thursdayend, sd.thursdaytotalhours,
                       sd.fridaybegin, sd.fridayend, sd.fridaytotalhours,
                       sd.saturdaybegin, sd.saturdayend, sd.saturdaytotalhours,
                       sd.sundaybegin, sd.sundayend, sd.sundaytotalhours
                FROM sysshiftdetail sd
                INNER JOIN sysuser u ON sd.userid = u.id
                INNER JOIN sysshift s ON sd.shiftid = s.id
                WHERE u.departmentid IN {TARGET_DEPARTMENTS} AND u.type IN {a_types}
            """)
            
            rows = cursor.fetchall()
            col_names = [column[0] for column in cursor.description]

            for row in rows:
                detail_id = row[0]
                # 处理 varchar 类型的总小时数
                try:
                    total_hours = float(str(row[1]).strip()) if row[1] else 0.0
                except ValueError:
                    total_hours = 0.0

                u_type = row[2]
                btrustid = row[3]
                period_begin = str(row[4]).strip() if row[4] else None
                limit = A_TYPE_LIMITS.get(u_type, 40)

                # 预检：是否存在单日工时超过 12 小时的情况
                has_over_12 = False
                for i in range(5, 26, 3):
                    try:
                        if float(str(row[i+2]).strip() or 0) > 12.0:
                            has_over_12 = True
                            break
                    except: pass

                if (round(total_hours, 2) != round(limit, 2) or has_over_12) and period_begin:
                    monday_date = datetime.datetime.strptime(period_begin, "%Y-%m-%d")
                    # 提取每天的数据用于调整
                    daily_data = []
                    for i in range(5, 26, 3): # 从 monday 到 sunday 的循环
                        begin_val = row[i]
                        end_val = row[i+1]
                        try:
                            t_val = float(str(row[i+2]).strip()) if row[i+2] else 0.0
                        except ValueError:
                            t_val = 0.0
                            
                        if t_val > 0:
                            daily_data.append((col_names[i+2], col_names[i+1], begin_val, end_val, t_val))
                    
                    result = adjust_shift_hours(detail_id, total_hours, limit, daily_data)
                    if result:
                        updates, new_total = result
                        update_sqls = [f"[{col}]='{val}'" for col, val in updates.items()]
                        update_sqls.append(f"totalhours='{new_total}'")
                        
                        sql = f"UPDATE sysshiftdetail SET {', '.join(update_sqls)} WHERE id={detail_id}"
                        cursor.execute(sql)

                        # 同步更新受影响日期的打卡数据
                        day_names = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                        for day_idx, day_prefix in enumerate(day_names):
                            if f"{day_prefix}end" in updates:
                                begin_time = str(row[5 + day_idx * 3]).strip()
                                new_end_time = updates[f"{day_prefix}end"]
                                target_date = monday_date + datetime.timedelta(days=day_idx)
                                update_punches(cursor, btrustid, target_date, begin_time, new_end_time)
                #print(f"row: {row} total_hours: {total_hours} limit: {limit} updates: {result if total_hours > limit else 'No adjustment needed'}")
            logging.info(f"Processed A-class shift adjustments for {len(rows)} records.")

            # --- 最终覆盖阶段：手动调整和特殊逻辑放在最后，确保不被自动平衡逻辑覆盖 ---
            cleanup_punch_problems(cursor)
            adjust_specific_a_users_holidays(cursor)
            cleanup_specific_holidays(cursor)
            adjust_user_20277(cursor)
            adjust_user_20194(cursor)
            adjust_users_20300_20278_20252(cursor)
            apply_manual_adjustments(cursor)
            logging.info("Applied final manual overrides and special user adjustments.")

if __name__ == "__main__":
    config_file = get_config_file()
    if not config_file:
        exit()
    config = configparser.ConfigParser()
    config.read(config_file, encoding="utf-8")
    _init(config)
    logging.basicConfig(level=logging.INFO)
    run_update()
