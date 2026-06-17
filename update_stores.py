import configparser
import pyodbc
import logging
import sys
import os
import random
import datetime
from collections import defaultdict
from helper import get_config_file, get_sqlserver_driver

# --- 从 update_shift.py 移植的常量 ---
A_TYPE_LIMITS = {
    '1': 30, '19': 30, '2': 15, '22': 15, '3': 24, '23': 24,
    '125': 25, '4': 30, '24': 30, '30': 30, '135': 35, '5': 36,
    '25': 36, '6': 40, '26': 40, '7': 44, '27': 44, '31': 20,
    '35': 40, '36': 10
}
C_TYPES_TO_DELETE = (10,)
TYPES_TO_CLEAR_SHIFTS = (11, 12, 14, 33)
D_TYPES = (13, 32, 37, 38, 39)
BS_TYPES = (0, 8, 9, 16, 17, 29, 34, 35, 36)

# 加拿大法定节假日
CANADIAN_HOLIDAYS = {
    datetime.date(2025, 12, 25), datetime.date(2025, 12, 26),
    datetime.date(2026, 1, 1),   datetime.date(2026, 2, 16),
    datetime.date(2026, 4, 3),   datetime.date(2026, 5, 18),
    datetime.date(2026, 7, 1),   datetime.date(2026, 9, 7),
    datetime.date(2026, 10, 12), datetime.date(2026, 12, 25),
    datetime.date(2026, 12, 26),
}

def to_sql_list(items):
    """将集合/元组转换为 SQL IN 子句使用的字符串，处理单元素元组的逗号问题并防止空列表"""
    if not items:
        return "(NULL)"  # 防止 IN () 导致的语法错误
    formatted = [f"'{x}'" if isinstance(x, str) else str(x) for x in items]
    return f"({','.join(formatted)})"

def update_punches(cursor, btrustid, target_date, begin_time, end_time):
    """移植自 update_shift.py: 更新打卡记录，带随机偏移"""
    y, m_p, d_p = target_date.strftime("%Y"), target_date.strftime("%m"), target_date.strftime("%d")
    m_r, d_r = str(target_date.month), str(target_date.day)

    cursor.execute(
        "DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
        (btrustid, y, m_p, m_r, d_p, d_r)
    )
    try:
        bh, bm = map(int, begin_time.split(':'))
        eh, em = map(int, end_time.split(':'))
        b_dt = target_date.replace(hour=bh, minute=bm) - datetime.timedelta(minutes=random.randint(0, 5))
        e_dt = target_date.replace(hour=eh, minute=em) + datetime.timedelta(minutes=random.randint(0, 5))

        for dt in [b_dt, e_dt]:
            h, m, mo, d = str(dt.hour).zfill(2), str(dt.minute).zfill(2), str(dt.month).zfill(2), str(dt.day).zfill(2)
            p_date = dt.strftime("%Y-%m-%d")
            sql = """INSERT INTO SysPunch(HandRec, SiteNumber, ClockNumber, ClockCode, BtrustId, 
                     WorkCode, Hour, Minute, Month, Day, Year, PunchDate) VALUES ('0', '3', '0', '1', ?, '0', ?, ?, ?, ?, ?, ?)"""
            cursor.execute(sql, (btrustid, h, m, mo, d, y, p_date))
    except Exception as e:
        logging.error(f"Failed to generate punches for {btrustid} on {y}-{m_r}-{d_r}: {e}")

def adjust_shift_hours(current_total, limit, daily_data, fixed_indices=None):
    """移植自 update_shift.py: 调整排班小时数逻辑"""
    # 根据周小时数限制确定每日最大工时限制
    if limit < 20:
        MAX_DAILY_HOURS = 3.0
    elif limit == 20:
        MAX_DAILY_HOURS = 4.0
    elif limit < 35:
        MAX_DAILY_HOURS = 6.0
    elif limit < 40:
        MAX_DAILY_HOURS = 7.0
    else:
        MAX_DAILY_HOURS = 8.0

    MIN_DAILY_HOURS = 3.0
    if fixed_indices is None:
        fixed_indices = []

    days = []
    for i, d in enumerate(daily_data):
        days.append({
            'total_idx': d[0], # Column name for totalhours (e.g., 'mondaytotalhours')
            'end_idx': d[1],   # Column name for end time (e.g., 'mondayend')
            'begin_idx': d[2], # Column name for begin time (e.g., 'mondaybegin')
            'begin_str': str(d[3]).strip() if d[3] else "00:00", # Original begin time string
            'end_str': str(d[4]).strip() if d[4] else "00:00",   # Original end time string
            'val': float(d[5]), # Current daily total hours (can be modified)
            'original_val': float(d[5]), # Original daily total hours
            'original_begin_str': str(d[3]).strip() if d[3] else "00:00", # Original begin time for comparison
            'original_end_str': str(d[4]).strip() if d[4] else "00:00",   # Original end time for comparison
            'modified': False,
            'fixed': i in fixed_indices
        })
    # 1. 处理固定日期（如节假日）和每日上限
    for d in days:
        if d['fixed']:
            if d['val'] != 0:
                d['val'], d['modified'] = 0.0, True
        elif d['val'] > MAX_DAILY_HOURS:
            d['val'], d['modified'] = MAX_DAILY_HOURS, True

    diff = sum(d['val'] for d in days) - limit
    # 如果当前总工时超过限制，参考 update_shift.py 的逻辑，
    # 采取“重新生成”策略：清空非固定日期并重新分配，以确保排班结构合理且不超额。
    if round(diff, 2) > 0:
        # 1. 直接清空所有非固定日期的工时（模拟“删除”逻辑）
        for d in days:
            if not d['fixed']:
                d['val'] = 0.0
                d['modified'] = True
        
        # 2. 重新分配 limit 到非固定日期（生成新的符合要求的排班）
        to_distribute = limit
        available_days = [d for d in days if not d['fixed']]
        if available_days:
            random.shuffle(available_days) # 随机化分配天数
            for d in available_days:
                if round(to_distribute, 2) <= 0: break
                take = min(to_distribute, MAX_DAILY_HOURS)
                d['val'] = take
                to_distribute -= take

    if round(diff, 2) < 0:
        to_inc = abs(diff)
        for d in sorted(days, key=lambda x: x['val'], reverse=True):
            if to_inc <= 0: break
            if d['fixed']: continue
            can_add = MAX_DAILY_HOURS - d['val']
            if can_add > 0:
                add = min(to_inc, can_add); d['val'] += add; to_inc -= add; d['modified'] = True

    if not any(d['modified'] for d in days) and round(current_total, 2) == round(limit, 2): return None

    new_vals = {}
    # Construct the dictionary of new values for SQL update
    for d in days:
        new_begin_str = d['begin_str']
        new_end_str = d['end_str']

        # 使用四舍五入判断，防止浮点数微小值导致产生 10:00-10:00 这种工时为0但时间非0的异常排班
        if round(d['val'], 2) <= 0:
            # If daily hours are 0, force begin/end to "00:00" and total to "0"
            new_begin_str = "00:00"
            new_end_str = "00:00"
            new_total_h_str = "0"
        else:
            # If daily hours > 0, calculate end time based on begin time and duration
            try:
                # If begin_str is "00:00" but val > 0, use a default start time for calculation
                effective_begin_str = d['begin_str']
                if effective_begin_str == "00:00":
                    # 如果是新生成的班次，随机选择 09:00 或 10:00 开始
                    effective_begin_str = f"{random.choice([9, 10]):02d}:00"

                h_start, m_start = map(int, effective_begin_str.split(':'))
                duration = d['val'] + (0.5 if d['val'] >= 5 else 0)
                mins = (h_start * 60 + m_start) + int(round(duration * 60))
                new_begin_str = effective_begin_str # Keep the effective begin string
                new_end_str = f"{(mins // 60) % 24:02d}:{mins % 60:02d}"
                new_total_h_str = str(round(d['val'], 2))
            except Exception as e:
                logging.error(f"Error recalculating end time for day (begin: {d['begin_str']}, val: {d['val']}): {e}")
                # Fallback to 00:00 if calculation fails
                new_begin_str = "00:00"
                new_end_str = "00:00"
                new_total_h_str = "0"
        
        # Only add to new_vals if any of the values (begin, end, totalhours) have changed
        if (new_begin_str != d['original_begin_str'] or
            new_end_str != d['original_end_str'] or
            float(new_total_h_str) != d['original_val']):
            
            new_vals[d['begin_idx']] = new_begin_str
            new_vals[d['end_idx']] = new_end_str
            new_vals[d['total_idx']] = new_total_h_str

    if not new_vals: # If no individual day values changed, and total didn't change significantly
        return None

    final_total = sum(d['val'] for d in days)
    return new_vals, str(round(final_total, 2))

def fetch_sysmenu_data(conn_str):
    """从源数据库获取 sysmenu 表的数据"""
    try:
        with pyodbc.connect(conn_str, timeout=15) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM sysmenu")
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                return columns, rows
    except Exception as e:
        logging.error(f"获取源 sysmenu 数据失败: {e}")
        return None, None

def sync_sysmenu_table(cursor, columns, rows):
    """同步 sysmenu 表到目标数据库：仅根据相同 ID 更新 menustatus 字段"""
    if not columns or rows is None:
        return

    # 获取字段索引，不区分大小写
    col_map = {col.lower(): i for i, col in enumerate(columns)}
    if 'id' not in col_map or 'menustatus' not in col_map:
        logging.error("源 sysmenu 数据缺少 'id' 或 'menustatus' 字段，无法执行更新。")
        return

    id_idx = col_map['id']
    status_idx = col_map['menustatus']

    # 准备批量更新数据：[(menustatus_val, id_val), ...]
    update_data = [(row[status_idx], row[id_idx]) for row in rows]

    try:
        sql = "UPDATE sysmenu SET menustatus = ? WHERE id = ?"
        cursor.executemany(sql, update_data)
        logging.info(f"成功更新 sysmenu 表的 menustatus 字段，共处理 {len(update_data)} 条记录。")
    except Exception as e:
        logging.error(f"更新 sysmenu 表 menustatus 失败: {e}")

def update_stores():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 获取配置文件路径
    config_file = get_config_file()
    if not config_file:
        logging.error("未找到配置文件。")
        return
        
    config = configparser.ConfigParser()
    try:
        config.read(config_file, encoding="utf-8")
    except Exception as e:
        logging.error(f"读取配置文件失败: {e}")
        return
    
    # 检查配置节是否存在
    if 'updatesqlservers' not in config:
        logging.error("配置文件中未找到 [updatesqlservers] 节。")
        return

    section = config['updatesqlservers']
    
    def parse_list(key):
        val = section.get(key, '')
        # 兼容中文逗号和英文逗号，并过滤空字符串
        return [v.strip() for v in val.replace('，', ',').split(',') if v.strip()]

    names = parse_list('name')
    hosts = parse_list('host')
    passwords = parse_list('password')
    databases = parse_list('database')
    departments = parse_list('department')

    # 确定需要处理的数据库数量
    count = min(len(names), len(hosts), len(passwords), len(databases), len(departments))
    if count == 0:
        logging.error("[updatesqlservers] 配置信息不足。")
        return
    
    # 自动获取驱动
    try:
        driver, is18 = get_sqlserver_driver()
    except Exception as e:
        logging.error(f"驱动检测失败: {e}")
        return

    # --- 从 [updatesqlserver] 读取 sysmenu 作为同步源 ---
    menu_cols, menu_rows = None, None
    if 'updatesqlserver' in config:
        src_conf = config['updatesqlserver']
        src_user = src_conf.get('name')
        src_host = src_conf.get('host')
        src_pw = src_conf.get('password')
        src_db = src_conf.get('database')
        
        src_conn_str = f'DRIVER={{{driver}}};SERVER={src_host};DATABASE={src_db};UID={src_user};PWD={src_pw};'
        if is18:
            src_conn_str += "Encrypt=yes;TrustServerCertificate=yes;"
        
        logging.info(f"正在从源数据库 {src_db} ({src_host}) 抓取 sysmenu 数据...")
        menu_cols, menu_rows = fetch_sysmenu_data(src_conn_str)
    else:
        logging.warning("配置文件中未定义 [updatesqlserver]，将无法同步 sysmenu。")

    for i in range(count):
        db_user = names[i]
        db_host = hosts[i]
        db_pw = passwords[i]
        db_name = databases[i]
        target_name = departments[i]
        
        logging.info(f"[{i+1}/{count}] 正在连接数据库 {db_name} ({db_host})，目标部门: {target_name}")
        
        conn_str = f'DRIVER={{{driver}}};SERVER={db_host};DATABASE={db_name};UID={db_user};PWD={db_pw};'
        if is18:
            conn_str += "Encrypt=yes;TrustServerCertificate=yes;"
            
        try:
            with pyodbc.connect(conn_str, timeout=10) as conn:
                with conn.cursor() as cursor:
                    # A. 首先同步 sysmenu 表数据
                    if menu_cols:
                        sync_sysmenu_table(cursor, menu_cols, menu_rows)

                    # 1. 获取所有部门数据以在内存中构建层级关系
                    cursor.execute("SELECT id, parentid, departmentName, ISNULL(baseisdelete, 0) FROM sysdepartment")
                    rows = cursor.fetchall()
                    
                    id_to_parent = {row[0]: row[1] for row in rows}
                    name_to_id = {row[2]: row[0] for row in rows}
                    originally_active_ids = {row[0] for row in rows if int(row[3]) == 0}
                    
                    if target_name not in name_to_id:
                        logging.warning(f"在数据库 '{db_name}' 中未找到目标部门 '{target_name}'。跳过。")
                        continue
                    
                    target_id = name_to_id[target_name]
                    keep_ids = {target_id}
                    action_dept_ids = {target_id}
                    
                    # 2. 向上寻找所有祖先 (Ancestors)
                    curr = target_id
                    while curr in id_to_parent and id_to_parent[curr] != 0:
                        curr = id_to_parent[curr]
                        keep_ids.add(curr)
                    
                    # 3. 向下寻找所有子孙 (Descendants) - 这些是业务处理的目标部门
                    children = defaultdict(list)
                    for d_id, p_id in id_to_parent.items():
                        children[p_id].append(d_id)
                    
                    queue = [target_id]
                    while queue:
                        pid = queue.pop(0)
                        for cid in children.get(pid, []):
                            if cid not in action_dept_ids:
                                action_dept_ids.add(cid)
                                keep_ids.add(cid)
                                queue.append(cid)
                    
                    # 新增：递归寻找 Montreal 及其所有子部门 ID
                    montreal_dept_ids = set()
                    if 'Montreal' in name_to_id:
                        m_id = name_to_id['Montreal']
                        montreal_dept_ids.add(m_id)
                        m_queue = [m_id]
                        while m_queue:
                            curr_m = m_queue.pop(0)
                            for c_id in children.get(curr_m, []):
                                if c_id not in montreal_dept_ids:
                                    montreal_dept_ids.add(c_id)
                                    m_queue.append(c_id)

                    # 4. 更新部门删除状态
                    cursor.execute("UPDATE sysdepartment SET baseisdelete = 1")
                    final_keep_ids = keep_ids.intersection(originally_active_ids)
                    if final_keep_ids:
                        placeholders = ",".join(["?"] * len(final_keep_ids))
                        cursor.execute(f"UPDATE sysdepartment SET baseisdelete = 0 WHERE id IN ({placeholders})", list(final_keep_ids))

                    # 5. 清理异常打卡记录 (syspunchproblem)
                    action_dept_list = list(action_dept_ids)
                    placeholders_action = ",".join(["?"] * len(action_dept_list))
                    cursor.execute(f"""DELETE FROM syspunchproblem WHERE userid IN (
                                       SELECT id FROM sysuser WHERE departmentid IN ({placeholders_action}) 
                                       AND type NOT IN {to_sql_list(BS_TYPES)})""", action_dept_list)

                    # 6. 处理 C 类员工 (Type 10): 删除人、排班、打卡
                    cursor.execute(f"SELECT id, btrustid FROM sysuser WHERE departmentid IN ({placeholders_action}) AND type IN {to_sql_list(C_TYPES_TO_DELETE)}", action_dept_list)
                    c_users = cursor.fetchall()
                    if c_users:
                        u_ids = [str(r[0]) for r in c_users]
                        b_ids = [f"'{r[1]}'" for r in c_users if r[1]]
                        cursor.execute(f"DELETE FROM sysshiftdetail WHERE userid IN ({','.join(u_ids)})")
                        if b_ids: cursor.execute(f"DELETE FROM syspunch WHERE btrustid IN ({','.join(b_ids)})")
                        cursor.execute(f"DELETE FROM sysuser WHERE id IN ({','.join(u_ids)})")
                        logging.info(f"Database {db_name}: Deleted {len(c_users)} C-class users.")

                    # 7. 处理清理类员工 (11, 12, 14, 33): 仅清空排班和打卡
                    cursor.execute(f"SELECT id, btrustid FROM sysuser WHERE departmentid IN ({placeholders_action}) AND type IN {to_sql_list(TYPES_TO_CLEAR_SHIFTS)}", action_dept_list)
                    clear_users = cursor.fetchall()
                    if clear_users:
                        u_ids = [str(r[0]) for r in clear_users]
                        b_ids = [f"'{r[1]}'" for r in clear_users if r[1]]
                        cursor.execute(f"DELETE FROM sysshiftdetail WHERE userid IN ({','.join(u_ids)})")
                        if b_ids: cursor.execute(f"DELETE FROM syspunch WHERE btrustid IN ({','.join(b_ids)})")
                        logging.info(f"Database {db_name}: Cleared shifts for {len(clear_users)} users.")

                    # 8. 处理 A 类和 D 类员工的排班平衡
                    a_types = tuple(A_TYPE_LIMITS.keys())
                    d_types = tuple(map(str, D_TYPES))
                    bs_special_types = ('8', '16', '34')
                    target_types = a_types + d_types + bs_special_types
                    
                    cursor.execute(f"""
                        SELECT sd.id, sd.totalhours, u.type, u.btrustid, s.periodBegin,
                               sd.mondaybegin, sd.mondayend, sd.mondaytotalhours,
                               sd.tuesdaybegin, sd.tuesdayend, sd.tuesdaytotalhours,
                               sd.wednesdaybegin, sd.wednesdayend, sd.wednesdaytotalhours,
                               sd.thursdaybegin, sd.thursdayend, sd.thursdaytotalhours,
                               sd.fridaybegin, sd.fridayend, sd.fridaytotalhours,
                               sd.saturdaybegin, sd.saturdayend, sd.saturdaytotalhours,
                               sd.sundaybegin, sd.sundayend, sd.sundaytotalhours,
                               u.departmentid
                        FROM sysshiftdetail sd
                        INNER JOIN sysuser u ON sd.userid = u.id
                        INNER JOIN sysshift s ON sd.shiftid = s.id
                        WHERE u.departmentid IN ({placeholders_action}) AND u.type IN {to_sql_list(target_types)}
                    """, action_dept_list)
                    
                    shift_rows = cursor.fetchall()
                    col_names = [c[0].lower() for c in cursor.description]
                    day_names = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

                    for row in shift_rows:
                        detail_id, b_id, p_begin = row[0], row[3], str(row[4]).strip() if row[4] else None
                        try: 
                            t_hours = float(str(row[1]).strip() or 0)
                        except: 
                            t_hours = 0.0
                        
                        # 获取限制：A类从映射表读，D类固定44，B/S/BQuest根据店面动态判断
                        u_type_str = str(row[2])
                        if u_type_str in a_types:
                            limit = float(A_TYPE_LIMITS.get(u_type_str, 40.0))
                        elif u_type_str in d_types:
                            limit = 44.0
                        elif u_type_str in bs_special_types:
                            limit = 40.0 if row[26] in montreal_dept_ids else 44.0
                        else:
                            limit = 168.0
                        
                        # 获取每日最大工时限制 (基于周限制)
                        if limit < 20:
                            max_daily = 3.0
                        elif limit == 20:
                            max_daily = 4.0
                        elif limit < 35:
                            max_daily = 6.0
                        elif limit < 40:
                            max_daily = 7.0
                        else:
                            max_daily = 8.0

                        # 节假日判定逻辑（针对 B10Sub 类型 36）
                        fixed_indices = []
                        if str(row[2]) == '36' and p_begin:
                            monday_dt = datetime.datetime.strptime(p_begin, "%Y-%m-%d")
                            for i in range(7):
                                if (monday_dt + datetime.timedelta(days=i)).date() in CANADIAN_HOLIDAYS and i not in fixed_indices:
                                    fixed_indices.append(i)

                        # 预检：判断是否需要调整排班
                        needs_adj = False
                        # Condition 1: Total hours don't match the limit
                        if abs(round(t_hours, 2) - round(limit, 2)) > 0.01:
                            needs_adj = True

                        # Condition 2: Check individual days for inconsistencies
                        # Only proceed if total hours are already fine, to find other issues, or if needs_adj is already true
                        if not needs_adj: 
                            for i in range(7):
                                try:
                                    h_val = float(str(row[7 + i*3]).strip() or 0)
                                    b_time = str(row[5 + i*3]).strip()
                                    e_time = str(row[6 + i*3]).strip()

                                    # If it's a fixed holiday and has hours
                                    if i in fixed_indices and h_val > 0:
                                        needs_adj = True; break
                                    # If daily hours exceed max_daily
                                    if h_val > max_daily:
                                        needs_adj = True; break
                                    # If daily hours are 0 but begin/end times are not "00:00"
                                    if h_val == 0 and (b_time not in ("", "00:00") or e_time not in ("", "00:00")):
                                        needs_adj = True; break
                                    # If daily hours are > 0 but begin/end times are "00:00" (inconsistent)
                                    if h_val > 0 and (b_time == "00:00" or e_time == "00:00"):
                                        needs_adj = True; break
                                except Exception as ex:
                                    logging.warning(f"Error checking daily shift for user {b_id} on day {i}: {ex}")
                                    needs_adj = True; break # Treat as needing adjustment if parsing fails

                        # If needs_adj is still false, but there are fixed holidays, we still need to process them
                        # to ensure they are set to 0 hours.
                        if not needs_adj and fixed_indices:
                            needs_adj = True

                        if needs_adj and p_begin:
                            # 准备每日数据进行调整
                            daily_data = []
                            for i in range(7):
                                h_val = float(str(row[7+i*3]).strip() or 0)
                                b_time = str(row[5+i*3]).strip()
                                e_time = str(row[6+i*3]).strip()
                                # Pass column names for begin, end, and totalhours, along with their values
                                daily_data.append((col_names[7+i*3], col_names[6+i*3], col_names[5+i*3], b_time, e_time, h_val))
                            
                            adj_res = adjust_shift_hours(t_hours, limit, daily_data, fixed_indices=fixed_indices)
                            if adj_res:
                                updates, new_total = adj_res

                                update_sqls = [f"[{k}] = ?" for k in updates.keys()]
                                params = list(updates.values())

                                update_sqls.append("totalhours = ?")
                                params.append(new_total)
                                params.append(detail_id)

                                sql = f"UPDATE sysshiftdetail SET {', '.join(update_sqls)} WHERE id = ?"
                                cursor.execute(sql, params)
                                
                                monday_dt = datetime.datetime.strptime(p_begin, "%Y-%m-%d")
                                for idx, day in enumerate(day_names):
                                    # Check if any of the begin, end, or totalhours for this day were updated
                                    begin_col_name = col_names[5+idx*3]
                                    end_col_name = col_names[6+idx*3]
                                    total_col_name = col_names[7+idx*3]

                                    if begin_col_name in updates or end_col_name in updates or total_col_name in updates:
                                        # Use the updated values if present, otherwise use the original values from 'row'
                                        new_b_time_for_punch = updates.get(begin_col_name, str(row[5+idx*3]).strip())
                                        new_e_time_for_punch = updates.get(end_col_name, str(row[6+idx*3]).strip())
                                        new_h_val_for_punch = float(updates.get(total_col_name, str(row[7+idx*3]).strip() or 0))

                                        # Only update punches if there's actual work scheduled for the day
                                        if new_h_val_for_punch > 0 and new_b_time_for_punch != "00:00" and new_e_time_for_punch != "00:00":
                                            update_punches(cursor, b_id, monday_dt + datetime.timedelta(days=idx), new_b_time_for_punch, new_e_time_for_punch)
                                        else:
                                            # If total hours for the day is 0 or shift times are "00:00", ensure no punches exist
                                            target_date = monday_dt + datetime.timedelta(days=idx)
                                            y, m_p, d_p = target_date.strftime("%Y"), target_date.strftime("%m"), target_date.strftime("%d")
                                            m_r, d_r = str(target_date.month), str(target_date.day)
                                            cursor.execute(
                                                "DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
                                                (b_id, y, m_p, m_r, d_p, d_r)
                                            )
                    
                    conn.commit()
                    logging.info(f"数据库 {db_name} 处理完成。最终激活部门数量: {len(final_keep_ids)}")
                    
        except Exception as e:
            logging.error(f"处理数据库 {db_name} 时发生错误: {e}")

if __name__ == "__main__":
    update_stores()