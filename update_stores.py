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
    '25': 36, '6': 40, '26': 40, '7': 44, '27': 44, '31': 20
}
C_TYPES_TO_DELETE = (10,)
TYPES_TO_CLEAR_SHIFTS = (11, 12, 14, 33)
D_TYPES = (13, 32)
BS_TYPES = (0, 8, 9, 16, 17, 29, 34, 35, 36)

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

def adjust_shift_hours(current_total, limit, daily_data):
    """移植自 update_shift.py: 调整排班小时数逻辑"""
    MAX_DAILY_HOURS, MIN_DAILY_HOURS = 12.0, 3.0
    days = [{'total_idx': d[0], 'end_idx': d[1], 'begin_str': str(d[2]).strip() if d[2] else "", 'val': float(d[4]), 'modified': False} for d in daily_data]

    for d in days:
        if d['val'] > MAX_DAILY_HOURS:
            d['val'], d['modified'] = MAX_DAILY_HOURS, True

    diff = sum(d['val'] for d in days) - limit
    if round(diff, 2) > 0:
        for d in sorted(days, key=lambda x: x['val'], reverse=True):
            if diff <= 0: break
            can_reduce = d['val'] - MIN_DAILY_HOURS
            if can_reduce > 0:
                reduce = min(diff, can_reduce); d['val'] -= reduce; diff -= reduce; d['modified'] = True
    elif round(diff, 2) < 0:
        to_inc = abs(diff)
        for d in sorted(days, key=lambda x: x['val'], reverse=True):
            if to_inc <= 0: break
            can_add = MAX_DAILY_HOURS - d['val']
            if can_add > 0:
                add = min(to_inc, can_add); d['val'] += add; to_inc -= add; d['modified'] = True

    if not any(d['modified'] for d in days) and round(current_total, 2) == round(limit, 2): return None

    new_vals = {}
    for d in days:
        if d['modified']:
            try:
                h_s, m_s = map(int, d['begin_str'].split(':'))
                duration = d['val'] + (0.5 if d['val'] >= 5 else 0)
                mins = (h_s * 60 + m_s) + int(duration * 60)
                new_vals[d['total_idx']] = str(round(d['val'], 2))
                new_vals[d['end_idx']] = f"{(mins // 60) % 24:02d}:{mins % 60:02d}"
            except: continue
    return new_vals, str(round(sum(d['val'] for d in days), 2))

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
                    target_types = a_types + d_types
                    
                    cursor.execute(f"""
                        SELECT sd.id, sd.totalhours, u.type, u.btrustid, s.periodBegin,
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
                        
                        # 获取限制：A类从映射表读，D类固定44
                        limit = float(A_TYPE_LIMITS.get(str(row[2]), 44.0 if str(row[2]) in d_types else 168.0))
                        
                        # 预检：工时超标或单日 > 12h
                        needs_adj = (round(t_hours, 2) > round(limit, 2))
                        if not needs_adj:
                            for i in range(7):
                                try:
                                    if float(str(row[7 + i*3]).strip() or 0) > 12.0: 
                                        needs_adj = True
                                        break
                                except: pass

                        if needs_adj and p_begin:
                            # 准备每日数据进行调整
                            daily_data = []
                            for i in range(7):
                                h_val = float(str(row[7+i*3]).strip() or 0)
                                if h_val > 0:
                                    daily_data.append((col_names[7+i*3], col_names[6+i*3], row[5+i*3], row[6+i*3], h_val))
                            
                            adj_res = adjust_shift_hours(t_hours, limit, daily_data)
                            if adj_res:
                                updates, new_total = adj_res
                                update_sqls = [f"[{k}]='{v}'" for k, v in updates.items()]
                                cursor.execute(f"UPDATE sysshiftdetail SET {', '.join(update_sqls)}, totalhours='{new_total}' WHERE id={detail_id}")
                                
                                monday_dt = datetime.datetime.strptime(p_begin, "%Y-%m-%d")
                                for idx, day in enumerate(day_names):
                                    if f"{day}end" in updates:
                                        update_punches(cursor, b_id, monday_dt + datetime.timedelta(days=idx), str(row[5+idx*3]).strip(), updates[f"{day}end"])
                    
                    conn.commit()
                    logging.info(f"数据库 {db_name} 处理完成。最终激活部门数量: {len(final_keep_ids)}")
                    
        except Exception as e:
            logging.error(f"处理数据库 {db_name} 时发生错误: {e}")

if __name__ == "__main__":
    update_stores()