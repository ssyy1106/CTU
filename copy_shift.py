import logging
import random
import datetime
import configparser
import sys

# Assuming helper.py, snowflake.py, UserType.py are in the same directory or accessible via PYTHONPATH
from helper import DBContextUpdater, _init, get_config_file, DBContextCopy
from snowflake import Snowflake
from UserType import UserType # To get user type definitions

# --- Constants from update_shift.py for adjustment logic ---
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

# Define user type categories for filtering (as strings, matching DB 'type' column)
A_TYPES = {str(k) for k in A_TYPE_LIMITS.keys()}
# These B and S types are derived from UserType.py and common usage patterns
B_TYPES = {'8', '9', '29', '34', '35', '36'} # B, BSub, BPlus, B40, B40Sub, B44
D_TYPES = {'13', '28', '32'} # D
S_TYPES = {'0', '16', '17'} # S20, S, SSub
# 参考 update_shift.py 的定义：C+ (11), C+? (12), M (14), MStar (33)
TYPES_TO_CLEAR_SHIFTS = {'11', '12', '14', '33'} 

# Combine all target types for initial user selection
ALL_TARGET_TYPES = A_TYPES.union(B_TYPES).union(D_TYPES).union(S_TYPES).union(TYPES_TO_CLEAR_SHIFTS)

# 加拿大法定节假日 (主要针对 2025-2026 年，可根据需要扩展)
# 假设这些是安大略省的公共假日
CANADIAN_HOLIDAYS = {
    datetime.date(2025, 12, 25), # Christmas Day
    datetime.date(2025, 12, 26), # Boxing Day
    datetime.date(2026, 1, 1),   # New Year's Day
    datetime.date(2026, 2, 16),  # Family Day (Ontario)
    datetime.date(2026, 4, 3),   # Good Friday
    datetime.date(2026, 5, 18),  # Victoria Day
    datetime.date(2026, 7, 1),   # Canada Day
    datetime.date(2026, 9, 7),   # Labour Day
    datetime.date(2026, 10, 12), # Thanksgiving Day
    datetime.date(2026, 12, 25), # Christmas Day
    datetime.date(2026, 12, 26), # Boxing Day
}
# --- Helper functions adapted from update_shift.py ---

def update_punches(dest_cursor, btrustid, target_date, begin_time, end_time):
    """
    Updates punch records in the destination database for a given btrustid and date.
    Deletes existing records for the day and inserts new ones based on shift times with random offsets.
    """
    year_str = target_date.strftime("%Y")
    month_padded = target_date.strftime("%m")
    month_raw = str(target_date.month)
    day_padded = target_date.strftime("%d")
    day_raw = str(target_date.day)

    # 1. Delete existing punches for the day in the destination
    dest_cursor.execute(
        "DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
        (btrustid, year_str, month_padded, month_raw, day_padded, day_raw)
    )

    # 2. Parse shift times and generate new punches with random offsets
    try:
        bh, bm = map(int, begin_time.split(':'))
        eh, em = map(int, end_time.split(':'))
        
        # Only insert if there's actual work time (end time > begin time)
        if (eh * 60 + em) > (bh * 60 + bm):
            # Clock-in: random 0-5 minutes early
            b_dt = target_date.replace(hour=bh, minute=bm) - datetime.timedelta(minutes=random.randint(0, 5))
            # Clock-out: random 0-5 minutes late
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
                dest_cursor.execute(sql, (btrustid, h, m, mo, d, y, p_date))
    except Exception as e:
        logging.error(f"Error generating punches for {btrustid} on {target_date.strftime('%Y-%m-%d')}: {e}")

def adjust_shift_hours(current_total, limit, daily_data_list, holiday_indices=None):
    """
    Adjusts shift hours to ensure total is within limit and daily hours are reasonable.
    Returns (new_daily_values_dict, new_total_hours_str) or None if no adjustment needed.
    daily_data_list: list of dicts, each dict: {'day_idx': int, 'begin_str': str, 'end_str': str, 'total_h_val': float}
    """
    MAX_DAILY_HOURS = 12.0
    MIN_DAILY_HOURS = 3.0
    
    if holiday_indices is None:
        holiday_indices = []

    # Create a mutable copy of daily_data for adjustments
    days_to_adjust = []
    for d in daily_data_list:
        days_to_adjust.append({
            'day_idx': d['day_idx'],
            'begin_str': d['begin_str'],
            'end_str': d['end_str'], # Keep original end_str for recalculation if begin_str is valid
            'val': d['total_h_val'],
            'modified': False
        })

    # 1. Force all days over MAX_DAILY_HOURS to MAX_DAILY_HOURS
    for d in days_to_adjust:
        if d['val'] > MAX_DAILY_HOURS:
            d['val'] = MAX_DAILY_HOURS
            d['modified'] = True
        # Force 0 for holidays and mark as modified
        if d['day_idx'] in holiday_indices:
            d['val'] = 0.0
            d['modified'] = True

    # 2. Calculate current sum and difference from limit
    current_sum = sum(d['val'] for d in days_to_adjust)
    diff = current_sum - limit  # Positive: need to reduce; Negative: need to increase

    # Check if adjustment is actually needed
    if abs(round(diff, 2)) < 0.01 and not any(d['modified'] for d in days_to_adjust):
        return None # No significant adjustment needed

    if round(diff, 2) > 0:
        # Reduce hours: from longest days first, down to MIN_DAILY_HOURS
        for d in sorted(days_to_adjust, key=lambda x: x['val'], reverse=True):
            if d['day_idx'] in holiday_indices: # Don't reduce hours on holidays (already 0)
                continue
            if round(diff, 2) <= 0:
                break
            can_reduce = d['val'] - MIN_DAILY_HOURS
            if can_reduce > 0:
                reduce_amount = min(diff, can_reduce)
                d['val'] -= reduce_amount
                diff -= reduce_amount
                d['modified'] = True
    elif round(diff, 2) < 0:
        # Increase hours: distribute to days not at MAX_DAILY_HOURS
        to_increase = abs(diff)
        for d in sorted(days_to_adjust, key=lambda x: x['val']): # Try to fill smaller days first
            if round(to_increase, 2) <= 0:
                break
            if d['day_idx'] in holiday_indices: # Don't increase hours on holidays
                continue
            can_add = MAX_DAILY_HOURS - d['val']
            if can_add > 0:
                addition = min(to_increase, can_add)
                d['val'] += addition
                to_increase -= addition
                d['modified'] = True

    # Final check for total hours after adjustments
    final_total = sum(d['val'] for d in days_to_adjust)
    if abs(round(final_total, 2) - round(limit, 2)) > 0.01:
        logging.warning(f"Shift adjustment for limit {limit} resulted in {final_total}. Could not perfectly match.")

    # 3. Re-calculate end times and construct return values
    new_daily_values = {} # Maps column name to new value
    day_prefixes = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    for d in days_to_adjust:
        prefix = day_prefixes[d['day_idx']]
        # If it's a holiday, ensure it's 0 hours and 00:00-00:00
        if d['day_idx'] in holiday_indices:
            new_daily_values[f"{prefix}begin"] = "00:00"
            new_daily_values[f"{prefix}end"] = "00:00"
            new_daily_values[f"{prefix}totalhours"] = "0"
        # Only include modified days or days where total_h_val changed significantly (and not a holiday)
        if d['modified'] or (abs(round(d['val'], 2) - round(daily_data_list[d['day_idx']]['total_h_val'], 2)) > 0.01):
            prefix = day_prefixes[d['day_idx']]
            new_daily_values[f"{prefix}totalhours"] = str(round(d['val'], 2))
            
            # Recalculate end time if begin time is available and not "00:00"
            if d['begin_str'] and d['begin_str'] != "00:00" and d['val'] > 0:
                try:
                    h_start, m_start = map(int, d['begin_str'].split(':'))
                    # Add 0.5h for lunch if duration is >= 5 hours
                    duration_with_lunch = d['val'] + (0.5 if d['val'] >= 5.0 else 0)
                    end_total_minutes = (h_start * 60 + m_start) + int(duration_with_lunch * 60)
                    new_end_time = f"{(end_total_minutes // 60) % 24:02d}:{end_total_minutes % 60:02d}"
                    new_daily_values[f"{prefix}begin"] = d['begin_str'] # Keep original begin time
                    new_daily_values[f"{prefix}end"] = new_end_time
                except Exception as e:
                    logging.error(f"Error recalculating end time for day {d['day_idx']} (begin: {d['begin_str']}, val: {d['val']}): {e}")
                    # If error, set to 00:00 for safety
                    new_daily_values[f"{prefix}begin"] = "00:00"
                    new_daily_values[f"{prefix}end"] = "00:00"
                    new_daily_values[f"{prefix}totalhours"] = "0"
            else:
                # If no begin time or 0 hours, set to 00:00
                new_daily_values[f"{prefix}begin"] = "00:00"
                new_daily_values[f"{prefix}end"] = "00:00"
                new_daily_values[f"{prefix}totalhours"] = "0"
        else:
            # If not modified, ensure original values are used for consistency
            prefix = day_prefixes[d['day_idx']]
            new_daily_values[f"{prefix}begin"] = d['begin_str']
            new_daily_values[f"{prefix}end"] = d['end_str']
            new_daily_values[f"{prefix}totalhours"] = str(round(d['val'], 2))

    return new_daily_values, str(round(final_total, 2))


def copy_and_adjust_shifts():
    logging.info("Starting shift and punch data copy and adjustment script...")

    with DBContextCopy() as source_conn: # Source database (config.ini [sqlserver])
        with source_conn.cursor() as source_cursor:
            with DBContextUpdater() as dest_conn: # Destination database (config.ini [updatesqlserver])
                with dest_conn.cursor() as dest_cursor:

                    # 1. Get all relevant users from source
                    # Fetch all columns from sysuser to ensure a complete copy
                    source_cursor.execute(f"SELECT * FROM sysuser WHERE type IN ({','.join(ALL_TARGET_TYPES)})")
                    source_users_data = source_cursor.fetchall()
                    source_user_col_names = [desc[0] for desc in source_cursor.description]
                    
                    day_prefixes = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

                    for user_row_data in source_users_data:
                        # 转换所有键为小写以解决数据库字段大小写不统一导致的 KeyError
                        user_data_dict = {k.lower(): v for k, v in zip(source_user_col_names, user_row_data)}
                        user_id = user_data_dict['id']
                        btrustid = user_data_dict['btrustid']
                        real_name = user_data_dict['realname']
                        user_type_str = str(user_data_dict['type']) # Ensure type is string for comparison with sets

                        logging.info(f"Processing user: {btrustid} ({real_name}, Type: {user_type_str})")

                        # Check if user already exists in destination by btrustid
                        dest_cursor.execute("SELECT id FROM sysuser WHERE btrustid = ?", (btrustid,))
                        dest_user_row = dest_cursor.fetchone()
                        
                        if dest_user_row:
                            dest_user_id = dest_user_row[0] # Get existing ID from destination
                            logging.info(f"User {btrustid} already exists in destination with ID {dest_user_id}. Skipping sysuser insert.")
                        else:
                            # Use the original source user_id for the new record in destination
                            dest_user_id = user_id
                            try:
                                user_data_dict['id'] = dest_user_id # Ensure the 'id' in user_data_dict matches the desired dest_user_id for insertion
                                
                                cols = ', '.join(source_user_col_names)
                                placeholders = ', '.join(['?' for _ in source_user_col_names])
                                insert_sql = f"INSERT INTO sysuser ({cols}) VALUES ({placeholders})"
                                # 按照原始列名的顺序（转小写后）从字典中提取值进行插入
                                insert_values = [user_data_dict[col.lower()] for col in source_user_col_names]
                                dest_cursor.execute(insert_sql, tuple(insert_values))
                                logging.info(f"Inserted user {btrustid} into destination with original ID {dest_user_id}.")
                            except Exception as e:
                                logging.error(f"Failed to insert user {btrustid} into destination: {e}")
                                continue # Skip to next user if user insertion fails

                        # 2. Copy/Adjust Shifts and Punches
                        # 如果是特定类型 (C+, C+?, M 类)，则只同步用户信息，不复制排班和打卡
                        if user_type_str in TYPES_TO_CLEAR_SHIFTS:
                            logging.info(f"User {btrustid} (Type: {user_type_str}) is in TYPES_TO_CLEAR_SHIFTS. Skipping shift/punch copy.")
                            continue

                        # Fetch all shift details for the current user from the source
                        source_cursor.execute(
                            """
                            SELECT s.id, s.periodBegin, s.departmentId,
                                   sd.id, sd.userid, sd.mondaybegin, sd.mondayend, sd.mondaytotalhours,
                                   sd.tuesdaybegin, sd.tuesdayend, sd.tuesdaytotalhours,
                                   sd.wednesdaybegin, sd.wednesdayend, sd.wednesdaytotalhours,
                                   sd.thursdaybegin, sd.thursdayend, sd.thursdaytotalhours,
                                   sd.fridaybegin, sd.fridayend, sd.fridaytotalhours,
                                   sd.saturdaybegin, sd.saturdayend, sd.saturdaytotalhours,
                                   sd.sundaybegin, sd.sundayend, sd.sundaytotalhours, sd.totalhours, d.lunchminute
                            FROM sysshift s
                            INNER JOIN sysshiftdetail sd ON s.id = sd.shiftid
                            INNER JOIN sysdepartment d ON s.departmentId = d.id
                            WHERE sd.userid = ?
                            ORDER BY s.periodBegin
                            """, (user_id,) # Use source user_id to fetch their shifts
                        )
                        source_shifts_details = source_cursor.fetchall()

                        for shift_row in source_shifts_details:
                            (source_shift_id, period_begin, department_id,
                             source_detail_id, _, # userid is not needed here, we use dest_user_id
                             *daily_shift_data_raw, # Contains begin, end, totalhours for each day
                             total_hours_str, lunch_minute) = shift_row

                            period_begin_str = str(period_begin).strip()
                            monday_date = datetime.datetime.strptime(period_begin_str, "%Y-%m-%d")
                            
                            # Check if shift (sysshift) already exists in destination for this period and department
                            # Use the original source shift ID if it doesn't exist in destination
                            dest_cursor.execute(
                                "SELECT id FROM sysshift WHERE periodBegin = ? AND departmentId = ?",
                                (period_begin_str, department_id)
                            )
                            # If a shift with the same periodBegin and departmentId exists, use its ID.
                            # Otherwise, use the source_shift_id for insertion.
                            # This prevents duplicate shifts in the destination if they were already copied
                            # but their details were not (e.g., due to an error or previous run).
                            # However, the current logic for sysshiftdetail insertion already checks for existence
                            # based on shiftid and userid, so this might be redundant or need careful consideration
                            # if source_shift_id can clash with existing dest_shift_id for different periodBegin/departmentId.
                            # For now, we'll assume source_shift_id is unique across all shifts.
                            dest_shift_row = dest_cursor.fetchone()
                            
                            if dest_shift_row:
                                dest_shift_id = dest_shift_row[0]
                            else:
                                dest_shift_id = source_shift_id # Use the original source shift ID
                                dest_cursor.execute(
                                    "INSERT INTO sysshift(id, periodBegin, departmentId) VALUES (?, ?, ?)",
                                    (dest_shift_id, period_begin_str, department_id)
                                )
                                logging.info(f"Inserted new sysshift {dest_shift_id} for {period_begin_str} in dept {department_id}.")

                            # Check if shiftdetail already exists for this user and shift in destination
                            dest_cursor.execute(
                                "SELECT id FROM sysshiftdetail WHERE shiftid = ? AND userid = ?",
                                (dest_shift_id, dest_user_id)
                            )
                            dest_detail_row = dest_cursor.fetchone()

                            if dest_detail_row:
                                logging.info(f"Shift detail for user {btrustid} and shift {period_begin_str} already exists in destination. Skipping.")
                                continue # Skip this shiftdetail if it already exists

                            # Prepare daily data for adjustment
                            daily_data_for_adjustment = []
                            current_total_hours = float(str(total_hours_str).strip() or 0)
                            
                            holiday_indices_for_week = []
                            for i in range(7): # 0=Monday, 6=Sunday
                                begin_str = str(daily_shift_data_raw[i*3]).strip() if daily_shift_data_raw[i*3] else "00:00"
                                end_str = str(daily_shift_data_raw[i*3+1]).strip() if daily_shift_data_raw[i*3+1] else "00:00"
                                total_h_val = float(str(daily_shift_data_raw[i*3+2]).strip() or 0)
                                
                                # 检查是否为节假日（仅针对 D 类）
                                current_date = (monday_date + datetime.timedelta(days=i)).date()
                                if user_type_str in D_TYPES and current_date in CANADIAN_HOLIDAYS:
                                    holiday_indices_for_week.append(i)

                                daily_data_for_adjustment.append({
                                    'day_idx': i,
                                    'begin_str': begin_str,
                                    'end_str': end_str,
                                    'total_h_val': total_h_val
                                })

                            adjusted_daily_values = None
                            new_total_hours_after_adjustment = current_total_hours
                            
                            # Apply adjustments for A and D types
                            if user_type_str in A_TYPES:
                                limit = A_TYPE_LIMITS.get(user_type_str, 40) # Default to 40 if not found
                                result = adjust_shift_hours(current_total_hours, limit, daily_data_for_adjustment)
                                if result:
                                    adjusted_daily_values, new_total_hours_after_adjustment = result
                                    logging.info(f"Adjusted A-type user {btrustid} shift for {period_begin_str} to {new_total_hours_after_adjustment}h (limit {limit}h).")
                            elif user_type_str in D_TYPES:
                                limit = 44.0 # D-type specific limit
                                result = adjust_shift_hours(current_total_hours, limit, daily_data_for_adjustment, holiday_indices=holiday_indices_for_week)
                                if result:
                                    adjusted_daily_values, new_total_hours_after_adjustment = result
                                    logging.info(f"Adjusted D-type user {btrustid} shift for {period_begin_str} to {new_total_hours_after_adjustment}h (limit {limit}h).")

                            # Construct INSERT statement for sysshiftdetail
                            dest_detail_id = source_detail_id # Use the original source shiftdetail ID
                            insert_detail_cols = [
                                "id", "shiftid", "userid",
                                "mondaybegin", "mondayend", "mondaytotalhours",
                                "tuesdaybegin", "tuesdayend", "tuesdaytotalhours",
                                "wednesdaybegin", "wednesdayend", "wednesdaytotalhours",
                                "thursdaybegin", "thursdayend", "thursdaytotalhours",
                                "fridaybegin", "fridayend", "fridaytotalhours",
                                "saturdaybegin", "saturdayend", "saturdaytotalhours",
                                "sundaybegin", "sundayend", "sundaytotalhours",
                                "totalhours"
                            ]
                            insert_detail_values = [dest_detail_id, dest_shift_id, dest_user_id]
                            
                            for i in range(7):
                                day_prefix = day_prefixes[i]
                                if adjusted_daily_values:
                                    insert_detail_values.append(adjusted_daily_values.get(f"{day_prefix}begin", daily_data_for_adjustment[i]['begin_str']))
                                    insert_detail_values.append(adjusted_daily_values.get(f"{day_prefix}end", daily_data_for_adjustment[i]['end_str']))
                                    insert_detail_values.append(adjusted_daily_values.get(f"{day_prefix}totalhours", str(round(daily_data_for_adjustment[i]['total_h_val'], 2))))
                                else: # No adjustment or not a D-type on holiday, use original values
                                    insert_detail_values.append(daily_data_for_adjustment[i]['begin_str'])
                                    insert_detail_values.append(daily_data_for_adjustment[i]['end_str'])
                                    insert_detail_values.append(str(round(daily_data_for_adjustment[i]['total_h_val'], 2)))
                            
                            insert_detail_values.append(str(round(float(new_total_hours_after_adjustment), 2)))

                            placeholders = ', '.join(['?' for _ in insert_detail_cols])
                            insert_sql = f"INSERT INTO sysshiftdetail ({','.join(insert_detail_cols)}) VALUES ({placeholders})"
                            dest_cursor.execute(insert_sql, tuple(insert_detail_values))
                            logging.info(f"Inserted sysshiftdetail {dest_detail_id} for user {btrustid} and shift {period_begin_str}.")

                            # 3. Copy/Adjust Punches (delete and re-insert based on final shift details)
                            # monday_date is already defined as datetime.date, convert to datetime for replace
                            for i in range(7):
                                target_date = monday_date + datetime.timedelta(days=i)
                                
                                current_day_begin = daily_data_for_adjustment[i]['begin_str']
                                current_day_end = daily_data_for_adjustment[i]['end_str']
                                current_day_total_h = daily_data_for_adjustment[i]['total_h_val']

                                if adjusted_daily_values:
                                    adjusted_day_begin = adjusted_daily_values.get(f"{day_prefixes[i]}begin", current_day_begin)
                                    adjusted_day_end = adjusted_daily_values.get(f"{day_prefixes[i]}end", current_day_end)
                                    adjusted_day_total_h = float(adjusted_daily_values.get(f"{day_prefixes[i]}totalhours", str(current_day_total_h)))
                                else:
                                    adjusted_day_begin = current_day_begin
                                    adjusted_day_end = current_day_end
                                    adjusted_day_total_h = current_day_total_h

                                # Only update punches if there's actual work scheduled for the day
                                if adjusted_day_total_h > 0 and adjusted_day_begin != "00:00" and adjusted_day_end != "00:00":
                                    update_punches(dest_cursor, btrustid, target_date, adjusted_day_begin, adjusted_day_end)
                                else:
                                    # If total hours for the day is 0 or shift times are "00:00", ensure no punches exist
                                    year_str = target_date.strftime("%Y")
                                    month_padded = target_date.strftime("%m")
                                    month_raw = str(target_date.month)
                                    day_padded = target_date.strftime("%d")
                                    day_raw = str(target_date.day)
                                    dest_cursor.execute(
                                        "DELETE FROM syspunch WHERE btrustid=? AND year=? AND month IN (?, ?) AND day IN (?, ?)",
                                        (btrustid, year_str, month_padded, month_raw, day_padded, day_raw)
                                    )
                                    
                    dest_conn.commit()
                    logging.info("Shift and punch data copy and adjustment finished.")

if __name__ == "__main__":
    config_file = get_config_file()
    if not config_file:
        sys.exit("Config file not found.")
    
    config = configparser.ConfigParser()
    config.read(config_file, encoding="utf-8")
    _init(config) # Initialize global CONFIG in helper.py
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        copy_and_adjust_shifts()
    except Exception as e:
        logging.exception("An error occurred during the copy and adjustment process.")