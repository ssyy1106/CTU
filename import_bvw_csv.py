import configparser
import csv
import datetime
import os
import sys

from helper import DBContext, _init


DEFAULT_CSV = "BVW20260113.csv"


def _load_config(config_path: str) -> None:
    config = configparser.ConfigParser()
    if not config.read(config_path, encoding="utf-8"):
        raise FileNotFoundError(f"Missing config file: {config_path}")
    _init(config)


def _is_header(row: list[str]) -> bool:
    if not row:
        return True
    first = (row[0] or "").strip()
    return not first.isdigit()


def import_bvw_csv(csv_path: str) -> None:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    inserted = 0
    with open(csv_path, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        with DBContext() as conn:
            with conn.cursor() as cursor:
                for row in reader:
                    if _is_header(row):
                        continue
                    if len(row) < 6:
                        continue
                    workno = row[0].strip()
                    date_str = row[2].strip()
                    time_str = row[3].strip()
                    try:
                        dt = datetime.datetime.strptime(
                            f"{date_str} {time_str}", "%m/%d/%Y %H:%M:%S"
                        )
                    except ValueError:
                        continue
                    year = dt.strftime("%Y")
                    month = dt.strftime("%m")
                    day = dt.strftime("%d")
                    hour = dt.strftime("%H")
                    minute = dt.strftime("%M")
                    punch_date = f"{year}-{month}-{day}"
                    sql = (
                        "Merge into SysPunch as t using (select "
                        f"'{workno}' as BtrustId, '{year}' as year, '{month}' as month, "
                        f"'{day}' as day, '{hour}' as hour, '{minute}' as minute) as s "
                        "on s.btrustid = t.btrustid and s.year = t.year and s.month = t.month "
                        "and s.day = t.day and s.hour = t.hour and s.minute = t.minute "
                        "when not matched then insert values("
                        f"'{workno}', '{year}', '{month}', '{day}', '{hour}', '{minute}', "
                        f"'0', '20', '0', '1', '0', 'CX7', '{punch_date}', null, null);"
                    )
                    cursor.execute(sql)
                    inserted += 1
    print(f"import done, rows processed: {inserted}")


def main() -> int:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config.ini")
    csv_path = os.path.join(base_dir, DEFAULT_CSV)

    if len(sys.argv) > 1:
        csv_path = sys.argv[1]

    _load_config(config_path)
    import_bvw_csv(csv_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
