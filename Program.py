import configparser
import logging
from helper import get_config_file, set_logging, _init
from TC import read_punch
from CX7 import read_cx7
from SendHREmail import (
    send_hr_email_sin,
    send_hr_email_benefit,
    send_hr_email_hire,
    send_hr_email_working_visa,
    send_hr_email_mp,
)
from HQShift import set_hq_shift, calculate_employee_day_hours
from RandomShift import send_hr_email_shift
import sys

if __name__ == "__main__":
    config_file = get_config_file()
    if not config_file:
        exit()
    config = configparser.ConfigParser()
    config.read(config_file, encoding="utf-8")
    _init(config)
    set_logging()
    #计算员工每日工时
    calculate_employee_day_hours(config)
    logging.info('calculate employee day hours finish')
    exit()

    read_punch()
    # Read data from https://api.us.crosschexcloud.com
    read_cx7(config)
    # 发送Sin number快过期的人给HR
    send_hr_email_sin(config)
    logging.info('Send HR SIN Email finish')
    # 发送Working Visa快过期的人给HR
    send_hr_email_working_visa(config)
    logging.info('Send HR Working Visa Email finish')
    # 发送随机排班表给HR
    # send_hr_email_shift(config)
    # logging.info('Send HR Shift finish')
    # 总部各部门自动排班，在周日凌晨，触发读取下周总部的班是否排过，没排的话自动排班，判断period，departmentid
    set_hq_shift(config)
    
    logging.info('Set Shift finish')
    send_hr_email_benefit(config)
    logging.info('Send HR Benefit Email finish')
    send_hr_email_hire(config)
    logging.info('Send HR Hire Date Email finish')
    send_hr_email_mp(config)
    logging.info('Send HR Missing Punch Email finish')
    logging.info('Program finish')
