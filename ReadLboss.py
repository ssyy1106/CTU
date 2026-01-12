from PIL import Image, ImageOps, ImageDraw, ImageFont
from os import listdir
from os.path import isfile, join
import psycopg2
import logging
import pandas as pd
import csv
from enum import Enum

Reading_Line_Prefix = ["RIPDsdY", "RIsDd", "RIDATFf", "RIDAQTFf", "RIDATf", "RIDAFSf"]

Non_Items_Exact_Match = [
    "HdDteTNCSMV123s,",
    "HdDteTNCSMV123,",
]

Possible_Columns = [
    "R",
    "I",
    "P",
    "D",
    "s",
    "d",
    "Y",
    "S",
    "F",
    "U",
    "b",
    "W",
    "p",
    "A",
    "O",
    "i",
    "K",
    "k",
    "Q",
    "V",
    "v",
    "u",
    "T",
    "h",
    "f",
    "m",
    "t",
    "c",
    "B",
    "D",
    "o",
]

Columns_Dictionary = {
    "R": "Sales_line",
    "I": "Sales_type",
    "P": "UPC",
    "D": "Description",
    "s": "Sub_department",
    "d": "Department",
    "Y": "Total_discount",
    "S": "Scale_or_tax",
    "F": "Function",
    "U": "Uom",
    "b": "Promotion_code",
    "W": "Weight",
    "p": "Unit_price",
    "A": "Total_amount",
    "O": "Cost",
    "i": "Constant_price",
    "K": "Promo_package_qty",
    "k": "Promo_total_price",
    "Q": "Sales_qty",
    "V": "Void_code",
    "v": "Void_indication",
    "u": "Void_description",
    "T": "Tax_type_or_return",
    # "h" : "Not_sure_h",
    "f": "Tax_flag",
    # "m" : "Not_sure_m",
    # "t" : "Not_sure_t",
    # "c" : "Not_sure_c",
    # "B" : "Not_sure_B",
    # "D" : "Not_sure_D",
    "o": "Not_sure_o",
    "q": "Not-sure_q",
    "M": "Not_sure_M",  # Only Montreal has this code
}

department_dict = {
    1: "Store Coupon",
    2: "Donation",
    30: "Gift Card",
    100: "Service Counter",
    170: "Gardening",
    200: "Groceries",
    270: "Houseware",
    300: "Personal Care",
    400: "Kitchen",
    500: "Produce",
    700: "Frozen/Daily",
    770: "Bakery",
    800: "Meat",
    900: "Seafood",
}

sub_department_dict = {
    1: "Coupon",
    9: "SickKids Donation",
    30: "Gift Card",
    106: "Health",
    107: "Personal Care (ServiceCounter)",
    108: "Tobacco",
    109: "Phone Card",
    131: "On-Line Lottery Ticket",
    132: "Scratch Ticket",
    139: "Lottery Gift Card",
    171: "Flower",
    172: "Plant",
    201: "Beverages",
    202: "Instant Foods",
    203: "Snacks",
    204: "Seasoning & Pickles",
    205: "Pantry",
    206: "Health",
    207: "Festival/Others",
    209: "Alcohol",
    271: "Household",
    272: "Kitchenware",
    273: "Appliances",
    274: "Home Decoration",
    275: "Pet Supplies",
    301: "Cosmetic",
    302: "Skin Care",
    303: "Oral Care",
    304: "Body Care",
    305: "Personal Care",
    306: "Hair Care",
    307: "Baby Care",
    308: "Boutique",
    309: "Agorav (B1)",
    401: "BBQ",
    403: "Gourmet",
    404: "Terra Justeas",
    407: "Terra",
    408: "Sushi",
    500: "Veg",
    600: "Fruit",
    701: "Daily",
    730: "Frozen",
    772: "Bakery",
    801: "Counter Meat",
    802: "Pack Meat",
    803: "Frozen Meat",
    804: "Meat product",
    901: "Fresh/Live Seafood",
    902: "Frozen Seafood"
}

class TransactionKind(Enum):
    Normal = 1
    Cancel = 2
    PowerFail = 3
    Reversal = 4
    Other = 5
    Return = 6
    Unknown = 7

Item_Discount = "RIDAFSf"
Normal_Header_Code = "HdDteTNCSMV123"
Irregular_Header_Code = "HdDteTNCSMV123s"
Return_Item = "HdDteTNCSMV123l"
Irregular_Tail_Code = {"R": TransactionKind.Reversal, "P": TransactionKind.PowerFail, "V": TransactionKind.Cancel}

def log_and_save(level, message):
    print(message)

def get_transaction_kind(line: str):
    parts = line.split(",")
    parts[-1] = parts[-1].replace("\n", "")

    if parts[0] == Irregular_Header_Code:
        if parts[-1] in Irregular_Tail_Code:
            return Irregular_Tail_Code[parts[-1]]
        log_and_save('WARNING', f"Header {Irregular_Header_Code} contains Other command, the transaction number is {parts[5]} the Current Line is {line} the current parts is {parts}")
        return TransactionKind.Other
    elif parts[0] == Normal_Header_Code:
        return TransactionKind.Normal
    elif parts[0] == Return_Item:
        return TransactionKind.Return
    log_and_save('WARNING', f"Header {parts[0]} contains Unknown command, the transaction number is {parts[5]} the Current Line is {line} the current parts is {parts}")
    return TransactionKind.Unknown

def get_transaction_id(line: str):
    parts = line.split(",")
    if len(parts) > 6:
        return parts[5]
    return -1

def get_transaction_kind_id(first_line: str) -> tuple:
    kind = get_transaction_kind(first_line)
    transaction_id = get_transaction_id(first_line)
    return (kind, transaction_id)

def read_blocks(f) -> list[list]:
    res = []
    block = []
    for line in f:
        line = line.replace('\n', '')
        # 空行意味着一个transaction的结束
        if line == "":
            if block:
                res.append(block)
            block = []
        else:
            block.append(line)
    return res

def get_parts(parts) -> list:
    if len(parts[0]) != len(parts):
        newParts = [0] * len(parts[0])
        descriptionPos = float("inf")
        if len(parts[0]) > len(parts):
            log_and_save('WARNING', f"Length of the salesLine column is not correct, the current parts is {parts}")

        for i in range(len(parts[0])):
            curChar = parts[0][i]
            if curChar == "D":
                descriptionPos = i
                break
        left = 0

        right1 = len(parts) - 1
        right2 = len(newParts) - 1

        while left < descriptionPos:
            newParts[left] = parts[left]
            left += 1
        while right2 > descriptionPos:
            newParts[right2] = parts[right1]
            right2 -= 1
            right1 -= 1

        newParts[descriptionPos] = "".join(parts[left : (right1 + 1)])
        return newParts
    return parts

def parse_line(line, kind):
    parts = line.split(",")
    parts[-1] = parts[-1].replace("\n", "")
    dic = {}
    for prefix in Reading_Line_Prefix:
        if parts[0].startswith(prefix):
            parts = get_parts(parts)
            for i in range(len(parts[0])):
                curChar = parts[0][i]
                if curChar in Columns_Dictionary:
                    if kind == TransactionKind.Reversal and curChar == "A":
                        # "A" means the total amount of the current item
                        dic[Columns_Dictionary[curChar]] = -float(parts[i].strip("\n"))
                    else:
                        # reading all normal items
                        dic[Columns_Dictionary[curChar]] = parts[i]

                    if curChar == "s":
                        subdepartmentCode = int(parts[i])
                        subdepartmentDescription = (
                            subdepartmentCode
                            if subdepartmentCode not in sub_department_dict
                            else sub_department_dict[subdepartmentCode]
                        )
                        dic["Subdepartment_description"] = subdepartmentDescription

                    if curChar == "d":
                        departmentCode = int(parts[i])
                        departmentDescription = (
                            departmentCode
                            if departmentCode not in department_dict
                            else department_dict[departmentCode]
                        )
                        dic["Department_description"] = departmentDescription
            return dic
    return None

def parse_block(lines: list, source_file_path: str, dic):
    kind, transaction_id = get_transaction_kind_id(lines[0])
    if (kind != TransactionKind.Normal and kind != TransactionKind.Reversal and kind != TransactionKind.Return) or transaction_id == -1:
        #print(f"kind: {kind} lines: {lines} \n")
        return []
    data = []
    for i, line in enumerate(lines):
        if i == 0:
            continue
        flag = False
        for prefix in Reading_Line_Prefix:
            if line.startswith(prefix):
                flag = True
                curRow = parse_line(line, kind)
                if prefix == Item_Discount:
                    totalAmount = float(data[-1]["Total_amount"].strip("\n"))
                    totalDiscount = float(curRow["Total_amount"].strip("\n"))
                    data[-1]["Total_amount"] = totalAmount + totalDiscount
                    break
                data.append(curRow) if curRow else None
        if not flag:
            parts = line.split(',')
            if parts:
                dic[parts[0]] = True
                if parts[0] == 'MID':
                    if i < len(lines) - 1:
                        last_parts = lines[i - 1].split(',')
                        next_parts = lines[i + 1].split(',')
                        if '-' + last_parts[-1] != next_parts[-1]:
                            continue
                            #print(f"last: {lines[i - 1]} \n this line: {line} \n next: {lines[i + 1]}")
    return data

def deal_file(source_file_path: str):
    dic = {}
    data = []
    with open(source_file_path) as f:
        for block in read_blocks(f):
            data.extend(parse_block(block, source_file_path, dic))
    # for k, v in dic.items():
    #     print(k)
    df = pd.DataFrame(data)
    df["Total_amount"] = pd.to_numeric(df["Total_amount"].astype(str).str.strip("\n"))
    df.to_csv(r'C:\program-Peng\EJ20240712.csv', index=False, encoding='utf-8')
    
if __name__ == "__main__":
    files = deal_file(r'C:\program-Peng\EJ20240404.DAT')

