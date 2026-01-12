from aip import AipOcr
import base64

# 替换成你自己的 APP_ID, API_KEY, SECRET_KEY
APP_ID = '6726013'
API_KEY = 'TZmSQZGlTHayWJjaJCVlEqxB'
SECRET_KEY = 'UQknzGnMGbUDRVpl2ghhcie5y8eWQvSx'

client = AipOcr(APP_ID, API_KEY, SECRET_KEY)

# 读取图片
def get_file_content(file_path):
    with open(file_path, 'rb') as f:
        return f.read()

image = get_file_content('C:\program-Peng\invoice1.jpg')

# 调用通用文字识别（高精度版，免费额度每天500次）
result = client.basicAccurate(image)

# 打印所有识别结果
for item in result['words_result']:
    print(item['words'])

# 如果要提取金额或编号，可以做简单关键词匹配
def extract_info(words_list):
    invoice_number = None
    amount = None
    for item in words_list:
        text = item['words']
        if '发票号码' in text or '票号' or 'Document Number' in text:
            invoice_number = text
        if '￥' in text or '金额' or 'Total' in text:
            amount = text
    return invoice_number, amount

invoice_number, amount = extract_info(result['words_result'])
print(f"发票号码: {invoice_number}")
print(f"金额: {amount}")
