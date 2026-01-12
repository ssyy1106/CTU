import boto3
from botocore.exceptions import ClientError
from AWS import Report
import configparser
import logging
import os

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.ini")
_CONFIG_SECTION = "AWS"


def _load_email_config():
    config = configparser.ConfigParser()
    if not config.read(_CONFIG_PATH):
        raise FileNotFoundError(f"Missing config file: {_CONFIG_PATH}")
    if _CONFIG_SECTION not in config:
        raise KeyError(f"Missing section [{_CONFIG_SECTION}] in {_CONFIG_PATH}")
    section = config[_CONFIG_SECTION]
    sender = section.get("sender")
    aws_region = section.get("region")
    access_key = section.get("access_key_id")
    secret_key = section.get("secret_access_key")
    if not all([sender, aws_region, access_key, secret_key]):
        raise ValueError(f"Incomplete AWS email configuration in {_CONFIG_PATH}")
    return sender, aws_region, access_key, secret_key


def _get_ses_client():
    sender, aws_region, access_key, secret_key = _load_email_config()
    client = boto3.client(
        "ses",
        region_name=aws_region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return sender, client

def send_email_items(templateDIR: str, messages: list, recipient: str, fileName: str = "Btrust_alert", type: int = 1):
    try:
        sender, client = _get_ses_client()
        report = Report(messages, sender, client)
        if type == 1:
            report.send(templateDIR, recipient)
        elif type == 5:
            report.sendVisa(templateDIR, recipient, fileName)
        elif type == 3:
            report.send_840(templateDIR, recipient, fileName)
        elif type == 4:
            report.send_90(templateDIR, recipient, fileName)
        else:
            report.sendShift(recipient, fileName)
    except Exception as e:
        logging.info(f"send_email_items error: {e}")

def send_missing_punch_email(templateDIR: str, incidents: list, recipient: str,
                             fileName: str = "Missing_Punch", context: dict | None = None,
                             show_meta: bool = False, type: int = 1, attachments=None):
    try:
        sender, client = _get_ses_client()
        report = Report(incidents, sender, client)
        context = context or {}
        report.send_missing_punch(
            templateDIR,
            recipient,
            fileName=fileName,
            period_start=context.get('period_start'),
            period_end=context.get('period_end'),
            name=context.get('name'),
            store=context.get('store'),
            department=context.get('department'),
            manager_name=context.get('manager_name'),
            manager_email=context.get('manager_email'),
            action_url=context.get('action_url'),
            context=context,
            show_meta=show_meta,
            type = type,
            attachments=attachments,
        )
    except Exception as e:
        logging.info(f"send_missing_punch_email error: {e}")

def send_email(text: str, recipient: str):
    sender, client = _get_ses_client()
    RECIPIENT = recipient
    SUBJECT = "TESING"
    #text = text[:9]
    BODY_TEXT = text
    BODY_HTML = text
    CHARSET = "UTF-8"
    try:
        response = client.send_email(
            Destination = {
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message = {
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },

                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source = sender,
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print('Email sent.')


if __name__ == "__main__":
    send_email("test1234567890", "peng.wang@btrust.ca")
