import csv
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
# from flask import render_template
import jinja2
import logging
from io import StringIO
import pathlib
from botocore.exceptions import ClientError
from jinja2 import Template, Environment, FileSystemLoader

logger = logging.getLogger(__name__)


class Report:
    """
    Encapsulates a report resource that gets work items from an
    Amazon Aurora Serverless database and uses Amazon SES to send emails about them.
    """

    def __init__(self, items, email_sender, ses_client):
        """
        :param storage: An object that manages moving data in and out of the underlying
                        database.
        :param email_sender: The email address from which the email report is sent.
        :param ses_client: A Boto3 Amazon SES client.
        """
        self.items = items
        self.email_sender = email_sender
        self.ses_client = ses_client

    # def _format_mime_message(self, recipient, text, html, attachment,  fileName = 'Btrust_alert', charset="utf-8",):
    #     """
    #     Formats the report as a MIME message. When the the email contains an attachment,
    #     it must be sent in MIME format.
    #     """
    #     msg = MIMEMultipart("mixed")
    #     msg["Subject"] = "Btrust alert"
    #     msg["From"] = self.email_sender
    #     msg["To"] = recipient
    #     msg_body = MIMEMultipart("alternative")

    #     textpart = MIMEText(text.encode(charset), "plain", charset)
    #     htmlpart = MIMEText(html.encode(charset), "html", charset)
    #     msg_body.attach(textpart)
    #     msg_body.attach(htmlpart)

    #     att = MIMEApplication(attachment.encode(charset))
    #     att.add_header("Content-Disposition", "attachment", filename = fileName + ".csv")
    #     msg.attach(msg_body)
    #     msg.attach(att)
    #     return msg
    
    def _format_mime_message(self, recipient, text, html, attachment, fileName='Btrust_alert', attachments=None, charset="utf-8"):
        """
        Formats the report as a MIME message. When the email contains an attachment,
        it must be sent in MIME format.
        """
        msg = MIMEMultipart("mixed")
        msg["Subject"] = "Btrust alert"
        msg["From"] = self.email_sender
        msg["To"] = recipient

        # --- 邮件正文 ---
        msg_body = MIMEMultipart("alternative")
        textpart = MIMEText(text, "plain", charset)
        htmlpart = MIMEText(html, "html", charset)
        msg_body.attach(textpart)
        msg_body.attach(htmlpart)

        msg.attach(msg_body)

        payloads = []
        if attachments:
            for att in attachments:
                if isinstance(att, dict):
                    content = att.get("content")
                    name = att.get("name")
                elif isinstance(att, (list, tuple)) and len(att) >= 2:
                    content, name = att[0], att[1]
                else:
                    content, name = att, None
                if not name:
                    name = fileName + ".csv"
                elif "." not in name:
                    name = name + ".csv"
                payloads.append((content, name))
        elif attachment is not None:
            payloads.append((attachment, fileName + ".csv"))

        # --- 附件部分 ---
        for content, name in payloads:
            if isinstance(content, str):
                # ✅ 加上 BOM 保证 Excel / 网页预览都不乱码
                content = "\ufeff" + content  
                att = MIMEApplication(content.encode(charset), Name=name)
            else:
                # 如果已经是 bytes
                att = MIMEApplication(content, Name=name)

            att.add_header("Content-Disposition", "attachment", filename=name)
            msg.attach(att)

        # 组装
        return msg

    @staticmethod
    def _render_csv(work_items, type = 1):
        """
        Renders work items to CSV format, with the field names as a header row.

        :param work_items: The work items to include in the CSV output.
        :return: Work items rendered to a string in CSV format.
        """
        with StringIO() as csv_buffer:
            if type == 2:
                writer = csv.DictWriter(
                    csv_buffer,
                    ["BtrustId", "WorkingHours", "RealName", "Store", "DepartmentName"],
                    extrasaction="ignore",
                )
            elif type == 3:
                writer = csv.DictWriter(
                    csv_buffer,
                    ["BtrustId", "HiringDate", "RealName", "Store", "DepartmentName"],
                    extrasaction="ignore",
                )
            else:
                writer = csv.DictWriter(
                    csv_buffer,
                    ["Date", "BtrustId", "RealName", "Store", "DepartmentName"],
                    extrasaction="ignore",
                )
            writer.writeheader()
            writer.writerows(work_items)
            csv_items = csv_buffer.getvalue()
        return csv_items

    def _render_csv_generic(self, work_items, fields):
        with StringIO() as csv_buffer:
            writer = csv.DictWriter(csv_buffer, fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(work_items)
            return csv_buffer.getvalue()

    def _send_with_template(self, templateDIR, email, template_file,
                             fileName="btrust_alert", subject="Btrust alert",
                             csv_type=None, csv_fields=None, attach_threshold=10,
                             always_attach=False, extra_context=None, attachments=None):
        try:
            work_items = self.items
            snap_time = datetime.now()
            # logger.info(f"Sending report of %s items to %s.", len(work_items), email)
            env = Environment(loader=FileSystemLoader(templateDIR))
            logo_path = None
            if templateDIR:
                try:
                    logo_path = pathlib.Path(templateDIR, 'logoHQ.png').resolve().as_uri()
                except Exception:
                    logo_path = None
            context = {
                "snap_time": snap_time,
                "work_items": work_items,
                "incidents": work_items,
                "item_count": len(work_items),
                "templateDIR": templateDIR,
                "logo_path": logo_path,
            }
            if extra_context and isinstance(extra_context, dict):
                context.update(extra_context)
            html_report = ""
            text_report = ""
            if template_file:
                template = env.get_template(template_file)
                html_report = template.render(**context)
                text_report = html_report

            csv_items = None
            if csv_type is not None:
                csv_items = self._render_csv(work_items, csv_type)
            elif csv_fields is not None:
                csv_items = self._render_csv_generic(work_items, csv_fields)

            attach_csv = False
            if csv_items is not None and not attachments:
                attach_csv = always_attach or (len(work_items) > attach_threshold)

            attach_payloads = []
            if attach_csv:
                attach_payloads.append({"content": csv_items, "name": fileName + ".csv"})
            if attachments:
                attach_payloads.extend(attachments)

            if attach_payloads:
                mime_msg = self._format_mime_message(
                    email, text_report, html_report, None, fileName, attachments=attach_payloads
                )
                self.ses_client.send_raw_email(
                    Source=self.email_sender,
                    Destinations=[email],
                    RawMessage={"Data": mime_msg.as_string()},
                )
            else:
                self.ses_client.send_email(
                    Source=self.email_sender,
                    Destination={"ToAddresses": [email]},
                    Message={
                        "Subject": {"Data": subject},
                        "Body": {
                            "Html": {"Data": html_report},
                            "Text": {"Data": text_report},
                        },
                    },
                )
        except ClientError as err:
            logger.exception(
                "Couldn't send email. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )

    @staticmethod
    def _render_csv_shift(work_items):
        with StringIO() as csv_buffer:
            writer = csv.DictWriter(
                csv_buffer,
                ["BtrustId", "RealName", "Store", "DepartmentName", "TotalHours", "MondayBegin", "MondayEnd", "MondayTotalHours",
                 "TuesdayBegin", "TuesdayEnd", "TuesdayTotalHours",
                 "WednesdayBegin", "WednesdayEnd", "WednesdayTotalHours",
                 "ThursdayBegin", "ThursdayEnd", "ThursdayTotalHours",
                 "FridayBegin", "FridayEnd", "FridayTotalHours",
                 "SaturdayBegin", "SaturdayEnd", "SaturdayTotalHours",
                 "SundayBegin", "SundayEnd", "SundayTotalHours"],
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(work_items)
            csv_items = csv_buffer.getvalue()
        return csv_items

    def send(self, templateDIR, email):
        try:
            work_items = self.items
            snap_time = datetime.now()
            logger.info(f"Sending report of %s items to %s.", len(work_items), email)
            # 创建一个 Environment 对象，指定模板文件的路径
            # env = Environment(loader=FileSystemLoader('template'))
            env = Environment(loader=FileSystemLoader(templateDIR))
            # 加载模板文件
            template = env.get_template('report.html')
            # 渲染模板，传入变量的值
            html_report = template.render(snap_time=snap_time, work_items = work_items, item_count=len(work_items))
            #html_report = "\n".join(messages)
            #text_report = "\n".join(messages)
            text_report = html_report
            csv_items = self._render_csv(work_items)
            if len(work_items) > 10:
                mime_msg = self._format_mime_message(
                    email, text_report, html_report, csv_items
                )
                response = self.ses_client.send_raw_email(
                    Source=self.email_sender,
                    Destinations=[email],
                    RawMessage={"Data": mime_msg.as_string()},
                )
            else:
                self.ses_client.send_email(
                    Source=self.email_sender,
                    Destination={"ToAddresses": [email]},
                    Message={
                        "Subject": {"Data": f"Btrust alert"},
                        "Body": {
                            "Html": {"Data": html_report},
                            "Text": {"Data": text_report},
                        },
                    },
                )
        except ClientError as err:
            logger.exception(
                "Couldn't send email. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )

    def sendVisa(self, templateDIR, email, fileName = "Working Visa"):
        try:
            work_items = self.items
            snap_time = datetime.now()
            logger.info(f"Sending report of %s items to %s.", len(work_items), email)
            # 创建一个 Environment 对象，指定模板文件的路径
            # env = Environment(loader=FileSystemLoader('template'))
            env = Environment(loader=FileSystemLoader(templateDIR))
            # 加载模板文件
            template = env.get_template('reportVisa.html')
            # 渲染模板，传入变量的值
            html_report = template.render(snap_time=snap_time, work_items = work_items, item_count=len(work_items))
            #html_report = "\n".join(messages)
            #text_report = "\n".join(messages)
            text_report = html_report
            csv_items = self._render_csv(work_items)
            if len(work_items) > 10:
                mime_msg = self._format_mime_message(
                    email, text_report, html_report, csv_items, fileName
                )
                response = self.ses_client.send_raw_email(
                    Source=self.email_sender,
                    Destinations=[email],
                    RawMessage={"Data": mime_msg.as_string()},
                )
            else:
                self.ses_client.send_email(
                    Source=self.email_sender,
                    Destination={"ToAddresses": [email]},
                    Message={
                        "Subject": {"Data": f"Btrust alert"},
                        "Body": {
                            "Html": {"Data": html_report},
                            "Text": {"Data": text_report},
                        },
                    },
                )
        except ClientError as err:
            logger.exception(
                "Couldn't send email. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )

    def send_840(self, templateDIR, email, fileName = "btrust_alert"):
        try:
            work_items = self.items
            snap_time = datetime.now()
            logger.info(f"Sending report of %s items to %s.", len(work_items), email)
            # 创建一个 Environment 对象，指定模板文件的路径
            env = Environment(loader=FileSystemLoader(templateDIR))
            # 加载模板文件
            template = env.get_template('report840.html')
            # 渲染模板，传入变量的值
            html_report = template.render(snap_time=snap_time, work_items = work_items, item_count=len(work_items))
            #html_report = "\n".join(messages)
            #text_report = "\n".join(messages)
            text_report = html_report
            csv_items = self._render_csv(work_items, 2)
            if len(work_items) > 10:
                mime_msg = self._format_mime_message(
                    email, text_report, html_report, csv_items, fileName
                )
                response = self.ses_client.send_raw_email(
                    Source=self.email_sender,
                    Destinations=[email],
                    RawMessage={"Data": mime_msg.as_string()},
                )
            else:
                self.ses_client.send_email(
                    Source=self.email_sender,
                    Destination={"ToAddresses": [email]},
                    Message={
                        "Subject": {"Data": f"Btrust alert"},
                        "Body": {
                            "Html": {"Data": html_report},
                            "Text": {"Data": text_report},
                        },
                    },
                )
        except ClientError as err:
            logger.exception(
                "Couldn't send email. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
    
    def send_90(self, templateDIR, email, filename = "btrust_alert"):
        try:
            work_items = self.items
            snap_time = datetime.now()
            logger.info(f"Sending report of %s items to %s.", len(work_items), email)
            # 创建一个 Environment 对象，指定模板文件的路径
            env = Environment(loader=FileSystemLoader(templateDIR))
            # 加载模板文件
            template = env.get_template('report90.html')
            # 渲染模板，传入变量的值
            html_report = template.render(snap_time=snap_time, work_items = work_items, item_count=len(work_items))
            #html_report = "\n".join(messages)
            #text_report = "\n".join(messages)
            text_report = html_report
            csv_items = self._render_csv(work_items, 3)
            if len(work_items) > 10:
                mime_msg = self._format_mime_message(
                    email, text_report, html_report, csv_items, filename
                )
                response = self.ses_client.send_raw_email(
                    Source=self.email_sender,
                    Destinations=[email],
                    RawMessage={"Data": mime_msg.as_string()},
                )
            else:
                self.ses_client.send_email(
                    Source=self.email_sender,
                    Destination={"ToAddresses": [email]},
                    Message={
                        "Subject": {"Data": f"Btrust alert"},
                        "Body": {
                            "Html": {"Data": html_report},
                            "Text": {"Data": text_report},
                        },
                    },
                )
        except ClientError as err:
            logger.exception(
                "Couldn't send email. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
    
    def sendShift(self, email, fileName):
        try:
            work_items = self.items
            snap_time = datetime.now()
            logger.info(f"Sending shifts report of %s items to %s.", len(work_items), email)

            html_report = ""
            text_report = html_report
            csv_items = self._render_csv_shift(work_items)
            mime_msg = self._format_mime_message(
                email, text_report, html_report, csv_items, fileName
            )
            response = self.ses_client.send_raw_email(
                Source=self.email_sender,
                Destinations=[email],
                RawMessage={"Data": mime_msg.as_string()},
            )
            
        except ClientError as err:
            logger.exception(
                "Couldn't send email. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )

    def send_missing_punch(self, templateDIR, email, fileName="Missing_Punch",
                           period_start=None, period_end=None, name=None,
                           store=None, department=None, manager_name=None,
                           manager_email=None, action_url=None, context=None,
                           subject="Missing Punch Reminder",
                           show_meta=False, type=1, attachments=None):
        extra_context = {
            "period_start": period_start,
            "period_end": period_end,
            "name": name,
            "store": store,
            "department": department,
            "manager_name": manager_name,
            "manager_email": manager_email,
            "action_url": action_url,
            "show_meta": show_meta,
        }
        # Merge caller-provided context so custom fields (e.g., employee_notices) flow into the template
        extra_context.update(context or {})
        template_file='missingpunch.html'
        if type == 2:
            template_file='missingpunch2.html'
        elif type == 3:
            template_file='missingpunch3.html'
        elif type == 4:
            template_file='missingpunch_manager.html'
        self._send_with_template(
            templateDIR,
            email,
            template_file=template_file,
            fileName=fileName,
            subject=subject,
            csv_fields=["Date", "BtrustID", "Store", "Department", "Note"],
            attach_threshold=5,
            always_attach=False,
            extra_context=extra_context,
            attachments=attachments,
        )
