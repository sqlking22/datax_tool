#!/usr/bin/python3
# -*- coding: utf-8 -*-
import smtplib
from config import global_config
from email.mime.text import MIMEText


smtp_server = global_config.get_value("email", "smtp_server")
smtp_port = global_config.get_value("email", "smtp_port")
from_mail = global_config.get_value("email", "from_mail")
from_name = global_config.get_value("email", "user_name")
mail_pass = global_config.get_value("email", "password")
to_mail = global_config.get_value("email", "to_mail").split(',')
cc_mail = global_config.get_value("email", "cc_mail").split(',')


def generate_html():
    mail_html = """
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <font style="font-family:SimSun;font-size:14pt;">
        <br>告警时间:
        <br>告警机器:
        <br>告警内容:
        <br>
        <br></font>
    <br>
    <font style="font-family:SimSun;font-size:16pt;color:red;">
        提醒：本邮件为系统自动发送，请勿回复。
    </font>
    <br>
    """
    return mail_html


def send_html_email(subject, html_body):
    # 邮件内容设置
    message = MIMEText(html_body, 'html', 'utf-8')
    # 邮件标题设置
    message['Subject'] = subject
    # 发件人
    message['From'] = from_name
    # 收件人
    message['To'] = ', '.join(to_mail)
    # 抄送人
    message['CC'] = ', '.join(cc_mail)

    try:
        s = smtplib.SMTP()
        s.connect(smtp_server, smtp_port)
        # 登录
        s.login(from_mail, mail_pass)
        # 发送邮件
        s.sendmail(from_mail, (to_mail + cc_mail), message.as_string())
        print('send email success!')
        s.quit()
    except smtplib.SMTPException as e:
        print("Error: %s" % e)


def sendmail(subject, content):
    # 邮件内容设置
    message = MIMEText(content, 'plain', 'utf-8')
    # 邮件标题设置
    message['Subject'] = subject
    # 发件人
    message['From'] = from_name
    # 收件人
    message['To'] = ', '.join(to_mail)
    # 抄送人
    message['CC'] = ', '.join(cc_mail)

    try:
        s = smtplib.SMTP()
        s.connect(smtp_server, smtp_port)
        # 登录
        s.login(from_mail, mail_pass)
        # 发送邮件
        s.sendmail(from_mail, (to_mail + cc_mail), message.as_string())
        print('send email success!')
        s.quit()
    except smtplib.SMTPException as e:
        print("Error: %s" % e)


if __name__ == '__main__':
    # 调用方式
    mail_html_body = generate_html()
    send_html_email("test_subject", mail_html_body)
