import sendgrid
import os
from sendgrid.helpers.mail import Mail, Email, To, Content
from dotenv import load_dotenv
import traceback
from dataclasses import dataclass
import ssl

def format_exception_email(exception:Exception):
    traceback_list = traceback.format_tb(exception.__traceback__)
    tb_str_email = ""
    for i in traceback_list:
        str_tuple = str(i).split(",")
        tb_str_email += f"<br>{str_tuple[0]} in {str_tuple[1]}"
    return f"Exception:{exception.__repr__()}<br>Traceback:{tb_str_email}"


@dataclass
class EmailTriggerFields:
    Subject:str
    MiddlewareName:str
    JobName:str
    JobType:str
    TableName:str
    LogType:str
    LogDescription:str

@dataclass
class SendgridCredentials:
    secret_key:str
    from_email:str

def email_trigger(email_list_str:str,field_config:EmailTriggerFields,credentials:SendgridCredentials):
    sg = sendgrid.SendGridAPIClient(api_key=credentials.secret_key)
    from_email = Email(credentials.from_email)
    subject = field_config.Subject
    html_body = f"""
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Document</title>
        </head>
        <body>
            <h1 style="color:rgb(176, 54, 2);font-size:40px;font-family:Arial, Helvetica, sans-serif"><b>{field_config.LogType}</b></h1>
            <hr>
            <h2 style="color: black;font-size: 20px;font-family:Arial, Helvetica, sans-serif"><b>Middleware Name:</b></h2>
            <p style="color: black;font-size: 15px;font-family:Arial, Helvetica, sans-serif">{field_config.MiddlewareName}</p>
            <hr>
            <h2 style="color: black;font-size: 20px;font-family:Arial, Helvetica, sans-serif"><b>Job Name:</b></h2>
            <p style="color: black;font-size: 15px;font-family:Arial, Helvetica, sans-serif">{field_config.JobName}</p>
            <hr>
            <h2 style="color: black;font-size: 20px;font-family:Arial, Helvetica, sans-serif"><b>Job Type:</b></h2>
            <p style="color: black;font-size: 15px;font-family:Arial, Helvetica, sans-serif">{field_config.JobType}</p>
            <hr>
            <h2 style="color: black;font-size: 20px;font-family:Arial, Helvetica, sans-serif"><b>TableName:</b></h2>
            <p style="color: black;font-size: 15px;font-family:Arial, Helvetica, sans-serif">{field_config.TableName}</p>
            <hr>
            <h2 style="color: black;font-size: 20px;font-family:Arial, Helvetica, sans-serif"><b>Log Type:</b></h2>
            <p style="color: black;font-size: 15px;font-family:Arial, Helvetica, sans-serif">{field_config.LogType}</p>
            <hr>
            <h2 style="color: black;font-size: 20px;font-family:Arial, Helvetica, sans-serif"><b>Log Description:</b></h2>
            <p style="color: black;font-size: 15px;font-family:Arial, Helvetica, sans-serif">{field_config.LogDescription}</p>
            <hr>
        </body>
        </html>
        """
    ssl._create_default_https_context = ssl._create_unverified_context
    content = Content("text/html", html_body)
    for email in email_list_str.split(";"):
        to_email = To(email)
        mail = Mail(from_email, to_email, subject, content)
        mail_json = mail.get()
        response = sg.client.mail.send.post(request_body=mail_json)
        print(response.status_code)
        print(response.headers)
    return None