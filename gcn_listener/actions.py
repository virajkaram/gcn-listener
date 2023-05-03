import os
import smtplib
import ssl
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional
from pathlib import Path
import getpass
import gzip
import logging
from gcn_listener.gcn_utils import get_dateobs, get_notice_type, get_properties, \
    notice_types_dict
from astropy.time import Time
import numpy as np
from twilio.rest import Client


logger = logging.getLogger(__name__)
GMAIL_PORT = 465  # For SSL


def send_voevent_email(voevent,
                       email_recipients: str | list[str]):
    dateobs = get_dateobs(voevent)
    date_isot = Time(dateobs).isot
    properties = get_properties(voevent)
    notice_type = notice_types_dict[get_notice_type(voevent)]
    email_subject = f"GCN {notice_type} {date_isot}"
    email_text = f"GCN {notice_type} {date_isot}"
    email_text += f"\nProperties: {properties}"
    logger.info(f"Sending email to {email_recipients}"
                f" with subject {email_subject}"
                f" and text {email_text}")
    send_gmail(email_recipients, email_subject, email_text)


def send_voevent_message(voevent,
                         message_recipients: str | list[str]):
    dateobs = get_dateobs(voevent)
    date_isot = Time(dateobs).isot
    properties = get_properties(voevent)
    notice_type = notice_types_dict[get_notice_type(voevent)]
    message_text = f"GCN {notice_type} {date_isot}"
    message_text += f"\nProperties: {properties}"
    logger.info(f"Sending message to {message_recipients}"
                f" with text {message_text}")
    send_message(message_recipients, message_text)


def make_voevent_phone_call(voevent,
                            phone_recipients: str | list[str]):
    dateobs = get_dateobs(voevent)
    date_isot = Time(dateobs).isot
    notice_type = notice_types_dict[get_notice_type(voevent)]
    phone_text = f"New GCN alert with notice type {notice_type} {date_isot}. "
    phone_text += "Check your message for more information"
    logger.info(f"Making phone call to {phone_recipients}"
                f" with text {phone_text}")
    make_phone_call(phone_recipients, phone_text)


def send_gmail(
    email_recipients: str | list[str],
    email_subject: str,
    email_text: str,
    email_sender: str = os.getenv("WATCHDOG_EMAIL"),
    email_password: str = os.getenv("WATCHDOG_EMAIL_PASSWORD"),
    attachments: Optional[str | list[str]] = None,
    auto_compress: bool = True,
):
    """
    Function to send an email to a list of recipients from a gmail account.

    :param email_recipients: recipients for email
    :param email_subject: subject for the email
    :param email_text: Text to send
    :param email_sender: Gmail to send from
    :param email_password: Password for sender gmail account
    :param attachments: Any files to attach
    :param auto_compress: Boolean to compress large attachments before sending
    :return:
    """
    # pylint: disable=too-many-arguments

    # Create a text/plain message
    msg = MIMEMultipart()
    msg.attach(MIMEText(email_text))

    if not isinstance(email_recipients, list):
        email_recipients = [email_recipients]

    msg["Subject"] = email_subject
    msg["From"] = email_sender
    msg["To"] = ", ".join(email_recipients)

    if attachments is None:
        attachments = []

    if not isinstance(attachments, list):
        attachments = [attachments]

    for file_path in attachments:
        if os.path.exists(file_path):
            base_name = os.path.basename(file_path)

            if not isinstance(file_path, Path):
                file_path = Path(file_path)

            with open(file_path, "rb") as attachment:
                if np.logical_and(
                    auto_compress, file_path.stat().st_size > (1024 * 1024)
                ):
                    data = gzip.compress(attachment.read())
                    base_name += ".gzip"
                else:
                    data = attachment.read()

                part = MIMEApplication(data, Name=base_name)

            # After the file is closed
            part["Content-Disposition"] = f"attachment; filename={base_name}"
            msg.attach(part)

        else:
            logger.warning(f"Attachment {file_path} not found, skipping.")

    logger.info(f"Sending email to {email_recipients}")

    if email_password is None:
        email_password = getpass.getpass()

    # Create a secure SSL context
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL("smtp.gmail.com", GMAIL_PORT, context=context) as server:
        server.login(email_sender, email_password)
        server.send_message(msg)


def send_message(
    message_recipients: str | list[str],
    message_text: str,
    twilio_account_sid: str = os.getenv("TWILIO_ACCOUNT_SID", None),
    twilio_auth_token: str = os.getenv("TWILIO_AUTH_TOKEN", None),
    twilio_phone_number: str = os.getenv("TWILIO_PHONE", None),
):
    """
    Function to send a text message to a list of recipients from a twilio account.

    :param message_recipients: recipients for message
    :param message_text: Text to send
    :param twilio_account_sid: Twilio account SID
    :param twilio_auth_token: Twilio auth token
    :param twilio_phone_number: Twilio phone number
    :return:
    """
    # pylint: disable=too-many-arguments

    if not isinstance(message_recipients, list):
        message_recipients = [message_recipients]

    if twilio_account_sid is None:
        twilio_account_sid = getpass.getpass(prompt="Twilio account SID: ")

    if twilio_auth_token is None:
        twilio_auth_token = getpass.getpass(prompt="Twilio auth token: ")

    if twilio_phone_number is None:
        twilio_phone_number = getpass.getpass(prompt="Twilio phone number: ")

    client = Client(twilio_account_sid, twilio_auth_token)

    for recipient in message_recipients:
        logger.info(f"Sending message to {recipient}")
        client.messages.create(
            body=message_text, from_=twilio_phone_number, to=recipient
        )


def make_phone_call(
        call_recipients: str | list[str],
        message_text: str,
        twilio_account_sid: str = os.getenv("TWILIO_ACCOUNT_SID", None),
        twilio_auth_token: str = os.getenv("TWILIO_AUTH_TOKEN", None),
        twilio_phone_number: str = os.getenv("TWILIO_PHONE", None),
):
    if not isinstance(call_recipients, list):
        call_recipients = [call_recipients]

    if twilio_account_sid is None:
        twilio_account_sid = getpass.getpass(prompt="Twilio account SID: ")

    if twilio_auth_token is None:
        twilio_auth_token = getpass.getpass(prompt="Twilio auth token: ")

    if twilio_phone_number is None:
        twilio_phone_number = getpass.getpass(prompt="Twilio phone number: ")

    client = Client(twilio_account_sid, twilio_auth_token)

    for recipient in call_recipients:
        logger.info(f"Calling {recipient}")
        client.calls.create(
            twiml=f'<Response><Say>{message_text}</Say></Response>',
            from_=twilio_phone_number,
            to=recipient
        )
