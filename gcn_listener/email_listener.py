# Module to download emails and check if they are Einstein Probe

import email
import imaplib
import numpy as np
import os
from time import sleep
from gcn_listener.actions import make_phone_call, send_message
from pathlib import Path

email_user = os.getenv("EMAIL_USER")
email_pass = os.getenv("EMAIL_PASS")
recipients = os.getenv("RECIPIENT_PHONE").split(",")

email_log_path = Path(__file__).parent / "data/email_ids.txt"

def listen_email(listen_from_email: str = "no-reply@gcn.nasa.gov"):
    """
    Function to download emails from Einstein Probe email account
    :return: None
    """
    # Login to email account
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(email_user, email_pass)
    mail.select("inbox")

    # Search for emails from ep_ta@bao.ac.cn
    result, data = mail.search(None,
                               f'(FROM "{listen_from_email}")')
    ids = data[0]
    id_list = ids.split()
    id_nums = [int(i) for i in id_list]
    # Load ids from file
    with open(email_log_path, "r") as f:
        email_ids = f.read().splitlines()
    email_ids = [int(i) for i in email_ids]

    # Check if email has been looked at
    new_ids = np.setdiff1d(id_nums, email_ids)

    if len(new_ids) == 0:
        print("No new emails from GCN")
        return

    # Get the emails that haven't been looked at yet
    for i in new_ids:
        result, data = mail.fetch(str(i), "(RFC822)")
        raw_email = data[0][1]
        raw_email_string = raw_email.decode("utf-8")
        email_message = email.message_from_string(raw_email_string)
        for part in email_message.walk():
            if part.get_content_type() == "text/plain":
                body = part.get_payload(decode=True)
                # print(body.decode("utf-8"))

                body_str = body.decode("utf-8")
                body_str_lines = np.array(body_str.split("\n"))

                from_line = ["FROM" in x for x in body_str_lines]

                if len(body_str_lines[from_line]) == 0:
                    continue
                from_text = body_str_lines[from_line][0]
                print(from_text)
                if "ep_ta@bao.ac.cn" in from_text:
                    print(f"Found Einstein Probe email with id {i}")
                    make_phone_call(call_recipients=recipients,
                                    message_text="New Einstein Probe alert")
                    send_message(message_recipients=recipients,
                                 message_text="New Einstein Probe alert")

    # Save ids to file
    with open(email_log_path, "a") as f:
        for i in new_ids:
            f.write(str(i) + "\n")


if __name__ == "__main__":
    while True:
        listen_email()
        sleep(30)
