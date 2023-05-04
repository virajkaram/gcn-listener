import numpy as np
from gcn_kafka import Consumer
from gcn_listener.gcn_utils import get_dateobs, get_properties, get_notice_type, \
    get_root_from_payload, inv_notice_types_dict, get_tags
from astropy.time import Time
import os
import gcn
from gcn_listener.actions import send_voevent_email, send_gmail, send_message, \
    make_phone_call, send_voevent_message, make_voevent_phone_call
import argparse
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger('gcn_listener')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

KAFKA_CLIENT_ID = os.getenv('KAFKA_CLIENT_ID', None)
KAFKA_CLIENT_SECRET = os.getenv('KAFKA_CLIENT_SECRET', None)

if KAFKA_CLIENT_ID is None:
    raise ValueError("KAFKA_CLIENT_ID not set")
if KAFKA_CLIENT_SECRET is None:
    raise ValueError("KAFKA_CLIENT_SECRET not set")

default_allowed_notice_type_list = [gcn.NoticeType.LVC_COUNTERPART,
                                    gcn.NoticeType.LVC_EARLY_WARNING,
                                    gcn.NoticeType.LVC_INITIAL,
                                    gcn.NoticeType.LVC_PRELIMINARY,
                                    gcn.NoticeType.LVC_RETRACTION,
                                    gcn.NoticeType.LVC_TEST,
                                    gcn.NoticeType.LVC_UPDATE, ]


def needs_action(voevent,
                 hasNS_thresh: float = None,
                 far_thresh_per_year: float = None,
                 allowed_notice_types: list = default_allowed_notice_type_list,
                 reject_tags: list = []):
    properties = get_properties(voevent)

    notice_type = get_notice_type(voevent)

    action_needed = True

    if hasNS_thresh is not None:
        action_needed = action_needed & (properties['HasNS'] > hasNS_thresh)

    if far_thresh_per_year is not None:
        action_needed = action_needed & (properties['FAR'] * 86400 * 365
                                         < far_thresh_per_year)

    tags = get_tags(voevent)
    logger.info(f"Event tags: {tags}")
    tags_intersection = list(set(tags).intersection(set(reject_tags)))
    action_needed = action_needed & (notice_type in allowed_notice_types) \
                    & (len(tags_intersection) == 0)
    return action_needed


def listen(hasNS_thresh: float = None,
           far_thresh_per_year: float = None,
           action: list = ['email'],
           allowed_notice_types: list = default_allowed_notice_type_list,
           email_recipients: str = os.getenv('RECIPIENT_EMAIL', None),
           phone_recipients: str = os.getenv('RECIPIENT_PHONE', None)
           ):
    # Connect as a consumer.
    # Warning: don't share the client secret with others.
    consumer = Consumer(client_id=KAFKA_CLIENT_ID,
                        client_secret=KAFKA_CLIENT_SECRET)
    # Subscribe to topics and receive alerts
    consumer.subscribe(['gcn.classic.voevent.LVC_COUNTERPART',
                        'gcn.classic.voevent.LVC_EARLY_WARNING',
                        'gcn.classic.voevent.LVC_INITIAL',
                        'gcn.classic.voevent.LVC_PRELIMINARY',
                        'gcn.classic.voevent.LVC_RETRACTION',
                        'gcn.classic.voevent.LVC_TEST',
                        'gcn.classic.voevent.LVC_UPDATE'])
    while True:
        for message in consumer.consume(timeout=1):
            value = message.value()
            if 'Subscribed topic' in str(value):
                continue
            if len(value) > 0:
                voevent = get_root_from_payload(value)
                dateobs = get_dateobs(voevent)

                logger.info(f"Received VOevent for {dateobs}")
                savedir = Path(f"~/Data/gcn_listener/voevents/")
                if not savedir.exists():
                    savedir.mkdir(parents=True)

                with open(f"~/Data/gcn_listener/voevents/{Time(dateobs).isot}",
                          'w') as f:
                    f.write(str(value))
                logger.info(f"Written VOevent to file - "
                            f"~/Data/gcn_listener/voevents/{Time(dateobs).isot}")

                action_needed = needs_action(voevent, hasNS_thresh=hasNS_thresh,
                                             far_thresh_per_year=far_thresh_per_year,
                                             allowed_notice_types=allowed_notice_types)

                if action_needed:
                    if 'email' in action:
                        if email_recipients is None:
                            err = "No email recipients provided"
                            raise ValueError(err)
                        try:
                            send_voevent_email(voevent,
                                               email_recipients=email_recipients)
                        except Exception as e:
                            logger.error(f"Failed to send email with error {e}")

                    if 'sms' in action:
                        if phone_recipients is None:
                            err = "No phone recipients provided"
                            raise ValueError(err)
                        try:
                            send_voevent_message(voevent,
                                                 message_recipients=phone_recipients)
                        except Exception as e:
                            logger.error(f"Failed to send message with error {e}")

                    if 'call' in action:
                        try:
                            make_voevent_phone_call(voevent,
                                                    phone_recipients=phone_recipients)
                        except Exception as e:
                            logger.error(f"Failed to make phone call with error {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-hasNS_thresh', default=None, type=float,
                        help='Threshold for HasNS')
    parser.add_argument('-FAR_thresh', default=None, type=float,
                        help='Threshold for FAR')
    parser.add_argument('-action', choices=['email', 'sms', 'call'], default=['email'],
                        help='Action to take', nargs="+")
    parser.add_argument('-notices', choices=['PRELIMINARY', 'INITIAL', 'RETRACTION',
                                             'UPDATE', 'TEST', 'EARLY_WARNING',
                                             'COUNTERPART', 'all'],
                        default=['PRELIMINARY', 'INITIAL', 'RETRACTION', 'UPDATE'],
                        help='Action to take', nargs="+")

    args = parser.parse_args()

    notices = args.notices
    if 'all' in notices:
        notices = ['PRELIMINARY', 'INITIAL', 'RETRACTION', 'UPDATE', 'TEST',
                   'EARLY_WARNING', 'COUNTERPART']

    allowed_notice_types = [inv_notice_types_dict[f"LVC_{notice}"]
                            for notice in notices]

    if 'email' in args.action:
        if os.getenv('RECIPIENT_EMAIL', None) is None:
            raise ValueError("No email recipients provided")
        if os.getenv('WATCHDOG_EMAIL', None) is None:
            raise ValueError("No email recipients provided")
        if os.getenv('WATCHDOG_EMAIL_PASSWORD', None) is None:
            raise ValueError("No email recipients provided")

        logger.info("Sending test email to recipients")
        send_gmail(email_recipients=os.getenv('RECIPIENT_EMAIL'),
                   email_subject="Started listening for GCN events",
                   email_text=f"Started listening for GCN events "
                              f" at {Time(datetime.utcnow()).isot}")

    if np.logical_or('sms' in args.action, 'call' in args.action):
        if os.getenv('RECIPIENT_PHONE', None) is None:
            raise ValueError("No phone recipients provided")
        if os.getenv('TWILIO_ACCOUNT_SID', None) is None:
            raise ValueError("No twilio account SID provided")
        if os.getenv('TWILIO_AUTH_TOKEN', None) is None:
            raise ValueError("No twilio auth token provided")
        if os.getenv('TWILIO_PHONE', None) is None:
            raise ValueError("No twilio phone number provided")

        if 'sms' in args.action:
            logger.info("Sending test SMS to recipients")
            send_message(message_recipients=os.getenv('RECIPIENT_PHONE'),
                         message_text="Started listening for GCN events"
                                      f" at {Time(datetime.utcnow()).isot}")

        if 'call' in args.action:
            logger.info("Making test phone call to recipients")
            make_phone_call(call_recipients=os.getenv('RECIPIENT_PHONE'),
                            message_text="Started listening for GCN events")

    listen(hasNS_thresh=args.hasNS_thresh,
           far_thresh_per_year=args.FAR_thresh,
           action=args.action,
           allowed_notice_types=allowed_notice_types
           )
