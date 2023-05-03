from gcn_kafka import Consumer
from gcn_listener.gcn_utils import get_dateobs, get_properties, get_notice_type, \
    get_root_from_payload
from astropy.time import Time
import os
import gcn
from gcn_listener.actions import send_voevent_email, send_gmail
import argparse
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger('gcn_listener')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

KAFKA_CLIENT_ID = os.getenv('CLIENT_ID', None)
KAFKA_CLIENT_SECRET = os.getenv('CLIENT_SECRET', None)

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
                 allowed_notice_types: list = default_allowed_notice_type_list):
    properties = get_properties(voevent)

    notice_type = get_notice_type(voevent)

    action_needed = True

    if hasNS_thresh is not None:
        action_needed = action_needed & (properties['HasNS'] > hasNS_thresh)

    if far_thresh_per_year is not None:
        action_needed = action_needed & (properties['FAR'] * 86400 * 365
                                         < far_thresh_per_year)

    action_needed = action_needed & (notice_type in allowed_notice_types)
    return action_needed


def listen(hasNS_thresh: float = None,
           far_thresh_per_year: float = None,
           action: str = 'email',
           email_recipients: str = os.getenv('RECIPIENT_EMAIL', None)
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
            logger.info(value)
            if 'Subscribed topic' in str(value):
                continue
            if len(value) > 0:
                voevent = get_root_from_payload(value)
                dateobs = get_dateobs(voevent)

                savedir = Path(f"~/Data/gcn_listener/voevents/")
                if not savedir.exists():
                    savedir.mkdir(parents=True)

                with open(f"~/Data/gcn_listener/voevents/{Time(dateobs).isot}",
                          'w') as f:
                    f.write(str(value))
                logger.info(f"Written VOevent to file - "
                            f"~/Data/gcn_listener/voevents/{Time(dateobs).isot}")

                action_needed = needs_action(voevent, hasNS_thresh=hasNS_thresh,
                                             far_thresh_per_year=far_thresh_per_year)

                if action_needed:
                    if action == 'email':
                        if email_recipients is None:
                            err = "No email recipients provided"
                            raise ValueError(err)
                        try:
                            send_voevent_email(voevent, email_recipients=email_recipients)
                        except Exception as e:
                            logger.error(f"Failed to send email with error {e}")

                    if action == 'phone':
                        raise NotImplementedError


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-hasNS_thresh', default=None, type=float,
                        help='Threshold for HasNS')
    parser.add_argument('-FAR_thresh', default=None, type=float,
                        help='Threshold for FAR')
    parser.add_argument('-action', choices=['email', 'phone'], default='email',
                        help='Action to take')

    args = parser.parse_args()

    if args.action == 'email':
        if os.getenv('RECIPIENT_EMAIL', None) is None:
            raise ValueError("No email recipients provided")
        if os.getenv('WATCHDOG_EMAIL', None) is None:
            raise ValueError("No email recipients provided")
        if os.getenv('WATCHDOG_EMAIL_PASSWORD', None) is None:
            raise ValueError("No email recipients provided")

        logger.info("Sending test email to recipients")
        send_gmail(email_recipients= os.getenv('RECIPIENT_EMAIL'),
                   email_subject="Started listening for GCN events",
                   email_text=f"Started listening for GCN events "
                              f"at {Time(datetime.utcnow()).isot}")

    listen(hasNS_thresh=args.hasNS_thresh,
           far_thresh_per_year=args.FAR_thresh,
           )

