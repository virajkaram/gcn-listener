Listener for LVC GCN events

## Installation

```pip install -e .```

## Usage

First, you need to configure a few environment variables:
```
export RECIPIENT_PHONE=<your phone number>
export TWILIO_ACCOUNT_SID=<twilio_sid>
export TWILIO_AUTH_TOKEN=<twilio_token>
export TWILIO_PHONE=<twilio_phone>
export RECIPIENT_EMAIL=<your-email-id>
export KAFKA_CLIENT_ID=<kafka-client-id>
export KAFKA_CLIENT_SECRET=<kafka-client-pwd>
export WATCHDOG_EMAIL_PASSWORD=<watchdog-email-pwd>
export WATCHDOG_EMAIL=<watchdog-email>
```

Then, you can run the listener as follows:
    ```python -m gcn_listener```

You can set several options using the command line:
```python -m gcn_listener -action email sms call``` 
to get both phone and email notifications

You can view all available options using:
```python -m gcn_listener --help```
