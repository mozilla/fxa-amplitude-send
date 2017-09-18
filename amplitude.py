import boto.s3
import hashlib
import hmac
import json
import os
import requests
import sys

from threading import Timer

AMPLITUDE_API_KEY = os.environ["FXA_AMPLITUDE_API_KEY"]
AWS_ACCESS_KEY = os.environ["FXA_AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["FXA_AWS_SECRET_KEY"]
HMAC_KEY = os.environ["FXA_AMPLITUDE_HMAC_KEY"]

# For crude pre-emptive rate-limit obedience.
MAX_EVENTS_PER_BATCH = 10
MAX_BATCHES_PER_SECOND = 100
BATCH_INTERVAL = 1.0 / MAX_BATCHES_PER_SECOND

def handle (message):
    # http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
    records = json.loads(message)["Records"]
    for record in records:
        if record["eventSource"] != "aws:s3":
            continue

        aws_region = boto.s3.connect_to_region(record["awsRegion"])
        s3_bucket = aws_region.get_bucket(record["s3"]["bucket"]["name"])
        s3_object = s3_bucket.get_key(record["s3"]["object"]["key"])

        send(s3_object.get_contents_as_string().splitlines())

def send (events):
    batch = []

    while len(batch) < MAX_EVENTS_PER_BATCH and len(events) > 0:
        event = json.loads(events.pop(0))

        # https://amplitude.zendesk.com/hc/en-us/articles/204771828#keys-for-the-event-argument
        assert("device_id" in event or "user_id" in event)
        assert("event_type" in event)
        assert("time" in event)

        insert_id_hmac = hmac.new(HMAC_KEY, digestmod=hashlib.sha256)

        if "user_id" in event:
            user_id_hmac = hmac.new(HMAC_KEY, event["user_id"], hashlib.sha256)
            event["user_id"] = user_id_hmac.hexdigest()
            insert_id_hmac.update(event["user_id"])

        if "device_id" in event:
            insert_id_hmac.update(event["device_id"])

        if "session_id" in event:
            insert_id_hmac.update(str(event["session_id"]))

        insert_id_hmac.update(event["event_type"])
        insert_id_hmac.update(str(event["time"]))
        event["insert_id"] = insert_id_hmac.hexdigest()

        batch.append(event)

    # https://amplitude.zendesk.com/hc/en-us/articles/204771828#request-format
    response = requests.post("https://api.amplitude.com/httpapi",
                             data={"api_key": AMPLITUDE_API_KEY, "event": json.dumps(batch)})

    if response.status_code >= 400:
        # For want of a better error-handling mechanism,
        # one failed request fails an entire dump from S3.
        raise RuntimeError("HTTP {} response")

    if len(events) > 0:
        # Wait 10 milliseconds before starting the next batch
        Timer(BATCH_INTERVAL, send, [events]).start()

if  __name__ == "__main__":
    send(sys.argv[1].splitlines())

