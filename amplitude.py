import boto3
import hashlib
import hmac
import json
import os
import requests
import sys
import time

AMPLITUDE_API_KEY = os.environ["FXA_AMPLITUDE_API_KEY"]
HMAC_KEY = os.environ["FXA_AMPLITUDE_HMAC_KEY"]

# For crude pre-emptive rate-limit obedience.
MAX_EVENTS_PER_BATCH = 10
MAX_BATCHES_PER_SECOND = 100
MIN_BATCH_INTERVAL = 1.0 / MAX_BATCHES_PER_SECOND

def handle (message):
    # http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
    records = json.loads(message)["Records"]
    for record in records:
        if record["eventSource"] != "aws:s3":
            continue

        s3 = boto3.resource("s3", region_name=record["awsRegion"])
        s3_object = s3.Object(record["s3"]["bucket"]["name"], record["s3"]["object"]["key"])

        send(s3_object.get()["Body"].read().decode("utf-8"))

def process (events):
    batch = []

    for event_string in events.splitlines():
        event = json.loads(event_string)

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
        if len(batch) == MAX_EVENTS_PER_BATCH:
            send(batch)
            batch = []

    if len(batch) > 0:
        send(batch)

def send (batch):
    batch_interval = time.time() - send.batch_time
    if batch_interval < MIN_BATCH_INTERVAL:
        time.sleep(MIN_BATCH_INTERVAL - batch_interval)

    # https://amplitude.zendesk.com/hc/en-us/articles/204771828#request-format
    response = requests.post("https://api.amplitude.com/httpapi",
                             data={"api_key": AMPLITUDE_API_KEY, "event": json.dumps(batch)})

    if response.status_code >= 400:
        # For want of a better error-handling mechanism,
        # one failed request fails an entire dump from S3.
        raise RuntimeError("HTTP {} response")

    send.batch_time = time.time()

send.batch_time = 0

if __name__ == "__main__":
    argc = len(sys.argv)
    if argc == 1:
        events = sys.stdin.read()
    elif argc == 2:
        events = sys.argv[1]
    else:
        sys.exit("Usage: {} <events>".format(sys.argv[0]))

    process(events)

