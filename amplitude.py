import hashlib
import hmac
import json
import os
import requests
import sys

API_KEY = os.environ["FXA_AMPLITUDE_API_KEY"]
HMAC_KEY = os.environ["FXA_AMPLITUDE_HMAC_KEY"]

def transform(line):
    try:
        event = json.loads(line)["Fields"]

        assert("time" in event)
        assert("user_id" in event or "device_id" in event)

        if "user_id" in event:
            user_id_hmac = hmac.new(HMAC_KEY, event["user_id"], hashlib.sha256)
            event["user_id"] = user_id_hmac.hexdigest()

        response = requests.post("https://api.amplitude.com/httpapi",
                                 data={"api_key": API_KEY, "event": json.dumps((event))})

        return response

    except:
        return None

if __name__ == "__main__":
    print transform(sys.argv[1])

