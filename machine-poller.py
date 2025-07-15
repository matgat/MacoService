#!/usr/bin/env python3
import os, time
from datetime import datetime, timezone
try: import redis
except ImportError: redis=None
from macotec_protocol import Connection

# 🧬 Settings ----------------------
MACH_HOST = "localhost"
MACH_CONN_PORT = 23200
MACH_NAME = "ActiveW"
POLL_INTERVAL = 1.0 # seconds
#----------------------------------
REDIS_HOST    = "localhost"
REDIS_PORT    = 6379
REDIS_DB      = 0
REDIS_CHANNEL = "machine_events"
#----------------------------------
GRAY    = '\033[90m'
RED     = '\033[91m'
GREEN   = '\033[92m'
YELLOW  = '\033[93m'
BLUE    = '\033[94m'
MAGENTA = '\033[95m'
CYAN    = '\033[96m'
END     = '\033[0m'
#----------------------------------

def convert_to_40factory(obj:dict) -> dict:
    conv_obj = {'timestamp':'', "deviceData":[]}
    for key, val in obj.items():
        conv_obj["deviceData"].append({"Id": key, "val": val})
    return conv_obj

def publish_status(raw_status, redis_client):
    if redis_client:
        converted_status = convert_to_40factory(raw_status)
        converted_status["timestamp"] = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace("+00:00","Z")
        print(f"\n{GRAY}{converted_status}{END}\n")
        try:
            redis_client.publish(REDIS_CHANNEL, json.dumps(converted_status, separators=(",", ":")))
        except Exception as e:
            print(f"{RED}!! Failed to publish to Redis: {e}{END}")
    else:
        print(f"\n{GRAY}{raw_status}{END}\n")

def poll_status(mach_conn, redis_client):
    status = {}
    msg_id = 1
    while True:
        try:
            status.update( mach_conn.read_status() )
        except:
            print(f"{RED}{e}{END}")

        # Poll additional data
        try:
            status.update( mach_conn.read( ["@statistics",
                                            "prj-name",
                                            "step-data",
                                            "work-selectors",
                                            "glass-id",
                                            "glass-type",
                                            "h-glass",
                                            "cut-recipe"] ) )
        except:
            print(f"{RED}{e}{END}")

        publish_status(status, redis_client)
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB) if redis else None
    if not redis_client:
        print("! Redis client not available, install it with: > pip install redis")
    mach_conn = Connection(MACH_HOST, MACH_CONN_PORT, MACH_NAME, os.path.basename(__file__))
    poll_status(mach_conn, redis_client)
