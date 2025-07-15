#!/usr/bin/env python3
import os, time, threading, json
from datetime import datetime, timezone
try: import redis
except ImportError: redis=None
from macotec_protocol import Connection

# 🧬 Settings ----------------------
MACH_HOST = "localhost"
MACH_CONN_PORT = 23200
MACH_NAME = "ActiveW"
PUBLISH_INTERVAL = 1.0 # seconds
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
    converted_obj = {'timestamp':'', "deviceData":[]}
    for key, val in obj.items():
        converted_obj["deviceData"].append({"Id": key, "val": val})
    return converted_obj

def initialize_fields(data_fields, mach_conn):
    try:
        data_fields.update( mach_conn.read_status() )
    except:
        print(f"{RED}{e}{END}")

    # Additional data
    try:
        data_fields.update( mach_conn.read(["@statistics",
                                            "prj-name",
                                            "step-data",
                                            "work-selectors",
                                            "glass-id",
                                            "glass-type",
                                            "h-glass",
                                            "cut-recipe"]) )
    except:
        print(f"{RED}{e}{END}")

def notifications_listener(mach_conn, data_fields, data_lock, stop_event):
    while not stop_event.is_set():
        try:
            incoming_msgs = mach_conn.receive_all()
            for msg in incoming_msgs:
                if (msg_txt:=msg.header.get("msg")):
                    if msg_txt=="news":
                        print(f"{CYAN}Status {msg}{END}")
                        with data_lock:
                            data_fields.update(msg.body)
                    elif msg_txt=="event":
                        print(f"{BLUE}Event {msg}{END}")
                        with data_lock:
                            if "event" in msg.body and msg.body["event"] not in data_fields["event"]:
                                data_fields["event"].append(msg.body["event"])
                            if "args" in msg.body:
                                data_fields.update(msg.body["args"])
                    else:
                        print(f"{YELLOW}Unhandled {msg}{END}")
                else:
                    print(f"{YELLOW}Strange {msg}{END}")
        except TimeoutError:
            pass
        except Exception as e:
            print(f"{MAGENTA}notifications_listener() {RED}{e}{END}")
        time.sleep(0.3)
    print(f"\n{GRAY}notifications_listener() exited{END}\n")

def cyclic_publisher(redis_client, data_fields, data_lock, stop_event):
    while not stop_event.is_set():
        try:
            time.sleep(PUBLISH_INTERVAL)
            with data_lock:
                published_data = convert_to_40factory(data_fields)
                published_data["timestamp"] = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace("+00:00","Z")
                published_str = json.dumps(published_data, separators=(",", ":"))
                data_fields["event"].clear()
            if redis_client:
                redis_client.publish(REDIS_CHANNEL, published_str)
            else:
                print(f"\n{GRAY}{published_str}{END}\n")
        except Exception as e:
            print(f"{MAGENTA}cyclic_publisher() {RED}{e}{END}")
    print(f"\n{GRAY}cyclic_publisher() exited{END}\n")

if __name__ == '__main__':
    data_fields = {
        "event": [],
        "cut-recipe": "",
        "generic-status": "",
        "glass-id": "",
        "glass-type": "",
        "h-glass": 0.0,
        "machine": "",
        "prj-name": "",
        "scheme": 0,
        "schemes-count": 0,
        "step-data": {},
        "user-buttons": "",
        "work-selectors": "",
        }
    mach_conn = Connection(MACH_HOST, MACH_CONN_PORT, MACH_NAME, os.path.basename(__file__))
    initialize_fields(data_fields, mach_conn)

    data_lock = threading.Lock()
    stop_event = threading.Event()
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB) if redis else None
    if not redis_client:
        print("! Redis client not available, install it with: > pip install redis")

    listener_thread = threading.Thread(target=notifications_listener, args=(mach_conn, data_fields, data_lock, stop_event))
    publisher_thread = threading.Thread(target=cyclic_publisher, args=(redis_client, data_fields, data_lock, stop_event))
    listener_thread.start()
    publisher_thread.start()

    mach_conn.subscribe_to_status_changes()

    # Detect abort (CTRL+C)
    while listener_thread.is_alive() and publisher_thread.is_alive():
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            print("\nShutting down")
            stop_event.set()
            publisher_thread.join()
            listener_thread.join()
