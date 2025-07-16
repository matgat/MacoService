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
CLIENT_NAME = os.path.basename(__file__)
PUBLISH_PERIOD = 1.0 # [s]
CONNCHECK_PERIOD = 10.0 # [s]
RESTART_TIME = 5.0 # [s]
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

#----------------------------------------------------------------------------
def convert_to_40factory(obj:dict) -> dict:
    converted_obj = {'timestamp':'', "deviceData":[]}
    for key, val in obj.items():
        converted_obj["deviceData"].append({"Id": key, "val": val})
    return converted_obj

#----------------------------------------------------------------------------
def notifications_listener(mach_conn, data_fields, data_lock, stop_event):
    print(f"{MAGENTA}notifications_listener() {GRAY}started{END}")
    #mach_refresh_period = float(T) if (T:=mach_conn.mach_data.get("refresh-period")) else 0.3
    mach_conn.sck.settimeout(CONNCHECK_PERIOD)
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
                            if "event" in msg.body:
                                event = msg.body.pop("event")
                                if event not in data_fields["event"]:
                                    if not isinstance(data_fields.get("event"), list):
                                        data_fields["event"] = []
                                    data_fields["event"].append(event)
                            data_fields.update(msg.body)
                    else:
                        print(f"{YELLOW}Unhandled {msg}{END}")
                else:
                    print(f"{YELLOW}Strange {msg}{END}")
        except TimeoutError:
            #print(f"{MAGENTA}notifications_listener() {YELLOW}checking connection{END}")
            try:
                status_fields = mach_conn.read_status()
            except Exception as e:
                print(f"{MAGENTA}notifications_listener() conn error {RED}{e}{END}")
                stop_event.set()
                break
            with data_lock:
                data_fields.update(status_fields)
        except OSError as e:
            print(f"{MAGENTA}notifications_listener() socket error {RED}{e}{END}")
            stop_event.set()
            break
        except Exception as e:
            print(f"{MAGENTA}notifications_listener() {RED}{e}{END}")
        #time.sleep(mach_refresh_period)
    mach_conn.disconnect()
    print(f"{MAGENTA}notifications_listener() {GRAY}exited{END}")

#----------------------------------------------------------------------------
def cyclic_publisher(redis_client, data_fields, data_lock, stop_event):
    print(f"{MAGENTA}cyclic_publisher() {GRAY}started{END}")
    while not stop_event.is_set():
        try:
            time.sleep(PUBLISH_PERIOD)
            with data_lock:
                published_data = convert_to_40factory(data_fields)
                published_data["timestamp"] = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace("+00:00","Z")
                published_str = json.dumps(published_data, separators=(",", ":"))
                data_fields["event"] = []
            if redis_client:
                redis_client.publish(REDIS_CHANNEL, published_str)
            else:
                print(f"\n{GRAY}{published_str}{END}\n")
        except Exception as e:
            print(f"{MAGENTA}cyclic_publisher() {RED}{e}{END}")
    print(f"{MAGENTA}cyclic_publisher() {GRAY}exited{END}")

#----------------------------------------------------------------------------
if __name__ == '__main__':
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB) if redis else None
    if not redis_client:
        print(f"{GRAY}! Redis client not available, install it with:{END} > pip install redis")

    data_lock = threading.Lock()
    stop_event = threading.Event()
    while True:
        try:
            print("Initializing connection")
            data_fields = {
                "event": [],
                "generic-status": "UNKN",
                "cut-recipe": "",
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

            mach_conn = Connection(MACH_HOST, MACH_CONN_PORT, MACH_NAME, CLIENT_NAME)
            data_fields.update( mach_conn.read_status() )
            data_fields.update( mach_conn.read(["@statistics",
                                                "prj-name",
                                                "step-data",
                                                "work-selectors",
                                                "glass-id",
                                                "glass-type",
                                                "h-glass",
                                                "cut-recipe"]) )

            listener_thread = threading.Thread(target=notifications_listener, args=(mach_conn, data_fields, data_lock, stop_event))
            publisher_thread = threading.Thread(target=cyclic_publisher, args=(redis_client, data_fields, data_lock, stop_event))
            listener_thread.start()
            publisher_thread.start()

            mach_conn.subscribe_to_status_changes()

            publisher_thread.join()
            listener_thread.join()

            if stop_event.is_set():
                stop_event.clear()
                print(f"Restarting in {RESTART_TIME} seconds...")
                time.sleep(RESTART_TIME)

        except Exception as e:
            print(f"{MAGENTA}main: {RED}{e}{END}")
            #import traceback; traceback.print_exc()
            print("Exiting threads...")
            stop_event.set()
            publisher_thread.join()
            listener_thread.join()
            stop_event.clear()
            print(f"Restarting in {RESTART_TIME} seconds...")
            time.sleep(RESTART_TIME)
