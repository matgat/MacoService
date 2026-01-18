#!/usr/bin/env python3
import os
from time import sleep
from threading import Thread, Lock, Event
from macotec_protocol import Connection

# 🧬 Settings ----------------------
MACH_HOST = "localhost"
MACH_CONN_PORT = 23200
MACH_NAME = "StarCut"
CLIENT_NAME = os.path.basename(__file__)
CONNCHECK_PERIOD = 60.0 # [s]
RESTART_TIME = 5.0 # [s]
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
# User libraries
import json
# Example:
#from datetime import datetime, timezone # for timestamps
#try: import redis
#except ImportError: redis=None
#----------------------------------
#REDIS_HOST    = "localhost"
#REDIS_PORT    = 6379
#REDIS_DB      = 0
#REDIS_CHANNEL = "machine_events"
#redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB) if redis else None
#if not redis_client:
#    print(f"{GRAY}! Redis client not available, install it with:{END} > pip install redis")


#----------------------------------------------------------------------------
def custom_convert(data_fields: dict) -> dict:
    #converted = data_fields
    # Filter position fields
    converted = {k: v for k, v in data_fields.items() if not k.endswith("-pos")}
    #for key, val in data_fields.items():
    #    if key == "name":
    #        # Intercepted a particular key...
    #    else:
    #        if isinstance(val, dict):
    #            # Serialize dict to JSON string
    #            converted['mykey'].append({'Id': key, 'val': json.dumps(val)})
    #        else:
    #            converted["mykey"].append(val)
    return converted


#----------------------------------------------------------------------------
def publish_data(new_fields: dict) -> None:
    published_data = custom_convert(new_fields)
    # May add a custom field to data before sending them
    #published_data["timestamp"] = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace("+00:00","Z")
    published_str = json.dumps(published_data, separators=(",", ":"))
    if len(published_str)>2:
        # Do something with the new data
        print(f"\n{GRAY}{published_str}{END}\n")
        # Example:
        #if redis_client:
        #    redis_client.publish(REDIS_CHANNEL, published_str)


#----------------------------------------------------------------------------
def at_first_connection(mach_conn: Connection) -> None:
    initial_fields = mach_conn.subscribe_to_status_changes()
    # Also additional fields?
    #initial_fields.update( mach_conn.read(["@statistics",
    #                                       "machine",
    #                                       "prj-name",
    #                                       "work-selectors",
    #                                       "glass-id",
    #                                       "glass-type",
    #                                       "h-glass",
    #                                       "cut-recipe"]) )
    publish_data(initial_fields)


#----------------------------------------------------------------------------
def notifications_listener(mach_conn: Connection, data_lock: Lock, stop_event: Event) -> None:
    print(f"{MAGENTA}notifications_listener() {GRAY}started{END}")
    #mach_refresh_period = float(T) if (T:=mach_conn.mach_data.get("refresh-period")) else 0.3
    mach_conn.sck.settimeout(CONNCHECK_PERIOD)
    while not stop_event.is_set():
        try:
            incoming_msgs = mach_conn.receive_all()
            for msg in incoming_msgs:
                if (msg_txt:=msg.header.get("msg")):
                    if msg_txt=="news":
                        #print(f"{CYAN}Status {msg}{END}")
                        with data_lock:
                            publish_data(msg.body)
                    elif msg_txt=="event":
                        #print(f"{BLUE}Event {msg}{END}")
                        with data_lock:
                            publish_data(msg.body)
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
            # May as well publish the read fields?
            #with data_lock:
            #    publish_data(status_fields)
        except OSError as e:
            print(f"{MAGENTA}notifications_listener() socket error {RED}{e}{END}")
            stop_event.set()
            break
        except Exception as e:
            print(f"{MAGENTA}notifications_listener() {RED}{e}{END}")
        #sleep(mach_refresh_period)
    mach_conn.disconnect()
    print(f"{MAGENTA}notifications_listener() {GRAY}exited{END}")


#----------------------------------------------------------------------------
if __name__ == '__main__':
    data_lock = Lock()
    stop_event = Event()
    while True:
        try:
            print("Initializing connection")
            mach_conn = Connection(MACH_HOST, MACH_CONN_PORT, MACH_NAME, CLIENT_NAME)
            at_first_connection(mach_conn)

            listener_thread = Thread(target=notifications_listener, args=(mach_conn, data_lock, stop_event))
            listener_thread.start()
            listener_thread.join()

            if stop_event.is_set():
                stop_event.clear()
                print(f"Restarting in {RESTART_TIME} seconds...")
                sleep(RESTART_TIME)

        except Exception as e:
            print(f"{MAGENTA}main: {RED}{e}{END}")
            #import traceback; traceback.print_exc()
            print("Exiting threads...")
            stop_event.set()
            if 'listener_thread' in vars(): listener_thread.join()
            stop_event.clear()
            print(f"Restarting in {RESTART_TIME} seconds...")
            sleep(RESTART_TIME)
