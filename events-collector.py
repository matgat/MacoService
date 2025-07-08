#!/usr/bin/env python3
import socketserver, json, re
import threading, time
from datetime import datetime, timezone
try: import redis
except ImportError: redis=None

# --- CONFIG ----------------------------------------------
SINK_HOST = "localhost"
SINK_PORT = 9999

REDIS_HOST    = "localhost"
REDIS_PORT    = 6379
REDIS_DB      = 0
REDIS_CHANNEL = "machine_events"
PUBLISH_INTERVAL = 5.0 # seconds
# ---------------------------------------------------------

current_state = {
    "cut-recipe": "",
    "emg-list": [],
    "generic-status": "",
    "glass-id": "",
    "glass-type": "",
    "h-glass": 0.0,
    "machine": "",
    "piece-taken": 0,
    "prj-name": "",
    "scheme": 0,
    "schemes-count": 0,
    "speed-override": 0,
    "step": 0,
    "step-data": {},
    "user-buttons": "",
    "work-selectors": "",
    # Contatori inizializzati da database
    #"blade-travel": float('nan'),
    #"sheets-done-count": None,
    #"cuts-done-count": None,
    #"lamp-total-time": float('nan'),
    #"lowe-travel": float('nan'),
    #"rinf-travel": float('nan'),
    #"rsup-travel": float('nan'),
    #"tinf-travel": float('nan'),
    #"tsup-travel": float('nan'),
}
event_list = ""
state_lock = threading.Lock()
stop_event = threading.Event()

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB) if redis else None
if not redis_client:
    print("! Redis client not available, install it with: > pip install redis")

def csv_to_intarray(csv_indexes):
    try:
        return [int(x) for x in csv_indexes.split(",") if x.strip()]
    except ValueError:
        return csv_indexes

def mac_to_object(bad_json_string):
    def parse_my_bad_json(s: str) -> str:
        parsed_object = {}
        pattern = re.compile(r"(\w+)\s*:\s*\{([^}]+)\}")
        for key, body in pattern.findall(s):
            sub_dict = {}
            items = re.split(r'[,\s]+', body.strip())
            for item in items:
                if ":" in item:
                    subkey,subval = item.split(":", 1)
                    try:
                        sub_dict[subkey] = int(subval)
                    except ValueError:
                        try:
                            sub_dict[subkey] = float(subval)
                        except ValueError:
                            sub_dict[subkey] = subval
                else:
                    sub_dict[item] = True
            parsed_object[key] = sub_dict
        return parsed_object
    return parse_my_bad_json(bad_json_string)

def convert_to_40factory(obj):
    conv_obj = {'timestamp':'', "deviceData":[]}
    for key, val in obj.items():
        if key=="emg-list" and isinstance(val, str):
            conv_obj["deviceData"].append({"Id": key, "val": csv_to_intarray(val)})
        elif key=="step-data" and isinstance(val, str):
            conv_obj["deviceData"].append({"Id": key, "val": mac_to_object(val)}) # json.dumps()
        else:
            conv_obj["deviceData"].append({"Id": key, "val": val})
    return conv_obj

def update_global_state(raw_event_notification):
    with state_lock:
        if "event" in raw_event_notification:
            global event_list
            event_list = f"{event_list}, {raw_event_notification["event"]}" if event_list else raw_event_notification["event"]
        if "args" in raw_event_notification:
            for key, val in raw_event_notification["args"].items():
                if key=="emg-list" and isinstance(val, str):
                    current_state[key] = csv_to_intarray(val)
                elif key=="step-data" and isinstance(val, str):
                    current_state[key] = mac_to_object(val)
                else:
                    current_state[key] = val

def cyclic_publisher():
    while not stop_event.is_set():
        time.sleep(PUBLISH_INTERVAL)
        with state_lock:
            snapshot = convert_to_40factory(current_state)
            global event_list
            snapshot["event"] = event_list
            event_list = ""
        snapshot["timestamp"] = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace("+00:00","Z")
        if redis_client:
            try:
                redis_client.publish(REDIS_CHANNEL, json.dumps(snapshot, separators=(",", ":")))
            except Exception as e:
                print(f"!! Failed to publish to Redis: {e}")
        else:
            print(f"\n{snapshot}", flush=True)


class JsonTcpHandler(socketserver.StreamRequestHandler):
    def handle(self):
        client_ip, client_port = self.client_address
        print("\n------------------------------------------")
        print(f"Connection from {client_ip}:{client_port}")

        while True:
            line = self.rfile.readline()
            if not line:
                print(f"\n[{client_ip}:{client_port} disconnected]")
                break
            try:
                incoming_payload = line.decode("utf8").strip()
                print(f"\n[from {client_ip}:{client_port} at {datetime.now().time()}]\n{incoming_payload}", flush=True)
                raw_data = json.loads(incoming_payload)
                update_global_state(raw_data)
            except json.JSONDecodeError as e:
                print(f"! Invalid json at {line!r}\nError: {e}")

if __name__ == '__main__':
    with socketserver.ThreadingTCPServer((SINK_HOST, SINK_PORT), JsonTcpHandler) as server:
        threading.Thread(target=cyclic_publisher, daemon=True).start()
        print(f"TCP sink listening on {SINK_HOST}:{SINK_PORT} ...  (CTRL+C to exit)")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nServer is shutting down")
            stop_event.set()
