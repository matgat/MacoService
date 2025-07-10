#!/usr/bin/env python3
import socketserver, json, re
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
# ---------------------------------------------------------

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

def convert_to_40factory(raw_data):
    """
    Convert:
    {'timestamp': '2025-06-05T08:46:11+02:00',
     'event': 'eventname',
     'args': {'arg1': 'str', 'arg2': 2}}

    To:
    {'timestamp': '2025-06-05T06:46:11Z',
      'deviceData': [
        {'Id': 'event', 'val': 'eventname'},
        {'Id': 'arg1', 'val': 'str'},
        {'Id': 'arg2', 'val': 2},
      ]}
    """
    conv_data = {'timestamp':'', "deviceData":[]}
    if "timestamp" in raw_data:
        # Convert timestamp to UTC
        conv_data["timestamp"] = datetime.fromisoformat(raw_data["timestamp"]).astimezone(timezone.utc).isoformat(timespec='milliseconds').replace("+00:00","Z")

    if "event" in raw_data:
        conv_data["deviceData"].append({"Id": "event", "val": raw_data["event"]})

    #if "args" in raw_data:
    #    conv_data["deviceData"].extend({"Id": key, "val": val} for key, val in raw_data["args"].items())

    if "args" in raw_data:
        for key, val in raw_data["args"].items():
            if key=="emg-list" and isinstance(val, str):
                conv_data["deviceData"].append({"Id": key, "val": csv_to_intarray(val)})
            elif key=="step-data" and isinstance(val, str):
                conv_data["deviceData"].append({"Id": key, "val": mac_to_object(val)}) # json.dumps()
            else:
                conv_data["deviceData"].append({"Id": key, "val": val})
    return conv_data


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
                print(f"\n[from {client_ip}:{client_port} at {datetime.now().time()}]\n{incoming_payload}")
                raw_data = json.loads(incoming_payload)
                converted_data = convert_to_40factory(raw_data)
                print(converted_data)
                if redis_client:
                    try:
                        redis_client.publish(REDIS_CHANNEL, json.dumps(converted_data, separators=(",", ":")))
                    except Exception as e:
                        print(f"!! Failed to publish to Redis: {e}")
            except json.JSONDecodeError as e:
                print(f"! Invalid json at {line!r}\nError: {e}")

if __name__ == '__main__':
    with socketserver.ThreadingTCPServer((SINK_HOST, SINK_PORT), JsonTcpHandler) as server:
        print(f"TCP sink listening on {SINK_HOST}:{SINK_PORT} ...  (CTRL+C to exit)")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nServer is shutting down")
