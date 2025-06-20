#!/usr/bin/env python3
import socketserver, json
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

def convert_json_payload(original_json):
    """
    Convert:
    {'timestamp': '2025-06-05T08:46:11+02:00',
     'event': 'step-started',
     'args': {'prj-name': 'test-prj', 'scheme': 1, 'step': 2}}

    To:
    {'timestamp': '2025-06-05T06:46:11Z',
      'deviceData': [
        {'Id': 'event', 'val': 'step-started'},
        {'Id': 'prj-name', 'val': 'test-prj'},
        {'Id': 'scheme', 'val': 1},
        {'Id': 'step', 'val': 2}
      ]}
    """
    converted_json = {'timestamp':'', "deviceData":[]}
    if "timestamp" in original_json:
        # Convert timestamp to UTC
        converted_json["timestamp"] = datetime.fromisoformat(original_json["timestamp"]).astimezone(timezone.utc).isoformat(timespec='milliseconds').replace("+00:00","Z")

    if "event" in original_json:
        converted_json["deviceData"].append({"Id": "event", "val": original_json["event"]})

    #if "args" in original_json:
    #    converted_json["deviceData"].extend({"Id": key, "val": val} for key, val in original_json["args"].items())

    def csv_to_intarray(csv_indexes):
        try:
            return [int(x) for x in csv_indexes.split(",") if x.strip()]
        except ValueError:
            return csv_indexes

    if "args" in original_json:
        for key, val in original_json["args"].items():
            if key=="emg-list" and isinstance(val, str):
                converted_json["deviceData"].append({"Id": key, "val": csv_to_intarray(val)})
            else:
                converted_json["deviceData"].append({"Id": key, "val": val})
    return converted_json

def handle_json_payload(client_ip, client_port, json_data):
    print_incoming_payload(client_ip, client_port, json_data)
    if redis_client:
        try:
            msg = json.dumps(json_data, separators=(",", ":"))
            redis_client.publish(REDIS_CHANNEL, msg)
        except Exception as e:
            print(f"!! Failed to publish to Redis: {e}")

def print_incoming_payload(client_ip, client_port, incoming_data):
    print(f"\n[from {client_ip}:{client_port} at {datetime.now().time()}]\n{incoming_data}")

class JsonTcpHandler(socketserver.StreamRequestHandler):
    def handle(self):
        client_ip, client_port = self.client_address
        print("\n------------------------------------------")
        print(f"Connection from {client_ip}:{client_port}")

        while True:
            line = self.rfile.readline()
            if not line:
                print(f"\nClient {client_ip}:{client_port} disconnected")
                break
            try:
                incoming_str = line.decode("utf8").strip()
                incoming_json = json.loads(incoming_str)
                converted_json = convert_json_payload(incoming_json)
                handle_json_payload(client_ip, client_port, converted_json)
            except json.JSONDecodeError as e:
                print_incoming_payload(client_ip, client_port, incoming_str)
                print(f"! Invalid json at {line!r}\nError: {e}")

if __name__ == '__main__':
    with socketserver.ThreadingTCPServer((SINK_HOST, SINK_PORT), JsonTcpHandler) as server:
        print(f"TCP sink listening on {SINK_HOST}:{SINK_PORT} ...  (CTRL+C to exit)")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nServer is shutting down")
