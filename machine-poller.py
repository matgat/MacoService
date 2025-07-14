#!/usr/bin/env python3
import socket, re, os
import time
from datetime import datetime, timezone
try: import redis
except ImportError: redis=None

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

script_name = os.path.basename(__file__)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB) if redis else None
if not redis_client:
    print("! Redis client not available, install it with: > pip install redis")

class Message:
    FRAME_START = b'\x01'
    FRAME_BODY  = b'\x02'
    FRAME_END   = b'\x03'

    def __init__(self, header, body=None):
        self.header = header
        self.body = body or {}

    def is_reply(self) -> bool:
        return self.header.get("rep-to") is not None

    def is_error(self) -> bool:
        return self.header.get("ret")

    def build_payload(self) -> bytes:
        def dict_to_payload(obj: dict) -> bytes:
            fields = []
            for key, val in obj.items():
                if val is None:
                    fields.append(key)
                else:
                    if isinstance(val, str) and re.search(r"[\s,;=]", val) or key=="message": val_str = f'"{val}"'
                    elif isinstance(val, int) and key=="auth-key": val_str = f"0x{val:X}"
                    else: val_str = str(val)
                    fields.append(f"{key}={val_str}")
            return ";".join(fields).encode('utf-8')
        if not self.body:
            return self.FRAME_START + dict_to_payload(self.header) + self.FRAME_END
        return self.FRAME_START + dict_to_payload(self.header) + self.FRAME_BODY + dict_to_payload(self.body) + self.FRAME_END

    @classmethod
    def parse_payload(cls, payload: bytes) -> list["Message"]:
        def parse_single_message(payload: bytes) -> "Message":
            def extract_fields_from(payload: bytes) -> dict:
                out = {}
                payload = payload.decode('utf-8')
                for field in payload.split(';'):
                    if '=' in field:
                        key, val = field.split('=', 1)
                        out[key] = cls.str_to_value(val)
                    else:
                        out[field] = None
                return out

            if cls.FRAME_BODY in payload:
                header_payload, body_payload = payload.split(cls.FRAME_BODY, 1)
                header = extract_fields_from(header_payload)
                body = extract_fields_from(body_payload)
            else:
                header = extract_fields_from(payload)
                body = {}
            return cls(header, body)
        messages = []
        pattern = re.compile(b'%s([^%s]+)%s' % (re.escape(cls.FRAME_START), re.escape(cls.FRAME_END), re.escape(cls.FRAME_END)))
        for m in pattern.finditer(payload):
            messages.append(parse_single_message(m.group(1)))
        return messages

    @staticmethod
    def str_to_value(value_str: str):
        def parse_ext_json(s: str) -> str:
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
        value_str = value_str.strip()
        if value_str.startswith('"') and value_str.endswith('"'):
            # A string, won't try to convert to number
            value_str = value_str.strip('"')
            if '{' in value_str and '}' in value_str:
                return parse_ext_json(value_str)
            elif ',' in value_str:
                return [x_trimmed for x in value_str.split(',') if(x_trimmed := x.strip())]
            return value_str
        # Could be a list of integers or a number
        if ',' in value_str:
            items = [x_trimmed for x in value_str.split(',') if(x_trimmed := x.strip())]
            try:
                return [int(x) for x in items]
            except ValueError:
                return items
        elif value_str.startswith('0x'):
            try: return int(value_str, 16)
            except ValueError: return value_str
        try: return int(value_str)
        except ValueError: pass
        try: return float(value_str)
        except ValueError: pass
        return value_str

def get_timestamp_tz() -> str:
    return datetime.now().astimezone().isoformat(timespec='milliseconds')

def decrypt(payload: bytes, key: int):
    return bytes([b ^ key for b in payload])

def encrypt(payload: bytes, key: int):
    return bytes([b ^ key for b in payload])

def send_payload(sck: socket.socket, payload: bytes, key: int):
    #print(f"{GRAY}[{get_timestamp_tz()}]{END} Send: {GREEN}{payload}{END}")
    if key is not None: payload = encrypt(payload, key)
    sck.sendall(payload)

def receive_payload(sck: socket.socket, key: int) -> bytes:
    payload = sck.recv(4096)
    if key is not None: payload = decrypt(payload, key)
    #print(f"{GRAY}[{get_timestamp_tz()}]{END} Recv: {YELLOW}{payload}{END}")
    return payload

def receive(sck: socket.socket, key: int =None) -> Message:
    payload = receive_payload(sck, key)
    return Message.parse_payload(payload)

def receive_one(sck: socket.socket, key: int =None) -> Message:
    payload = receive_payload(sck, key)
    messages = Message.parse_payload(payload)
    if( not messages ): raise Exception(f"No valid message received: {payload}")
    elif( len(messages)>1 ): raise Exception(f"Multiple messages: {payload}")
    return messages[0]

def send(sck: socket.socket, msg: Message, key: int =None) -> None:
    payload = msg.build_payload()
    send_payload(sck, payload, key)

def authenticate(host: str, port: int, mach_name: str) -> tuple[int, int]:
    def create_auth_key(pub_key: int) -> int:
        return 0xAB
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sck:
        sck.connect((host, port))
        greet_msg = receive_one(sck)
        available_machines = greet_msg.body['machines'] if greet_msg.body['machines'] else []
        if not mach_name in available_machines:
            raise Exception(f"Machine {mach_name} is not available between {available_machines}")
        pub_key = create_auth_key(greet_msg.body['auth-key'])
        send(sck, Message(header={"id":1, "rep-to":greet_msg.header["id"], "msg":"connect"}, body={"sender":f'"{script_name}"', "machine":f'"{mach_name}"', "auth-lvl":0, "auth-key":pub_key}))
        reply = receive_one(sck)
        if reply.is_error():
            raise Exception(f"Couldn't authenticate for {mach_name} ({reply.header.get("msg")})")
        return int(reply.body["port"]), pub_key

def connect(host: str, port: int, key: int) -> tuple[socket.socket, dict]:
    sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sck.connect((host, port))
    send(sck, Message(header={"id":1}, body={"sender":f'"{script_name}"'}), key)
    reply = receive_one(sck, key)
    if reply.is_error():
        raise Exception(f"Couldn't connect to {mach_name} ({reply.header.get("msg")})")
    mach_data = reply.body
    return sck, mach_data

def convert_to_40factory(obj: dict):
    conv_obj = {'timestamp':'', "deviceData":[]}
    for key, val in obj.items():
        conv_obj["deviceData"].append({"Id": key, "val": val})
    return conv_obj

def publish_status(raw_status: dict):
    if redis_client:
        converted_status = convert_to_40factory(raw_status)
        print(f"\n{GRAY}{converted_status}{END}\n")
        try:
            redis_client.publish(REDIS_CHANNEL, json.dumps(converted_status, separators=(",", ":")))
        except Exception as e:
            print(f"{RED}!! Failed to publish to Redis: {e}{END}")
    else:
        print(f"\n{GRAY}{raw_status}{END}\n")

def poll_status(sck: socket.socket, key: int) -> None:
    msg_id = 1
    status = {}
    while True:
        msg_id += 1
        send(sck, Message(header={"id":msg_id}, body={"$status":None}), key)
        reply = receive_one(sck, key)
        if reply.is_error():
            print(f"{RED}{reply.header.get("msg")}{END}")
        else:
            status.update(reply.body)
        # Poll other data
        send(sck, Message(header={"id":msg_id}, body={"@statistics":None,
                                                      "prj-name":None,
                                                      "step-data":None,
                                                      "work-selectors":None,
                                                      #"user-buttons":None,
                                                      "glass-id":None,
                                                      "glass-type":None,
                                                      "h-glass":None,
                                                      "cut-recipe":None,}), key)
        reply = receive_one(sck, key)
        if reply.is_error():
            print(f"{RED}{reply.header.get("msg")}{END}")
        else:
            status.update(reply.body)
        publish_status(status)
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    mach_port, key = authenticate(MACH_HOST, MACH_CONN_PORT, MACH_NAME)
    sck, mach_data = connect(MACH_HOST, mach_port, key)
    poll_status(sck, key)
