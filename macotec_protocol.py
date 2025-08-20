#!/usr/bin/env python3
import socket, re, random

class Message:
    FRAME_START = b'\x01'
    FRAME_BODY  = b'\x02'
    FRAME_END   = b'\x03'

    def __init__(self, header, body=None):
        self.header = header
        self.body = body or {}

    def __str__(self):
        return f"{self.header}{self.body}"

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


class Connection:
    msg_id = 0

    def __init__(self, hostname: str, conn_port: int, mach_name: str, client_name: str):
        mach_port, self.key = Connection.authenticate(hostname, conn_port, mach_name, client_name)
        self.sck, self.mach_data = Connection.connect(hostname, mach_port, self.key, client_name)

    def __del__(self):
        self.disconnect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    @staticmethod
    def decrypt(payload:bytes, key:int) -> bytes:
        return bytes([b ^ key for b in payload])

    @staticmethod
    def encrypt(payload:bytes, key:int) -> bytes:
        return bytes([b ^ key for b in payload])

    @staticmethod
    def send_payload(payload:bytes, sck:socket.socket, key:int) -> None:
        #print(f"Send: {payload}")
        if key is not None: payload = Connection.encrypt(payload, key)
        sck.sendall(payload)

    @staticmethod
    def receive_payload(sck:socket.socket, key:int) -> bytes:
        payload = sck.recv(4096)
        if not payload:
            raise ConnectionAbortedError("Socket closed by MacoLayer")
        if key is not None: payload = Connection.decrypt(payload, key)
        #print(f"Recv: {payload}")
        return payload

    @staticmethod
    def send_msg(msg:Message, sck:socket.socket, key:int) -> None:
        payload = msg.build_payload()
        Connection.send_payload(payload, sck, key)

    @staticmethod
    def receive_one(sck:socket.socket, key:int) -> Message:
        payload = Connection.receive_payload(sck, key)
        messages = Message.parse_payload(payload)
        if( not messages ):
            raise RuntimeError(f"No valid message received: {payload}")
        elif( len(messages)>1 ):
            raise RuntimeError(f"Multiple messages: {payload}")
        return messages[0]

    @staticmethod
    def authenticate(host:str, port:int, mach_name:str, client_name:str) -> tuple[int, int]:
        def create_auth_key(pub_key: int) -> int:
            return random.randint(1, 254)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sck:
            sck.settimeout(0.5)
            sck.connect((host, port))
            greet_msg = Connection.receive_one(sck,None)
            available_machines = greet_msg.body['machines'] if greet_msg.body['machines'] else []
            if not mach_name in available_machines:
                raise RuntimeError(f"Machine {mach_name} is not available between {available_machines}")
            pub_key = create_auth_key(greet_msg.body['auth-key'])
            Connection.send_msg(Message(header={"id":1, "rep-to":greet_msg.header["id"], "msg":"connect"}, body={"sender":f'"{client_name}"', "machine":f'"{mach_name}"', "auth-lvl":0, "auth-key":pub_key}), sck, None)
            reply = Connection.receive_one(sck,None)
            if reply.is_error():
                raise RuntimeError(f"Couldn't authenticate for {mach_name} ({reply.header.get('msg')})")
            return int(reply.body["port"]), pub_key

    @staticmethod
    def connect(host:str, port:int, key:int, client_name:str) -> tuple[socket.socket, dict]:
        sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sck.settimeout(0.5)
        sck.connect((host, port))
        Connection.send_msg(Message(header={"id":1}, body={"sender":f'"{client_name}"'}), sck, key)
        reply = Connection.receive_one(sck, key)
        if reply.is_error():
            raise RuntimeError(f"Couldn't connect to {mach_name} ({reply.header.get('msg')})")
        mach_data = reply.body
        return sck, mach_data

    def disconnect(self) -> None:
        try:
            self.sck.close()
        except Exception:
            pass

    def receive_all(self) -> list["Message"]:
        payload = Connection.receive_payload(self.sck, self.key)
        return Message.parse_payload(payload)

    def read(self, fields:list[str]) -> dict:
        self.msg_id += 1
        Connection.send_msg(Message(header={"id":self.msg_id}, body={field: None for field in fields}), self.sck, self.key)
        reply = Connection.receive_one(self.sck, self.key)
        if reply.is_error():
            raise RuntimeError(reply.header.get("msg"))
        return reply.body

    def write(self, fields:dict) -> None:
        self.msg_id += 1
        Connection.send_msg(Message(header={"id":self.msg_id}, body=fields), self.sck, self.key)
        reply = Connection.receive_one(self.sck, self.key)
        if reply.is_error():
            raise RuntimeError(reply.header.get("msg"))

    def notify(self, fields:dict) -> None:
        Connection.send_msg(Message(header={"msg":"notify"}, body=fields), self.sck, self.key)

    def read_status(self) -> dict:
        return self.read(["$status"])

    def subscribe_to_status_changes(self) -> None:
        self.msg_id += 1
        Connection.send_msg(Message(header={"id":self.msg_id}, body={"$subscribed":"$status"}), self.sck, self.key)
        reply = Connection.receive_one(self.sck, self.key)
        if reply.is_error():
            raise RuntimeError(f"Cannot subscribe to status changes: {reply.header.get('msg')}")
        if not reply.body:
            raise RuntimeError("Status change subscription not supported")
        return reply.body

    def ping(self) -> str:
        Connection.send_msg(Message(header={"id":self.msg_id, msg:"ping"}), self.sck, self.key)
        reply = Connection.receive_one(self.sck, self.key)
        return reply.header["msg"]
