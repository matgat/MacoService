#!/usr/bin/env python3
import socket, re
import random # For authentication key
from pathlib import Path; import tempfile # For send_mac_file

class Message:
    FRAME_START = b'\x01'
    FRAME_BODY  = b'\x02'
    FRAME_END   = b'\x03'

    RE_FRAME = re.compile(b'%s([^%s]+)%s' % (re.escape(FRAME_START), re.escape(FRAME_END), re.escape(FRAME_END)))

    def __init__(self, header: dict[str, object], body: dict[str, object]=None) -> None:
        self.header = header
        self.body = body or {}

    def __str__(self) -> str:
        return f"{self.header}{self.body}"

    def is_reply(self) -> bool:
        return self.header.get("rep-to") is not None

    def is_error(self) -> bool:
        return self.header.get("ret")

    def build_frame(self) -> bytes:
        def dict_to_payload(obj: dict) -> bytes:
            fields = []
            for key, val in obj.items():
                if val is None:
                    fields.append(key)
                else:
                    if isinstance(val, str) and re.search(r"[\s,;=]", val) or key=="message":
                        val_str = f'"{val}"'
                    elif isinstance(val, int) and key=="auth-key":
                        val_str = f"0x{val:X}"
                    else:
                        val_str = str(val)
                    fields.append(f"{key}={val_str}")
            return ";".join(fields).encode('utf-8')
        if not self.body:
            return b"".join([ self.FRAME_START,
                              dict_to_payload(self.header),
                              self.FRAME_END ])
        return b"".join([ self.FRAME_START,
                          dict_to_payload(self.header),
                          self.FRAME_BODY,
                          dict_to_payload(self.body),
                          self.FRAME_END ])

    @classmethod
    def build_frame_raw(cls, header: str, body: str) -> bytes:
        return b"".join([ cls.FRAME_START,
                          header.encode('ascii'),
                          cls.FRAME_BODY,
                          body.encode('ascii'),
                          cls.FRAME_END ])

    @classmethod
    def parse_frame(cls, frame: bytes) -> list["Message"]:
        def parse_single_message(frame: bytes) -> "Message":
            def extract_fields_from(content_bytes: bytes) -> dict[str, object]:
                out = {}
                content_str = content_bytes.decode('utf-8')
                for field in content_str.split(';'):
                    if '=' in field:
                        key, val = field.split('=', 1)
                        out[key] = cls.str_to_value(val)
                    elif field:
                        out[field] = None
                return out

            if cls.FRAME_BODY in frame:
                header_bytes, body_bytes = frame.split(cls.FRAME_BODY, 1)
                header = extract_fields_from(header_bytes)
                body = extract_fields_from(body_bytes)
            else:
                header = extract_fields_from(frame)
                body = {}
            return cls(header, body)
        messages = []
        for m in cls.RE_FRAME.finditer(frame):
            messages.append(parse_single_message(m.group(1)))
        return messages

    @staticmethod
    def str_to_value(value_str: str) -> object:
        def parse_ext_json(s: str) -> dict[str, object]:
            parsed_object = {}
            for key, body in re.findall(r"(\w+)\s*:\s*\{([^}]+)\}", s):
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
            try:
                return int(value_str, 16)
            except ValueError:
                return value_str
        try:
            return int(value_str)
        except ValueError:
            pass
        try:
            return float(value_str)
        except ValueError:
            pass
        return value_str




class Connection:

    def __init__(self, hostname: str, conn_port: int, mach_name: str, client_name: str):
        self.msg_id = 0
        self.sck: socket.socket | None = None
        mach_port, self.key = self.authenticate(hostname, conn_port, mach_name, client_name)
        self.sck, self.mach_data = self.connect(hostname, mach_port, self.key, client_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __del__(self):
        self.disconnect()

    @staticmethod
    def decrypt(frame:bytes, key:int) -> bytes:
        return bytes([b ^ key for b in frame])

    @staticmethod
    def encrypt(frame:bytes, key:int) -> bytes:
        return bytes([b ^ key for b in frame])

    @classmethod
    def send_frame(cls, frame:bytes, sck:socket.socket, key:int|None) -> None:
        #print(f"Send: {frame}")
        if key is not None: frame = cls.encrypt(frame, key)
        sck.sendall(frame)

    @classmethod
    def receive_buffer(cls, sck:socket.socket, key:int|None) -> bytes:
        buf = sck.recv(4096)
        if not buf:
            raise ConnectionAbortedError("Socket closed by MacoLayer")
        if key is not None: buf = cls.decrypt(buf, key)
        #print(f"Recv: {buf}")
        return buf

    @classmethod
    def send_msg_raw(cls, msg:Message, sck:socket.socket, key:int|None) -> None:
        frame = msg.build_frame()
        cls.send_frame(frame, sck, key)

    @classmethod
    def receive_one_raw(cls, sck:socket.socket, key:int|None) -> Message:
        buf = cls.receive_buffer(sck, key)
        messages = Message.parse_frame(buf)
        if( not messages ):
            raise RuntimeError(f"No valid message received: {buf}")
        elif( len(messages)>1 ):
            raise RuntimeError(f"Multiple messages: {buf}")
        return messages[0]

    @classmethod
    def authenticate(cls, host:str, port:int, mach_name:str, client_name:str) -> tuple[int, int]:
        def create_auth_key(pub_key: int) -> int:
            return random.randint(1, 254)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sck:
            sck.settimeout(0.5)
            try:
                sck.connect((host, port))
            except OSError as e:
                raise RuntimeError(f"Ensure MacoLayer running on {host}:{port}") from e
            greet_msg = cls.receive_one_raw(sck,None)
            available_machines = greet_msg.body['machines'] if greet_msg.body['machines'] else []
            if not mach_name in available_machines:
                raise RuntimeError(f"Machine {mach_name} is not available between {available_machines}")
            pub_key = create_auth_key(greet_msg.body['auth-key'])
            cls.send_msg_raw(Message(header={"id":1, "rep-to":greet_msg.header["id"], "msg":"connect"}, body={"sender":f'"{client_name}"', "machine":f'"{mach_name}"', "auth-lvl":0, "auth-key":pub_key}), sck, None)
            reply = cls.receive_one_raw(sck,None)
            if reply.is_error():
                raise RuntimeError(f"Couldn't authenticate for {mach_name} ({reply.header.get('msg')})")
            return int(reply.body["port"]), pub_key

    @classmethod
    def connect(cls, host:str, port:int, key:int, client_name:str) -> tuple[socket.socket, dict]:
        sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sck.settimeout(0.5)
        try:
            sck.connect((host, port))
        except OSError as e:
            raise RuntimeError(f"Can't connect to machine at {host}:{port}") from e
        cls.send_msg_raw(Message(header={"id":1}, body={"sender":f'"{client_name}"'}), sck, key)
        reply = cls.receive_one_raw(sck, key)
        if reply.is_error():
            raise RuntimeError(f"Machine refused: {reply.header.get('msg')}")
        mach_data = reply.body
        return sck, mach_data

    def disconnect(self) -> None:
        try:
            self.sck.close()
        except Exception:
            pass

    def send_raw(self, frame:bytes) -> None:
        self.send_frame(frame, self.sck, self.key)

    def send_message(self, msg:Message) -> None:
        self.send_msg_raw(msg, self.sck, self.key)

    def send_request(self, fields:dict) -> None:
        self.msg_id += 1
        self.send_message(Message(header={"id":self.msg_id}, body=fields))

    def send_notification(self, txt:str, fields:dict) -> None:
        self.send_message(Message(header={"msg":txt}, body=fields))

    def receive_one(self) -> Message:
        return self.receive_one_raw(self.sck, self.key)

    def receive_all(self) -> list["Message"]:
        frame = self.receive_buffer(self.sck, self.key)
        return Message.parse_frame(frame)

    def check_positive_reply(self) -> None:
        reply = self.receive_one()
        if reply.is_error():
            raise RuntimeError(f"Error reply: {reply.header.get('msg')}")

    def read(self, fields_list:list[str]) -> dict:
        fields = {field: None for field in fields_list} # Values of dictionary items is None
        self.send_request(fields)
        reply = self.receive_one()
        if reply.is_error():
            raise RuntimeError(reply.header.get("msg"))
        return reply.body

    def write(self, fields:dict) -> None:
        self.send_request(fields)
        self.check_positive_reply()

    def notify(self, fields:dict) -> None:
        self.send_notification(fields)

    def read_status(self) -> dict:
        return self.read(["$status"])

    def subscribe_to_status_changes(self) -> None:
        self.send_request({"$subscribed":"$status"})
        reply = self.receive_one()
        if reply.is_error():
            raise RuntimeError(f"Cannot subscribe to status changes: {reply.header.get('msg')}")
        if not reply.body:
            raise RuntimeError("Status change subscription not supported")
        return reply.body

    def ping(self) -> str:
        self.msg_id += 1
        self.send_msg(Message(header={"id":self.msg_id, "msg":"ping"}))
        reply = self.receive_one()
        return reply.header["msg"]

    def submit_mac_file(self, prj_name:str, mac_content:str) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            mac_file_path = Path(temp_dir) / "scheme.mac"
            mac_file_path.write_text(mac_content, encoding="utf-8")
            self.send_request({"prj-name":f'"{prj_name}"', "prj-path":f'"{temp_dir}"'})
            self.check_positive_reply()
