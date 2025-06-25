#!/usr/bin/env python3
import socket, time, json
from datetime import datetime

# --- CONFIG ----------------------------------------------
HOST = "localhost"
PORT = 9999
# ---------------------------------------------------------

def get_timestamp_tz():
    return datetime.now().astimezone().isoformat(timespec='milliseconds')

def send_object(client_socket, obj):
    send_json_str(client_socket, f"{json.dumps(obj)}\n")

def send_json_str(client_socket, json_str):
    json_str = json_str.encode("utf-8")
    client_socket.sendall(json_str)
    print(f"Sent: {json_str}")

if __name__ == '__main__':
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((HOST, PORT))
        send_object(client_socket, {'timestamp':get_timestamp_tz(),
                                    'event':'status-changed',
                                    'args':{'generic-status':'WORK'} })
        time.sleep(1)
        send_object(client_socket, {'timestamp':get_timestamp_tz(),
                                    'event':'step-started',
                                    'args':{'prj-name':'test-prj', 'scheme':1, 'step':2, 'step-data':'status:{stack:0, proc:1}, sheet:{size:3210x2400, align:2447},  op:{lowe, score, y:500|1600}, prod:{size:2447x2400, rotate:-90}, remn:{size:763x2400}'} })
        time.sleep(1)
        send_object(client_socket, {'timestamp':get_timestamp_tz(),
                                    'event':'alarms-changed',
                                    'args':{'emg-list':'123,30800012'} })
