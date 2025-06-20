#!/usr/bin/env python3
import socket, time, json
from datetime import datetime

# --- CONFIG ----------------------------------------------
HOST = "localhost"
PORT = 9999
# ---------------------------------------------------------

def get_timestamp_tz():
    return datetime.now().astimezone().isoformat(timespec='milliseconds')

def send_data(client_socket, data):
    json_str = json.dumps(data) + "\n"
    client_socket.sendall(json_str.encode("utf-8"))
    print(f"Sent: {json_str}")

if __name__ == '__main__':
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((HOST, PORT))
        send_data(client_socket, { 'timestamp':get_timestamp_tz(),
                                   'event':'status-changed',
                                   'args':{'generic-status':'WORK'} })
        time.sleep(1)
        send_data(client_socket, { 'timestamp':get_timestamp_tz(),
                                   'event':'step-started',
                                   'args':{'prj-name':'test-prj', 'scheme':1, 'step':2} })
        time.sleep(1)
        send_data(client_socket, { 'timestamp':get_timestamp_tz(),
                                   'event':'alarms-changed',
                                   'args':{'emg-list':'123,30800012'} })
