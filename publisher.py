import json
import time
import logging as lg
import paho.mqtt.client as paho
import os
import platform
version = platform.python_version().split(".")[0]
if version == "3":
  import app_config.app_config as cfg
elif version == "2":
  import app_config as cfg
config = cfg.getconfig()
import datetime
import traceback

def on_message(client, userdata, msg):
    
    pass

def on_connect(client, userdata, flags, rc):
    pass


def on_log(client, userdata, obj, buff):
    print("log:" + str(buff))

port = os.environ.get("Q_PORT")
if not port:
    port = 1883
else:
    port = int(port)
print("Running port", port)


print(config['BROKER_ADDRESS'])


client = paho.Client()
client.on_log = on_log
client.on_connect = on_connect
client.on_message = on_message


client.connect(config['BROKER_ADDRESS'], port, 60)

deleteMeta = "apcManager/deleteMeta"
createMeta = "apcManager/createMeta"

unitsIdList = ["635b6f9fef6a59000703f924"]

while True:
    ip = input("enter command...")
    if ip == "d":
       client.publish(deleteMeta,json.dumps(unitsIdList))
    elif ip == "c":
       client.publish(createMeta,json.dumps(unitsIdList))