from apcmanagerlmpl import config,apcManager,os,json
import paho.mqtt.client as paho

# unitsIdList = ["62f3a6d6f38f4206da2bf0a5","62f3a6ebf38f4206da2bf0a7"]
# unitsIdList = ["635b6f9fef6a59000703f924"]
unitsIdList = ["5c4ed3f5fe02914642b4d5bc"]
apc = apcManager(unitsIdList)
# apc.deleteTagAndCalMeta()
apc.createTagAndCalMeta()
# apcapi = apcManagerApi(unitsIdList)
# # apcapi.ApcData("months","Equipment")
# apcapi.apcDataIndividualTag("days")


def on_message(client, userdata, msg):
    body = json.loads(msg.payload)
    unitsIdList = body["unitsIdList"]
    apc = apcManager(unitsIdList)

    if "createMeta" in msg.topic:
        apc.createTagAndCalMeta()
    elif "deleteMeta" in msg.topic:
        apc.deleteTagAndCalMeta()
        


def on_connect(client, userdata, flags, rc):
    topic_line = ["apcManager/createMeta","apcManager/deleteMeta"]
    for i in topic_line:
        client.subscribe(i)



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
client.loop_forever(retry_first_connection=True)