import paho.mqtt.client as mqtt
import os
import asyncio
import uuid
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
from datetime import datetime

ScriptVersion = "1.0"
ModifiedDate = "Monday 15, November 2021"
MQTTBrokerIP = "10.0.0.200" #IP Address of your MQTT Broker
MQTTTopicSubscribe = "stat/+/POWER" #MQTT Topic Filter
MQTTClientName = "RaspiPI4" #Used to identify the device to your MQTT Broker
AzureIOTHub_conn_str = "HostName=IOTHub-Baldacchino.azure-devices.net;DeviceId=HVXL;SharedAccessKey=sTlA3ArDViPt7iVgcKCHsddWp6wH6ryvDJNJ3VN1zsA=" #Azure IOT Hub Connection String

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print(str(datetime.now()) + " | Connecting to MQTT Broker : " + MQTTBrokerIP)
    print(str(datetime.now()) + " | Connected with result code {0}".format(str(rc))) 
    print(str(datetime.now()) + " | We are connected!")
    print()
    print(str(datetime.now()) + " | Subscribing to MQTT Topics")
    print(str(datetime.now()) + " | Subscribing to " + MQTTTopicSubscribe)
    client.subscribe(MQTTTopicSubscribe)
    print()

def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    global mqtt_topic
    mqtt_topic = msg.topic
    global mqtt_payload
    mqtt_payload = str(msg.payload)
    print(str(datetime.now()) + " | Message received")
    print(str(datetime.now()) + " | MQTT Topic and Payload: " + msg.topic + " " + str(msg.payload)[2:][:-1])  # Print a received msg
    asyncio.run(azure())
    

async def azure():
    # Create instance of the device client using the connection string
    device_client = IoTHubDeviceClient.create_from_connection_string(AzureIOTHub_conn_str)
     
    # Connect the device client.
    await device_client.connect()
    print(str(datetime.now()) + " | Async connection established to Azure IOT")

    # Send a single message
    print(str(datetime.now()) + " | Sending message to Azure IOT Hub")
    msg = Message("{ \"DateTime\": \"" + str(datetime.now()) + "\", \"MQTT Topic\": \"" + mqtt_topic + "\", \"Payload\": \"" + mqtt_payload[2:][:-1] + "\" }")
    msg.message_id = uuid.uuid4()
    msg.content_encoding = "utf-8"
    msg.content_type = "application/json"

    
    await device_client.send_message(msg)
    print(str(datetime.now()) + " | Message sent, tearing down Azure IOT Hub connection")
    print()

    # Finally, shut down the client
    await device_client.shutdown()

print("*********************************************************************")
print("*                                                                   *")
print("*                                                                   *")
print("*               MQTT --> Azure IOT Bridge                           *")
print("*                                                                   *")
print("*                                                                   *")
print("*                                                                   *")
print("* shane@baldacchino.net                                             *")
print(f"* Version : {ScriptVersion}                                                     *")
print(f"* Modified Date : {ModifiedDate}                          *")
print("*                                                                   *")
print("*********************************************************************")


client = mqtt.Client(MQTTClientName)  
client.on_connect = on_connect 
print(str(datetime.now()) + " | Listening for messages")
print()
client.on_message = on_message  
client.connect(MQTTBrokerIP, 1883, 60)  # Connect to (broker, port, keepalive-time)
client.loop_forever()  # Start networking daemon
try:
    asyncio.run(azure())
except:
    pass #continue on errors - used to solve internet connectivity issues. 
