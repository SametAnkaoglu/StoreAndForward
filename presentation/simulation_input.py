import paho.mqtt.client as mqtt
import os
import random
import json
import time
import csv

__edge_device_connected_with_cloud          = True
__edge_device_connected_with_iot_devices    = False

def on_connect(client, user__data, flags, rc):
    """ Prints at successful connection
        Define the connect callback implementation.

        Expected signature for MQTT v3.1 and v3.1.1 is:
            connect_callback(client, userdata, flags, rc, properties=None)

        and for MQTT v5.0:
            connect_callback(client, userdata, flags, reasonCode, properties)

        client:     the client instance for this callback
        userdata:   the private user data as set in Client() or userdata_set()
        flags:      response flags sent by the broker
        rc:         the connection result
        reasonCode: the MQTT v5.0 reason code: an instance of the ReasonCode class.
                    ReasonCode may be compared to interger.
        properties: the MQTT v5.0 properties returned from the broker.  An instance
                    of the Properties class.
                    For MQTT v3.1 and v3.1.1 properties is not provided but for compatibility
                    with MQTT v5.0, we recommand adding properties=None.

        flags is a dict that contains response flags from the broker:
            flags['session present'] - this flag is useful for clients that are
                using clean session set to 0 only. If a client with clean
                session=0, that reconnects to a broker that it has previously
                connected to, this flag indicates whether the broker still has the
                session information for the client. If 1, the session still exists.

        The value of rc indicates success or not:
            0: Connection successful
            1: Connection refused - incorrect protocol version
            2: Connection refused - invalid client identifier
            3: Connection refused - server unavailable
            4: Connection refused - bad username or password
            5: Connection refused - not authorised
            6-255: Currently unused.
    """    
    print("Connected with result code " + str(rc))

def on_message(client, user__data, msg):
    """ Gets triggered when an subscribed topic receive an message.
        It will become an message from the edge device that will change the values and states 
        of the machine.  
        Define the message received callback implementation.

        Expected signature is:
            on_message_callback(client, userdata, message)

        client:     the client instance for this callback
        userdata:   the private user data as set in Client() or userdata_set()
        message:    an instance of MQTTMessage.
                    This is a class with members topic, payload, qos, retain.
    """
    global __edge_device_connected_with_cloud
    global __edge_device_connected_with_iot_devices
    
    # Prints the topic and message
    print(msg.topic + " " + str(msg.payload))

    mqtt_msg = json.loads(msg.payload)

    if msg.topic == "cloud/":
        if bool(mqtt_msg["get_sensor_data"]) or bool(mqtt_msg["publish_update"]):
            __edge_device_connected_with_cloud = False
    
    if msg.topic == "edge-device/":
        __edge_device_connected_with_iot_devices = mqtt_msg["edge_device_connected_with_iot_devices"]

client              = mqtt.Client()
client.on_connect   = on_connect
client.on_message   = on_message
client.subscribe("cloud/#")
client.subscribe("edge-device/#")
client.connect("localhost", 1883, 60)
client.loop_start()

while True:
    
    print("__edge_device_connected_with_cloud: " + str(__edge_device_connected_with_cloud)+ "\n__edge_device_connected_with_iot_devices: "+ str(__edge_device_connected_with_iot_devices))

    input_action = int(input("1)Connect edge-device to iot devices \n2)Connect edge-device to cloud \n3)Disconnect edge-device from iot-devices \n4)Disonnect edge-device from cloud \ntype number:"))

    if      input_action == 1:
        __edge_device_connected_with_iot_devices    = True

    elif    input_action == 2:
        __edge_device_connected_with_cloud          = True
        
    elif    input_action == 3:
        __edge_device_connected_with_iot_devices    = False
    
    elif    input_action == 4:
        __edge_device_connected_with_cloud          = False
    
    # Publish values of the iot_devices to edge device
    if input_action > 0 and input_action < 5:
        client.publish("simulation/", json.dumps({"edge_device_connected_with_iot_devices": __edge_device_connected_with_iot_devices, "edge_device_connected_with_cloud": __edge_device_connected_with_cloud}))