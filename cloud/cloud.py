import time
import paho.mqtt.client as mqtt
import json

__path_to_home_directory                = "/home/rgx/workspace/University/Verteilte-Systeme/cloud/"
__path_to_update_file                   = "update.txt"
__count_of_groups                       = 0
__existing_groups                       = list()        # Fill this with commandline inputs
__edge_device_connected_with_cloud      = True         
__publish_update                        = False
__get_sensor_data                       = False
__edge_device_is_able_to_go             = False
__need_to_save_report                   = False


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
    # Define globals
    global __edge_device_connected_with_cloud
    global __edge_device_is_able_to_go
    
    # Prints the topic and message
    print(msg.topic + " " + str(msg.payload))

    # Convert to json
    mqtt_msg = json.loads(msg.payload)

    if msg.topic == "simulation/":
        __edge_device_connected_with_cloud = mqtt_msg["edge_device_connected_with_cloud"]
    
    # Msg from edge-device
    if __edge_device_connected_with_cloud:
        if msg.topic == "edge-device/update/ack/":
            print("Edge-device received update.txt")
            __edge_device_is_able_to_go = bool(mqtt_msg["ack"])
        
        if msg.topic == "edge-device/sensordata/":
            __edge_device_is_able_to_go = bool(mqtt_msg["ack"])

        if msg.topic == "edge-device/report/":
            __edge_device_is_able_to_go = bool(mqtt_msg["ack"])

def add_group_to_list(group):
    global __existing_groups
    __existing_groups.insert(len(__existing_groups),group)

def create_groups_list():
    global __count_of_groups
    global __existing_groups

    if len(__existing_groups) > 0:
        __existing_groups.clear()

    __count_of_groups = int(input("Enter the count of groups: "))

    i = 0
    while i < __count_of_groups:
        input_group = input("Enter one group name then press enter: ")
        add_group_to_list(input_group)
        i += 1

def publish_data(topic, path_to_data, iot_device_id):
    """Publish file to topic/
    Returns
    -------
    none
    
    """

    file                            = open(path_to_data, "rb")
    imagestring                     = file.read()
    byteArray                       = bytes(imagestring).decode('utf8').replace("'", '"')
    file.close()
            
    data                            = {"iot_device_id" : iot_device_id, "byteArray": byteArray}
    
    data                            = json.dumps(data)

    client.publish(topic, data)

client              = mqtt.Client()
client.on_connect   = on_connect
client.on_message   = on_message
client.connect("localhost", 1883, 60)
client.subscribe("simulation/#") # Topic for simulation inputs 
client.subscribe("edge-device/#")
client.loop_start()

create_groups_list()

while True:

    if __edge_device_connected_with_cloud:

        print("Connection to edge-device is etablished")

        # If edge device comes back from the trip to the iot-devices
        if      __get_sensor_data   or     __publish_update:
            print("Edge-device is back from his trip. See to the report for more information.")
            
            # Reset flags
            __need_to_save_report                   = True
            __get_sensor_data                       = False
            __publish_update                        = False
            __edge_device_is_able_to_go             = False
        
        elif __need_to_save_report:
            print("Save report from edge-device...")

            # TODO: Check if the hole file was transfered
            
            __need_to_save_report = False

        # Send edge device to the trip to the iot-devices with an job
        else:
            input_action = int(input("Edge-device is connected \n1)get data from an group of iot devices \n2)publish update to a group of iot devices \n3)get data and publish update to a group of iot devices \ntype number:"))

            # Get sensordata
            if      input_action == 1:
                __get_sensor_data                       = True
                __publish_update                        = False
                __edge_device_is_able_to_go             = True

            # Publish update
            elif    input_action == 2:
                __get_sensor_data                       = False
                __publish_update                        = True
                publish_data("cloud/update/", __path_to_home_directory +__path_to_update_file, "edge-device")

            # Get sensor data and publish update
            elif    input_action == 3:
                __get_sensor_data                       = True
                __publish_update                        = True
                publish_data("cloud/update/", __path_to_home_directory +__path_to_update_file, "edge-device")

            # Send edge device to the iot devices after get ack
            if input_action > 0 and input_action < 4 and (__get_sensor_data == True or __publish_update == True):
                
                if __edge_device_is_able_to_go:
                    group = input("Input group name: ")
                    count = input("Input count of iot devices in the group: ")
                    client.publish("cloud/job/", json.dumps({"group": group, "count_of_iot_devices": count, "get_sensor_data": __get_sensor_data, "publish_update": __publish_update}))
                    __edge_device_connected_with_cloud = False

                else:
                    print("Error! Update couldnt transfered to edge-device. Try again...")

    # If the edge device is not connected, output its current job 
    else:
        
        print("Edge-device is not in reach. It's going to ")
        
        if      __get_sensor_data   and     __publish_update:
            print("collect data from iot-devices and publish updates on them")

        elif    __get_sensor_data   and not __publish_update:
            print("collect data from iot-devices")
        
        elif    __publish_update    and not __get_sensor_data:
            print("publish updates")
        
        else:
            print("be not connected. Check if an edge device at all is running?")
        
    time.sleep(1)        

