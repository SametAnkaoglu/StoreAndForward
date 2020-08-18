import paho.mqtt.client as mqtt
import os
import random
import json
import time
import csv

# Attributes
__attributes = {
    "iot_device_id" : 0, # os.environ["iot_device_id"],
    "group"         : "A",
    "is_runnable"   : True,
    "sensor_a"      : 0.0,
    "sensor_b"      : 0,
    "sensor_c"      : 0
}

__edge_device_connected_with_iot_devices    = False     
__no_empty_space                            = False     # When an empty space limit is reached 
__path_to_collected_data                    = "/home/rgx/workspace/University/Verteilte-Systeme/iot-device/collected_data_" + str(__attributes["iot_device_id"]) + ".csv"
__write_error_in_file_once                  = False
__published_all_data                        = False
__registered_at_edge_device                 = False

# Flags for ack from edge-device.


# Const
SENSOR_UPPER_LIMIT                          = 30        # int(os.environ["SENSOR_UPPER_LIMIT"])
SENSOR_LOWER_LIMIT                          = -30       # int(os.environ["SENSOR_LOWER_LIMIT"])
IOT_DEVICE_CAPACITANCE_MB                   = 0.0025 

# Methods
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
    
    # Define global
    global __edge_device_connected_with_iot_devices
    global __registered_at_edge_device
    # Prints the topic and message
    print(msg.topic + " " + str(msg.payload))

    # Convert to json
    mqtt_msg = json.loads(msg.payload)

    # Update localization of the edge-device
    if msg.topic == "simulation/":
        __edge_device_connected_with_iot_devices = mqtt_msg["edge_device_connected_with_iot_devices"]

    # Msg from cloud
    if __edge_device_connected_with_iot_devices:

        if msg.topic == "edge-device/connection/":
            __edge_device_connected_with_iot_devices = mqtt_msg["edge_device_connected_with_iot_devices"]

        if msg.topic == "edge-device/delete_data/":
            if int(mqtt_msg["iot_device_id"]) == __attributes["iot_device_id"]:
                os.remove(__path_to_collected_data)

        if msg.topic == "edge-device/send_data/":
            if int(mqtt_msg["iot_device_id"]) == __attributes["iot_device_id"]:  
                __registered_at_edge_device     = True

                file                            = open(__path_to_collected_data, "rb")
                imagestring                     = file.read()
                byteArray                       = bytes(imagestring).decode('utf8').replace("'", '"')
                file.close()

                data                            = {"iot_device_id" : __attributes["iot_device_id"], "byteArray": byteArray}

                data                            = json.dumps(data)

                # Publish values of the iot_devices to edge device
                client.publish("iot-devices/collected_data/", data)
                    
def generate_sensor_data():
    # Possibility that an sensor change his value. 20% is setted and the iot_device needs to be runnable for changes.
    if __attributes["is_runnable"] and random.randint(0,100) > 50:
        __attributes["sensor_a"] += (random.randint(-9, 9)) / 10
        __attributes["sensor_b"] += (random.randint(-9, 9))
        __attributes["sensor_c"] += (random.randint(-9, 9))
        print_attributes()

def check_limits():  
    # If an sensor exceed an limit or no empty space is avaible for saving data -> set is_runnable flag on False. 
    if (__no_empty_space or __attributes["sensor_a"] > SENSOR_UPPER_LIMIT or __attributes["sensor_a"] < SENSOR_LOWER_LIMIT or __attributes["sensor_b"] > SENSOR_UPPER_LIMIT or __attributes["sensor_b"] < SENSOR_LOWER_LIMIT or __attributes["sensor_c"] > SENSOR_UPPER_LIMIT or __attributes["sensor_c"] < SENSOR_LOWER_LIMIT):
        __attributes["is_runnable"] = False
        print_attributes()
    else:
        __attributes["is_runnable"] = True
        __write_error_in_file_once = False

def check_capacitance():
    global __no_empty_space

    # Check if an file exist
    if os.path.isfile(__path_to_collected_data):
        # Get actual file size
        file_stats      = os.stat(__path_to_collected_data)
        file_size_in_mb = float(file_stats.st_size / (1024 * 1024))

    else:
        file_size_in_mb = 0.0
    
    # Print occupied space
    print(str(IOT_DEVICE_CAPACITANCE_MB) + "MB/" + str(file_size_in_mb))

    # Check if file size reach the limit 
    if file_size_in_mb >= IOT_DEVICE_CAPACITANCE_MB:
        __no_empty_space = True
    else:
        __no_empty_space = False

def print_attributes():
    print("ID: " + str(__attributes["iot_device_id"]) + ", is_runnable: "+ str(__attributes["is_runnable"]) + ", sensor_a: " + str(__attributes["sensor_a"]) + ", sensor_b: " + str(__attributes["sensor_b"]) + ", sensor_c: " + str(__attributes["sensor_c"]))
 
def publish_data():
    """Publish __data to iot_devices/ (edge device).
    Returns
    -------
    none
    
    """

    file                            = open(__path_to_collected_data, "rb")
    imagestring                     = file.read()
    byteArray                       = bytes(imagestring).decode('utf8').replace("'", '"')
    file.close()
            
    data                            = {"iot_device_id" : __attributes["iot_device_id"], "byteArray": byteArray}
    
    data                            = json.dumps(data)

    # Publish values of the iot_devices to edge device
    client.publish("iot-devices/collected_data/", data)


def write_collected_data_into_file(__path, __data):
     # Fieldnames
        fnames = ['iot_device_id', 'group', 'is_runnable', 'sensor_a', 'sensor_b', 'sensor_c']
        
        # If file already exist. Add entry at the end of the file
        if os.path.isfile(__path) :
            ml_file = open(__path, 'a') 
            with ml_file: 
                writer = csv.DictWriter(ml_file, fieldnames=fnames)
                writer.writerow({   'iot_device_id'     : __data["iot_device_id"],    
                                    'group'             : __data["group"],
                                    'is_runnable'       : __data["is_runnable"],
                                    'sensor_a'          : __data["sensor_a"],
                                    'sensor_b'          : __data["sensor_b"],
                                    'sensor_c'          : __data["sensor_c"]})
                ml_file.close()                                
        
        # Else file dont exist. Create an new file
        else:
            ml_file = open(__path, 'w')
            with ml_file:
                writer = csv.DictWriter(ml_file, fieldnames=fnames) 
                writer.writeheader() 
                writer.writerow({   'iot_device_id'     : __data["iot_device_id"],    
                                    'group'             : __data["group"],
                                    'is_runnable'       : __data["is_runnable"],
                                    'sensor_a'          : __data["sensor_a"],
                                    'sensor_b'          : __data["sensor_b"],
                                    'sensor_c'          : __data["sensor_c"]})
                ml_file.close()
 
# Mqtt initialization, subscribtions and start client loop etc.
client              = mqtt.Client()
client.on_connect   = on_connect
client.on_message   = on_message
client.connect("localhost", 1883, 60)
client.subscribe("edge-device/#")
client.subscribe("simulation/#") # Topic for simulation inputs    
client.loop_start()

while True:

    # Only if iot-device is runnable generate sensor data
    if __attributes["is_runnable"]:
        generate_sensor_data()
        __write_error_in_file_once = False
    
    # If not runnable
    else:

        # Need to write the error state into the file. Only at an error state NOT at full space
        if not __write_error_in_file_once and not __no_empty_space:
            write_collected_data_into_file(__path_to_collected_data, __attributes)
            __write_error_in_file_once = True
 
    check_capacitance()
    check_limits() 

    # If no storage is free stop saving data
    if __no_empty_space:
        print("No empty space for saving data. Need to publish...")
        
    # If space is empty save data in file
    else:    

        # Only if iot-device is runnable save data in file
        if __attributes["is_runnable"]:
            print("Keep saving data...")
            write_collected_data_into_file(__path_to_collected_data, __attributes)
        
        # If iot-device is broken wait until its get repaired 
        else:
            print("Iot-device is broken cannot save data...")

    # Connection to edge-device is etablished
    if __edge_device_connected_with_iot_devices:
        if not __registered_at_edge_device:
            print("Connected to edge-device. Send actual informations...")
            client.publish("iot-devices/live_state/", json.dumps(__attributes))

    time.sleep(1)
    
    