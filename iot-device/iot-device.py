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
    "update_version": "0.0",
    "is_runnable"   : True,
    "sensor_a"      : 0.0,
    "sensor_b"      : 0,
    "sensor_c"      : 0
}

__edge_device_connected_with_iot_devices    = False     
__no_empty_space                            = False     # When an empty space limit is reached 
__path_to_collected_data                    = "/home/rgx/workspace/University/Verteilte-Systeme/iot-device/collected_data_" + str(__attributes["iot_device_id"]) + ".csv"
__path_to_home_directory                    = "/home/rgx/workspace/University/Verteilte-Systeme/iot-device/"
__write_error_in_file_once                  = False
__published_all_data                        = False
__registered_at_edge_device                 = False

# Const
SENSOR_UPPER_LIMIT                          = 30        # int(os.environ["SENSOR_UPPER_LIMIT"])
SENSOR_LOWER_LIMIT                          = -30       # int(os.environ["SENSOR_LOWER_LIMIT"])
IOT_DEVICE_CAPACITANCE_MB                   = 0.0025 

# Methods
def on_connect(client, user__data, flags, rc):   
    print("Connected with result code " + str(rc))

def on_message(client, user__data, msg):
 
    # Define global
    global __edge_device_connected_with_iot_devices
    global __registered_at_edge_device

    # Convert to json
    mqtt_msg = json.loads(msg.payload)

    # Update localization of the edge-device
    if msg.topic == "simulation/":
        __edge_device_connected_with_iot_devices = mqtt_msg["edge_device_connected_with_iot_devices"]

    # Msg from edge-device
    if __edge_device_connected_with_iot_devices:

        if msg.topic == "edge-device/connection/": 
            __edge_device_connected_with_iot_devices    = mqtt_msg["edge_device_connected_with_iot_devices"]
            
            # After disconnect reset flag
            __registered_at_edge_device                 = False

        if msg.topic == "edge-device/delete_data/":
            if int(mqtt_msg["iot_device_id"]) == __attributes["iot_device_id"]:
                os.rename(__path_to_collected_data, __path_to_home_directory + "backup_" + str(__attributes["iot_device_id"]) + ".csv")

        if msg.topic == "edge-device/send_data/":
            if int(mqtt_msg["iot_device_id"]) == __attributes["iot_device_id"]:  
                __registered_at_edge_device     = True
                
                if bool(mqtt_msg["send_backup_file"]) and os.path.isfile(__path_to_home_directory + "backup_" + str(__attributes["iot_device_id"]) + ".csv"):
                    file                            = open(__path_to_home_directory + "backup_" + str(__attributes["iot_device_id"]) + ".csv", "rb")
                    imagestring                     = file.read()
                    byteArray                       = bytes(imagestring).decode('utf8').replace("'", '"')
                    file.close()

                    data                            = {"iot_device_id" : __attributes["iot_device_id"], "byteArray": byteArray}

                    data                            = json.dumps(data)

                    # Publish values of the iot_devices to edge device
                    client.publish("iot-devices/backup/", data)

                    os.remove(__path_to_home_directory + "backup_" + str(__attributes["iot_device_id"]) + ".csv")

                file                            = open(__path_to_home_directory + "collected_data_" + str(__attributes["iot_device_id"]) + ".csv", "rb")
                imagestring                     = file.read()
                byteArray                       = bytes(imagestring).decode('utf8').replace("'", '"')
                file.close()

                data                            = {"iot_device_id" : __attributes["iot_device_id"], "byteArray": byteArray}

                data                            = json.dumps(data)

                # Publish values of the iot_devices to edge device
                client.publish("iot-devices/collected_data/", data)

        if msg.topic == "edge-device/update/":
            group           = json.loads(msg.payload)["group"]
            update_version  = json.loads(msg.payload)["update_version"]

            if group == __attributes["group"]:

                if update_version != __attributes["update_version"]:
                    mqtt_msg_json   = json.loads(msg.payload.decode('utf8').replace('\\\\','\\'))
                    byteArray       = mqtt_msg_json["byteArray"].encode('utf-8')

                    write_received_update_to_file(byteArray, update_version)
                    client.publish("iot-devices/live_state/", json.dumps(__attributes))

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
        fnames = ['iot_device_id', 'group','update_version', 'is_runnable', 'sensor_a', 'sensor_b', 'sensor_c']
        
        # If file already exist. Add entry at the end of the file
        if os.path.isfile(__path) :
            ml_file = open(__path, 'a') 
            with ml_file: 
                writer = csv.DictWriter(ml_file, fieldnames=fnames)
                writer.writerow({   'iot_device_id'     : __data["iot_device_id"],    
                                    'group'             : __data["group"],
                                    'update_version'    : __data["update_version"],
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
                                    'update_version'    : __data["update_version"],
                                    'is_runnable'       : __data["is_runnable"],
                                    'sensor_a'          : __data["sensor_a"],
                                    'sensor_b'          : __data["sensor_b"],
                                    'sensor_c'          : __data["sensor_c"]})
                ml_file.close()

def write_received_update_to_file(byteArray, update_version):
    if not os.path.isfile(__path_to_home_directory + "update_" + str(update_version) + ".txt"):
        with open(__path_to_home_directory + "update_" + str(update_version) + ".txt", "wb") as fd:
            fd.write(byteArray)
            fd.close()

    if os.path.isfile(__path_to_home_directory + "update_" + str(__attributes["update_version"]) + ".txt"):
        os.remove(__path_to_home_directory + "update_" + str(__attributes["update_version"]) + ".txt")
    
    __attributes["update_version"] = update_version

def delete_files_with_start(path, starting):
    files_in_directory = os.listdir(path)
    filtered_files = [file for file in files_in_directory if file.startswith(str(starting))]
    
    for file in filtered_files:
	    path_to_file = os.path.join(path, file)
	    os.remove(path_to_file)


# Mqtt initialization, subscribtions and start client loop etc.
client              = mqtt.Client()
client.on_connect   = on_connect
client.on_message   = on_message
client.connect("localhost", 1883, 60)
client.subscribe("edge-device/#")
client.subscribe("simulation/#") # Topic for simulation inputs    
client.loop_start()

delete_files_with_start(__path_to_home_directory, "update")
delete_files_with_start(__path_to_home_directory, "collected")
delete_files_with_start(__path_to_home_directory, "backup")

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
        
        # Need to write an error entry for the next data collecting from the edge-device
        if not os.path.isfile(__path_to_collected_data):
            write_collected_data_into_file(__path_to_collected_data, __attributes)

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
    
    