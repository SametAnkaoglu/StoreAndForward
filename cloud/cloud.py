import time
import paho.mqtt.client as mqtt
import json
import os
from datetime import datetime

__path_to_home_directory                = "/home/rgx/workspace/University/Verteilte-Systeme/cloud/"
__path_to_update_file                   = "update.txt"
__count_of_groups                       = 0
__existing_groups                       = list()        # Fill this with commandline inputs
__edge_device_connected_with_cloud      = True         
__publish_update                        = False
__get_sensor_data                       = False
__edge_device_is_able_to_go             = False
__target_group                          = "A"
__target_count                          = "1"
__target_update_version                 = 1
__job                                   = 3 # 1) get data from an group of iot devices, 2) publish update to a group of iot devices, 3) get data and publish update to a group of iot device

def on_connect(client, user__data, flags, rc):
    print("Connected with result code " + str(rc))

def on_message(client, user__data, msg):
    # Define globals
    global __edge_device_connected_with_cloud
    global __edge_device_is_able_to_go
    
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
            iot_device_id   = json.loads(msg.payload)["iot_device_id"]
            mqtt_msg_json   = json.loads(msg.payload.decode('utf8').replace('\\\\','\\'))
            byteArray       = mqtt_msg_json["byteArray"].encode('utf-8')

            write_received_data_to_file(byteArray, iot_device_id)
        
        if msg.topic == "edge-device/backup/":
            iot_device_id   = json.loads(msg.payload)["iot_device_id"]
            mqtt_msg_json   = json.loads(msg.payload.decode('utf8').replace('\\\\','\\'))
            byteArray       = mqtt_msg_json["byteArray"].encode('utf-8')

            write_received_data_to_file(byteArray, str(iot_device_id) +"_backup")

        if msg.topic == "edge-device/report/":
            mqtt_msg_json   = json.loads(msg.payload.decode('utf8').replace('\\\\','\\'))
            byteArray       = mqtt_msg_json["byteArray"].encode('utf-8')

            write_report_data_to_file(byteArray)

def add_group_to_list(group):
    global __existing_groups
    __existing_groups.insert(len(__existing_groups),group)

def create_groups_list():
    global __count_of_groups
    global __existing_groups

    if len(__existing_groups) > 0:
        __existing_groups.clear()

    __count_of_groups = 3
    add_group_to_list("A")
    add_group_to_list("B")
    add_group_to_list("C")
    
    #i = 0
    #while i < __count_of_groups:
        #input_group = input("Enter one group name then press enter: ")

        #i += 1

def publish_data(topic, path_to_data, updated_version):
    file                            = open(path_to_data, "rb")
    imagestring                     = file.read()
    byteArray                       = bytes(imagestring).decode('utf8').replace("'", '"')
    file.close()
            
    data                            = {"update_version" : updated_version, "byteArray": byteArray}
    
    data                            = json.dumps(data)

    client.publish(topic, data)

def write_received_data_to_file(byteArray, iot_device_id):
    now         = datetime.now()
    dt_string   = str(now.strftime("%d_%m_%Y_%H_%M_%S")) 

    if not os.path.isfile(__path_to_home_directory + "iot_device_id_" + str(iot_device_id) + "_" + dt_string + ".csv"):
        with open(__path_to_home_directory + "iot_device_id_" + str(iot_device_id) + "_" + dt_string + ".csv", "wb") as fd:
            fd.write(byteArray)
            fd.close()

def write_report_data_to_file(byteArray):
    now         = datetime.now()
    dt_string   = str(now.strftime("%d_%m_%Y_%H_%M_%S")) 

    if not os.path.isfile(__path_to_home_directory + "Report_" + dt_string + ".csv"):
        with open(__path_to_home_directory + "Report_" + dt_string + ".csv", "wb") as fd:
            fd.write(byteArray)
            fd.close()

def delete_files_with_end(path, ending):
    files_in_directory = os.listdir(path)
    filtered_files = [file for file in files_in_directory if file.endswith(str(ending))]
    
    for file in filtered_files:
	    path_to_file = os.path.join(path, file)
	    os.remove(path_to_file)
        
client              = mqtt.Client()
client.on_connect   = on_connect
client.on_message   = on_message
client.connect("localhost", 1883, 60)
client.subscribe("simulation/#") # Topic for simulation inputs 
client.subscribe("edge-device/#")
client.loop_start()

create_groups_list()

# Delete existing sensordata-files for clear start
delete_files_with_end(__path_to_home_directory, ".csv")

while True:

    if __edge_device_connected_with_cloud:

        print("Connection to edge-device is etablished")

        # If edge device comes back from the trip to the iot-devices
        if      __get_sensor_data   or     __publish_update:
            print("Edge-device is back from his trip. Look at the sensordata-files or the report for more information.")
            
            # Reset flags
            __get_sensor_data                       = False
            __publish_update                        = False     

        # Send edge device to the trip to the iot-devices with an job
        else:
            # __job = int(input("Edge-device is connected \n1)get data from an group of iot devices \n2)publish update to a group of iot devices \n3)get data and publish update to a group of iot devices \ntype number:"))

            # Get sensordata
            if      __job == 1:
                __get_sensor_data                       = True
                __publish_update                        = False

            # Publish update
            elif    __job == 2:
                __get_sensor_data                       = False
                __publish_update                        = True
                publish_data("cloud/update/", __path_to_home_directory + __path_to_update_file, __target_update_version)

            # Get sensor data and publish update
            elif    __job == 3:
                __get_sensor_data                       = True
                __publish_update                        = True
                publish_data("cloud/update/", __path_to_home_directory + __path_to_update_file, __target_update_version)

            # Send edge device to the iot devices after get ack
            if __job > 0 and __job < 4 and (__get_sensor_data == True or __publish_update == True):                
                client.publish("cloud/job/", json.dumps({"group": __target_group, "count_of_iot_devices": __target_count, "get_sensor_data": __get_sensor_data, "publish_update": __publish_update}))
                __edge_device_connected_with_cloud = False


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

