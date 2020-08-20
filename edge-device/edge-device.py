import paho.mqtt.client as mqtt
import json
import time
import os
import csv 

time.sleep(3)

__iot_devices                                               = list()
__edge_device_connected_with_cloud                          = True
__edge_device_connected_with_iot_devices                    = False
__edge_device_is_in_possession_of_collected_data            = False
__edge_device_published_update_to_all_iot_devices           = False
__error_publish_update                                      = False
__error_get_sensor_data                                     = False
__group                                                     = "empty"
__count_of_iot_devices                                      = 0
__publish_update                                            = False
__get_sensor_data                                           = False
__path_to_home_directory                                    = "./"
__update_version                                            = "0.0"
__get_backup_files                                          = bool(os.environ["GET_BACKUP_FILES"])

def on_connect(client, user__data, flags, rc):
    print("Connected with result code " + str(rc))

def on_message(client, user__data, msg):
    global __count_of_iot_devices
    global __group
    global __get_sensor_data
    global __publish_update
    global __edge_device_connected_with_cloud
    global __edge_device_connected_with_iot_devices
    global __update_version

    # Convert to json
    mqtt_msg_json = json.loads(msg.payload)

    # Update localization of the edge-device
    if msg.topic == "simulation/":
        __edge_device_connected_with_cloud          = bool(mqtt_msg_json["edge_device_connected_with_cloud"])
        __edge_device_connected_with_iot_devices    = bool(mqtt_msg_json["edge_device_connected_with_iot_devices"])
    
    # Msg from cloud
    if __edge_device_connected_with_cloud:
        # Update information for trip
        if msg.topic == "cloud/job/":
            __count_of_iot_devices              = int(mqtt_msg_json["count_of_iot_devices"])
            __group                             = str(mqtt_msg_json["group"]) 
            __get_sensor_data                   = bool(mqtt_msg_json["get_sensor_data"])
            __publish_update                    = bool(mqtt_msg_json["publish_update"])
            __edge_device_connected_with_cloud  = False

        if msg.topic == "cloud/update/":
            update_version      = json.loads(msg.payload)["update_version"]
            mqtt_msg_json       = json.loads(msg.payload.decode('utf8').replace('\\\\','\\'))
            byteArray           = mqtt_msg_json["byteArray"].encode('utf-8')
            __update_version    = str(update_version)  
            write_received_update_to_file(byteArray, update_version)
            
    # Msg from iot-devices
    if __edge_device_connected_with_iot_devices:

        # Update the iot_device_list
        if msg.topic == "iot-devices/live_state/":
            update_iot_device_list(msg)

        if msg.topic == "iot-devices/collected_data/":
            iot_device_id   = json.loads(msg.payload)["iot_device_id"]
            mqtt_msg_json   = json.loads(msg.payload.decode('utf8').replace('\\\\','\\'))
            byteArray       = mqtt_msg_json["byteArray"].encode('utf-8')

            write_received_data_to_file(byteArray, iot_device_id)

        if msg.topic == "iot-devices/backup/" and __get_backup_files:
            iot_device_id   = json.loads(msg.payload)["iot_device_id"]
            mqtt_msg_json   = json.loads(msg.payload.decode('utf8').replace('\\\\','\\'))
            byteArray       = mqtt_msg_json["byteArray"].encode('utf-8')

            write_received_data_to_file(byteArray, str(iot_device_id) +"_backup")

def update_iot_device_list(msg):
    global __iot_devices
    
    # convert to json
    data = json.loads(msg.payload)

    # Only if the group name is valid save the device
    if __group == data["group"]:
        i = 0
        is_updated = False
        
        # Itterating over the Machine List to check if the Machine that send an message is already in the list.
        # if: true -> Updating Values, false -> Insert to the end of the list
        while i < len(__iot_devices) and is_updated == False:

            # true -> Updating Values
            if data["iot_device_id"] == __iot_devices[i]["iot_device_id"]:
                
                # Updating values
                __iot_devices[i]    = data
                
                # Set flag
                is_updated          = True

                # Prints machine that is added to the list
                print("IOT-DEVICE: " + str(data["iot_device_id"]) + " update values")

            i += 1

        # false -> Insert to the end of the list
        if is_updated == False:

            # Insert at the end of the list
            __iot_devices.insert(len(__iot_devices), data)
            
            # Prints machine that is added to the list
            print("IOT-DEVICE: " + str(data["iot_device_id"]) + " dont exist so add to List")

def reset_edge_device_attributes():
    global __iot_devices                                   
    global __edge_device_connected_with_cloud              
    global __edge_device_connected_with_iot_devices        
    global __edge_device_is_in_possession_of_collected_data
    global __error_publish_update                          
    global __error_get_sensor_data                         
    global __group                                         
    global __count_of_iot_devices                          
    global __publish_update                                
    global __get_sensor_data
    global __edge_device_published_update_to_all_iot_devices

    __iot_devices.clear()
    __edge_device_connected_with_cloud                          = True
    __edge_device_connected_with_iot_devices                    = False
    __edge_device_is_in_possession_of_collected_data            = False
    __edge_device_published_update_to_all_iot_devices           = False
    __error_publish_update                                      = False
    __error_get_sensor_data                                     = False
    __group                                                     = "empty"
    __count_of_iot_devices                                      = 0
    __publish_update                                            = False
    __get_sensor_data                                           = False

    # Delete existing sensordata-files for clear start
    delete_files_with_end(__path_to_home_directory, ".csv")
    delete_files_with_start(__path_to_home_directory, "update")

def write_received_data_to_file(byteArray, iot_device_id):
    if not os.path.isfile(__path_to_home_directory + "iot_device_id_" + str(iot_device_id) + ".csv"):
        with open(__path_to_home_directory + "iot_device_id_" + str(iot_device_id) + ".csv", "wb") as fd:
            fd.write(byteArray)
            fd.close()

def write_received_update_to_file(byteArray, update_version):
    if not os.path.isfile(__path_to_home_directory + "update_" + str(update_version) + ".txt"):
        with open(__path_to_home_directory + "update_" + str(update_version) + ".txt", "wb") as fd:
            fd.write(byteArray)
            fd.close()

def get_list_of_not_collected_iot_devices():
    i = 0
    need_to_collect = list()

    while i < len(__iot_devices):
        # If the data from the iot device "i" has already collected by the edge-device go to next
        if not os.path.isfile(__path_to_home_directory + "iot_device_id_" + str(__iot_devices[i]["iot_device_id"]) + ".csv"):
            need_to_collect.insert(len(need_to_collect), __iot_devices[i]["iot_device_id"])
        else:
            client.publish("edge-device/delete_data/", json.dumps({"iot_device_id" : str(__iot_devices[i]["iot_device_id"])}))
                  
        i += 1
    
    return need_to_collect

def delete_files_with_end(path, ending):
    files_in_directory = os.listdir(path)
    filtered_files = [file for file in files_in_directory if file.endswith(str(ending))]
    
    for file in filtered_files:
	    path_to_file = os.path.join(path, file)
	    os.remove(path_to_file)
        
def delete_files_with_start(path, starting):
    files_in_directory = os.listdir(path)
    filtered_files = [file for file in files_in_directory if file.startswith(str(starting))]
    
    for file in filtered_files:
	    path_to_file = os.path.join(path, file)
	    os.remove(path_to_file)

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

def publish_update(topic, path_to_data, group, update_version):
    """Publish file to topic/
    Returns
    -------
    none
    
    """

    file                            = open(path_to_data, "rb")
    imagestring                     = file.read()
    byteArray                       = bytes(imagestring).decode('utf8').replace("'", '"')
    file.close()
            
    data                            = {"update_version": update_version ,"group" : group, "byteArray": byteArray}
    
    data                            = json.dumps(data)

    client.publish(topic, data)

def check_update_is_done_on_all_iot_devices(update_version):
    i = 0

    while i < len(__iot_devices):
        if not __iot_devices[i]["update_version"] == update_version:
            return False
        i += 1
    
    return True

def write_report_into_file(path, data):
    fnames = ['iot_device_id', 'group','update_version', 'is_runnable', 'sensor_a', 'sensor_b', 'sensor_c']
        
    # If file already exist. Add entry at the end of the file
    if os.path.isfile(path) :
        ml_file = open(path, 'a') 
        with ml_file: 
            writer = csv.DictWriter(ml_file, fieldnames=fnames)
            writer.writerow({   'iot_device_id'     : data["iot_device_id"],    
                                'group'             : data["group"],
                                'update_version'    : data["update_version"],
                                'is_runnable'       : data["is_runnable"],
                                'sensor_a'          : data["sensor_a"],
                                'sensor_b'          : data["sensor_b"],
                                'sensor_c'          : data["sensor_c"]})
            ml_file.close()                                
    
    # Else file dont exist. Create an new file
    else:
        ml_file = open(path, 'w')
        with ml_file:
            writer = csv.DictWriter(ml_file, fieldnames=fnames) 
            writer.writeheader() 
            writer.writerow({   'iot_device_id'     : data["iot_device_id"],    
                                'group'             : data["group"],
                                'update_version'    : data["update_version"],
                                'is_runnable'       : data["is_runnable"],
                                'sensor_a'          : data["sensor_a"],
                                'sensor_b'          : data["sensor_b"],
                                'sensor_c'          : data["sensor_c"]})
            ml_file.close()


client              = mqtt.Client()
client.on_connect   = on_connect
client.on_message   = on_message
client.connect("localhost", 1883, 60)
client.subscribe("iot-devices/#")
client.subscribe("cloud/#")
client.subscribe("simulation/#") # Topic for simulation inputs 
client.loop_start()
i = 0

# Delete existing files for clear start
delete_files_with_end(__path_to_home_directory, ".csv")
delete_files_with_start(__path_to_home_directory, "update")

while True:
    
    # Connected to iot-devices
    if __edge_device_connected_with_iot_devices and not __edge_device_connected_with_cloud:
        
        # Wait until all devices are connected
        if len(__iot_devices) < __count_of_iot_devices:
            print(str(len(__iot_devices)) + "/" + str(__count_of_iot_devices) + " are connected")
        
        else:
            print("All devices are connected")
            
            # Only get sensor data
            if __get_sensor_data and not __publish_update:

                # Get list of iot device ids that data are missing 
                need_to_collect_iot_device_ids = get_list_of_not_collected_iot_devices()

                i = 0
                while i < len(need_to_collect_iot_device_ids):
                    print("Get data from iot-device: " + str(need_to_collect_iot_device_ids[i]))

                    # Say to the iot-device "i" that it can send his collected data
                    client.publish("edge-device/send_data/", json.dumps({"iot_device_id" : need_to_collect_iot_device_ids[i], "send_backup_file": __get_backup_files}))
                    
                    i += 1

                if len(need_to_collect_iot_device_ids) == 0:
                    print("In possession of collected data")
                    __edge_device_is_in_possession_of_collected_data            = True

            # Only publish update
            elif not __get_sensor_data and __publish_update:
                
                # Publish update to iot-devices
                publish_update("edge-device/update/", __path_to_home_directory + "update_" + __update_version + ".txt", __group, __update_version)

                while not __edge_device_published_update_to_all_iot_devices:
                    
                    if check_update_is_done_on_all_iot_devices(__update_version):
                        __edge_device_published_update_to_all_iot_devices           = True
                        print("Update has succesfully published to all iot-devices")
                    else:
                        print("Update isnt deployed on all devices")
                          
            # Get sensor data after that publish update
            elif __get_sensor_data and __publish_update:

                # Get list of iot device ids that data are missing 
                need_to_collect_iot_device_ids = get_list_of_not_collected_iot_devices()

                i = 0
                while i < len(need_to_collect_iot_device_ids):
                    print("Get data from iot-device: " + str(need_to_collect_iot_device_ids[i]))

                    # Say to the iot-device "i" that it can send his collected data
                    client.publish("edge-device/send_data/", json.dumps({"iot_device_id" : need_to_collect_iot_device_ids[i], "send_backup_file": __get_backup_files}))
                    
                    i += 1

                if len(need_to_collect_iot_device_ids) == 0:
                    print("In possession of collected data")

                     # Publish update to iot-devices
                    publish_update("edge-device/update/", __path_to_home_directory + "update_" + __update_version + ".txt", __group, __update_version)

                    while not __edge_device_published_update_to_all_iot_devices:

                        if check_update_is_done_on_all_iot_devices(__update_version):
                            __edge_device_published_update_to_all_iot_devices           = True
                            __edge_device_is_in_possession_of_collected_data            = True
                            print("Update has succesfully published to all iot-devices")
                        else:
                            print("Update isnt deployed on all devices")
 
            # After job is done dissconnect from iot-devices
            if __edge_device_published_update_to_all_iot_devices or __edge_device_is_in_possession_of_collected_data:

                # Disconnenct from iot-devices
                __edge_device_connected_with_iot_devices                    = False

                # Publish to iot-devices
                client.publish("edge-device/connection/", json.dumps({"edge_device_connected_with_iot_devices": __edge_device_connected_with_iot_devices}))
    
    # Connected to cloud
    elif not __edge_device_connected_with_iot_devices and __edge_device_connected_with_cloud:

        if __edge_device_is_in_possession_of_collected_data or __edge_device_published_update_to_all_iot_devices:
            print("Publish data to Cloud...")

            i = 0
            while i < len(__iot_devices):
                if __get_sensor_data:
                    publish_data("edge-device/sensordata/", __path_to_home_directory +"iot_device_id_" + str(__iot_devices[i]["iot_device_id"]) + ".csv", str(__iot_devices[i]["iot_device_id"]))
                    
                    if __get_backup_files and os.path.isfile(__path_to_home_directory +"iot_device_id_" + str(__iot_devices[i]["iot_device_id"]) + "_backup.csv"):
                        publish_data("edge-device/backup/", __path_to_home_directory +"iot_device_id_" + str(__iot_devices[i]["iot_device_id"]) + "_backup.csv", str(__iot_devices[i]["iot_device_id"]))

                if __publish_update:
                    write_report_into_file(__path_to_home_directory + "Report.csv", __iot_devices[i])
                i += 1

            if __publish_update:
                publish_data("edge-device/report/", __path_to_home_directory + "Report.csv", "Report")

            # After successfully publish data to cloud reset edge-device for a new job
            reset_edge_device_attributes()

        else:
            print("Wait for instructions...")

    # Connected to iot-devices and cloud
    elif __edge_device_connected_with_iot_devices and __edge_device_connected_with_cloud:
        print("Disconnect from cloud or iot device...")

    # Connected to non of them. Just print actual job.
    elif not __edge_device_connected_with_iot_devices and not __edge_device_connected_with_cloud:
         
        if __error_publish_update:
            print("Error! Publish update.")

        if __error_get_sensor_data:
            print("Error! Get sensor data.")
        
        if __edge_device_is_in_possession_of_collected_data:
            print("Data was succesfull collected. On the way to cloud.")
        
        if __edge_device_published_update_to_all_iot_devices:
            print("Iot-devices successfully updated. On the way to cloud.")

        else:
            print("On the way to iot-devices. Job: ")
            print("Get sensor data: " + str(__get_sensor_data) + ", publish update: " + str(__publish_update) + ", group: " + str(__group) + ", count: " + str(__count_of_iot_devices))
    
    time.sleep(1)