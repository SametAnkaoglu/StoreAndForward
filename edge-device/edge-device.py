import paho.mqtt.client as mqtt
import json
import time
import os

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
__path_to_home_directory                                    = "/home/rgx/workspace/University/Verteilte-Systeme/edge-device/"

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
    global __count_of_iot_devices
    global __group
    global __get_sensor_data
    global __publish_update
    global __edge_device_connected_with_cloud
    global __edge_device_connected_with_iot_devices

    # Prints the topic and message
    print(msg.topic + " " + str(msg.payload))

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

    # Msg from iot-devices
    if __edge_device_connected_with_iot_devices:

        # Update the iot_device_list
        if msg.topic == "iot-devices/live_state/":
            update_iot_device_list(msg)

        if msg.topic == "iot-devices/collected_data/" and __edge_device_connected_with_iot_devices:
            iot_device_id   = json.loads(msg.payload)["iot_device_id"]
            mqtt_msg_json   = json.loads(msg.payload.decode('utf8').replace('\\\\','\\'))
            byteArray       = mqtt_msg_json["byteArray"].encode('utf-8')

            write_received_data_to_file(byteArray, iot_device_id)

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

def write_received_data_to_file(byteArray, iot_device_id):
    if not os.path.isfile(__path_to_home_directory + "iot_device_id_" + str(iot_device_id) + ".csv"):
        with open(__path_to_home_directory + "iot_device_id_" + str(iot_device_id) + ".csv", "wb") as fd:
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
client.subscribe("iot-devices/#")
client.subscribe("cloud/#")
client.subscribe("simulation/#") # Topic for simulation inputs 
client.loop_start()
i = 0

# Delete existing sensordata-files for clear start
delete_files_with_end(__path_to_home_directory, ".csv")

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
                    client.publish("edge-device/send_data/", json.dumps({"iot_device_id" : need_to_collect_iot_device_ids[i]}))
                    
                    i += 1

                if len(need_to_collect_iot_device_ids) == 0:
                    print("In possession of collected data")
                    __edge_device_is_in_possession_of_collected_data            = True

            # Only publish update
            elif not __get_sensor_data and __publish_update:

                # TODO: Publish update and get an ack from iot-devices

                print("Update has succesfully published to all iot-devices")
                __edge_device_published_update_to_all_iot_devices           = True

            # Get sensor data after that publish update
            elif __get_sensor_data and __publish_update:

                # TODO: Like above

                print("In possession of collected data and the update has succesfully published to all iot-devices")
                __edge_device_is_in_possession_of_collected_data            = True
                __edge_device_published_update_to_all_iot_devices           = True
            
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

            # TODO: Send all sensor data in a while loop to cloud
            publish_data("edge-device/sensordata/", )

            # After successfully publish data to cloud reset edge-device for a new job
            reset_edge_device_attributes()

        else:
            print("Wait for instructions...")

    # Connected to iot-devices and cloud
    elif __edge_device_connected_with_iot_devices and __edge_device_connected_with_cloud:
        print("Complicated...")

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