import paho.mqtt.client as mqtt

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

    # Prints the topic and message
    print(msg.topic + " " + str(msg.payload))

client              = mqtt.Client()
client.on_connect   = on_connect
client.on_message   = on_message
client.connect("localhost", 1883, 60)
client.loop_start()

