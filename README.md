# StoreAndForward
Practical implementation of an edge computing pattern wich was designed for an iot problem where iot-devices located in hard to reach areas

## Start
For starting the simulation we recommend to start the simulation localy over the python scripts. The docker simulation shows undetected failure. We will work on this to fix this.

### Start over python scripts (StoreAndForward/presentation/...)
1. First u need to change the path to the home directory in each python script.

2. After that u can run the python scripts in the terminal with:

```
/usr/bin/python3 /<folder>/<python.py> 
```

Important is that first u must start the iot-devices from 0-3. Than u must start the edge-device. After that the simulation.py and at last the cloud.py must be started.

3. In cloud.py u can change the job that the edge-device is going to do or the target groups that will be addressed.

3. The simulation script will offer you an input for the place where the edge-device is. Use this input for letting fly the edge-device to the iot-device or the cloud.

