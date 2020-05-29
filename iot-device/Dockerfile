FROM python:3

ADD iot-device.py /
ADD requirements.txt /

RUN pip3 install -r requirements.txt

CMD [ "python", "./iot-device.py" ]
