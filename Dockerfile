FROM python:3.11-alpine

RUN apk --no-cache add --virtual build-deps gcc g++ make libffi-dev openssl-dev && \
    pip install setuptools ruamel.yaml~=0.16.12 click paho-mqtt pymodbus~=2.5.3 SungrowModbusTcpClient~=0.1.5
COPY ["README.md", "setup.py", "/modbus4mqtt/"]
COPY ["./modbus4mqtt/*", "/modbus4mqtt/modbus4mqtt/"]

RUN pip install /modbus4mqtt && \
    apk del build-deps

ENTRYPOINT ["modbus4mqtt"]
