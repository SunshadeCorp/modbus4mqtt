FROM python:3-buster

COPY ["README.md", "setup.py", "/modbus4mqtt/"]
COPY ["./modbus4mqtt/*", "/modbus4mqtt/modbus4mqtt/"]

RUN pip install /modbus4mqtt

ENTRYPOINT ["modbus4mqtt"]
