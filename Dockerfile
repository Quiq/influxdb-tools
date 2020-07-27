FROM python:3.8

RUN pip install requirments.txt

COPY . /

ENTRYPOINT /influx-backup.py