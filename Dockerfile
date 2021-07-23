FROM python:3.8-alpine

RUN pip install pymongo requests urllib3

COPY sync.py /opt/sync.py

ENTRYPOINT ["python"]

CMD ["/opt/sync.py"]