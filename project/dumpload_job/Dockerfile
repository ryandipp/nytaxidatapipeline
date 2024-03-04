FROM python:3.9

WORKDIR /app

RUN apt-get update && apt-get install -y postgresql-client

COPY dump_load.py /app/dump_load.py

CMD ["python", "dump_load.py"]