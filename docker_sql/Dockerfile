FROM python:3.9

RUN pip install pandas pyarrow psycopg2 sqlalchemy

WORKDIR /app

COPY ingest_data.py ingest_data.py

COPY add_taxi_zone_lookup_file.py add_taxi_zone_lookup_file.py

COPY entrypoint.py entrypoint.py

COPY config.py config.py

ENTRYPOINT [ "python", "entrypoint.py"]