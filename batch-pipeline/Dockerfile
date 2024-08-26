FROM python:3-slim
WORKDIR /app
COPY batch_pipeline.py /app/
COPY scheduler.py /app/
RUN pip3 install -U pip wheel
RUN pip3 install pandas pyarrow kafka-python minio apscheduler
CMD python3 scheduler.py