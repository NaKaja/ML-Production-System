FROM python:3-slim
WORKDIR /app
COPY batch_training_pipeline.py /app/
COPY scheduler.py /app/
RUN pip3 install -U pip wheel
RUN pip3 install pandas pyarrow minio apscheduler scikit-learn prometheus_client
CMD python3 scheduler.py