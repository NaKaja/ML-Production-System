FROM python:3-slim
WORKDIR /app
COPY evaluation_pipeline.py /app/
COPY pipeline_manager.py /app/
RUN pip3 install -U pip wheel
RUN pip3 install pandas kafka-python minio apscheduler scikit-learn prometheus_client
CMD python3 pipeline_manager.py