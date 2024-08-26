FROM python:3-slim
WORKDIR /app
COPY etl_pipeline.py /app/
COPY run.py /app/
RUN pip3 install -U pip wheel
RUN pip3 install bytewax confluent-kafka pymongo
RUN mkdir recovery
RUN python3 -m bytewax.recovery recovery/ 1
CMD python3 -m run