FROM apache/spark:3.5.1
USER root
WORKDIR /opt/app

COPY requirements.txt .

# make pip less likely to timeout and avoid cache bloat
ENV PIP_DEFAULT_TIMEOUT=120
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt

COPY train_model.py .
COPY stream_processor.py .

CMD ["python3", "stream_processor.py"]
