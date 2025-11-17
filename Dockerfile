FROM python:3.10-slim

WORKDIR /opt/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY news_fetcher.py .

CMD ["python3", "news_fetcher.py"]
