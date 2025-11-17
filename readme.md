better use the zip file and unzip and do the rest !!
news-sentiment/
│
├── docker-compose.yml
├── .env
│
├── data/
│   ├── input/        # Raw NDJSON files from fetcher
│   ├── processed/    # Spark processed sentiment output
│   └── models/       # If storing ML models outside containers (optional)
│
├── fetcher/
│   ├── Dockerfile
│   ├── news_fetcher.py
│   ├── requirements.txt
│   └── __init__.py
│
├── spark/
│   ├── Dockerfile
│   ├── stream_processor.py
│   ├── train_pipeline.py     # If you trained pipeline inside this project
│   ├── requirements.txt
│   └── models/
│       └── sentiment_pipeline/   # Exported Spark ML pipeline
│
├── dashboard/
│   ├── Dockerfile
│   ├── app.py         # Streamlit UI
│   └── requirements.txt
│
├── scripts/
│   ├── build_all.sh    # (Optional) build all docker images at once
│   ├── push_all.sh     # (Optional) push all images to docker hub
│   └── clean.sh        # (Optional) reset data & logs
│
├── docs/
│   ├── architecture.png
│   ├── pipeline_diagram.png


