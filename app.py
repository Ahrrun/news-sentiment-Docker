import os
import json
import pandas as pd
import streamlit as st

DATA_DIR = "/opt/app/data/processed"

st.title("Real-Time News Sentiment Dashboard")

records = []

# Load all Spark output JSON files
if os.path.exists(DATA_DIR):
    for f in os.listdir(DATA_DIR):
        if f.endswith(".json"):
            records = []

            if os.path.exists(DATA_DIR):
                for f in os.listdir(DATA_DIR):
                    if f.endswith(".json"):
                        try:
                            df_part = pd.read_json(os.path.join(DATA_DIR, f), lines=True)
                            records.extend(df_part.to_dict(orient='records'))
                        except Exception as e:
                            print("Error reading file:", f, e)

if len(records) == 0:
    st.warning("Waiting for news data...")
else:
    df = pd.DataFrame(records)

    # Ensure missing columns exist
    for col in ["publishedAt", "source", "title", "sentiment"]:
        if col not in df.columns:
            df[col] = None

    st.subheader("Latest Articles")
    st.dataframe(df[["publishedAt", "source", "title", "sentiment"]].tail(20))

    st.subheader("Sentiment Distribution")
    st.bar_chart(df["sentiment"].value_counts())
