# stream_processor.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.pipeline import PipelineModel

# Paths (match your docker-compose volumes)
INPUT_DIR = "/opt/app/data/input"
OUTPUT_DIR = "/opt/app/data/processed"
CHECKPOINT = "/tmp/checkpoint_news"
MODEL_PATH = "/opt/app/models/sentiment_pipeline"

# Global model cache (loaded on driver inside foreachBatch)
_LOADED_MODEL = None

def load_model_once():
    """Load the Spark PipelineModel once on the driver and cache it."""
    global _LOADED_MODEL
    if _LOADED_MODEL is None:
        print(f"Loading model from: {MODEL_PATH}")
        _LOADED_MODEL = PipelineModel.load(MODEL_PATH)
    return _LOADED_MODEL

# === OPTIONAL: Example UDF location (WHERE THE UDF GOES) ===
# If you have a pure-Python/sklearn model (picklable) you might register a UDF.
# NOTE: This is commented out because a Spark PipelineModel must be applied with spark transforms.
#
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType
#
# # WHERE THE UDF GOES: Example
# @udf(StringType())
# def predict_label_udf(text):
#     if text is None:
#         return "neutral"
#     # If you have a picklable model object (not a Spark PipelineModel), load it at import
#     # and use it here. Do NOT create SparkSession or access SparkContext inside this function.
#     return my_python_model_predict(text)
#
# If you're using the Spark PipelineModel (as in your train script), do NOT broadcast it or
# call SparkSession inside worker UDFs — use foreachBatch below.

def process_batch(batch_df, batch_id):
    """
    This runs on the driver for each micro-batch.
    - Loads (and caches) the Spark PipelineModel on the driver.
    - Renames incoming column to match pipeline input if needed.
    - Calls model.transform(batch_df) to produce predictions.
    - Converts numeric prediction to readable sentiment label and writes JSON files to OUTPUT_DIR.
    """
    if batch_df.rdd.isEmpty():
        # nothing to do
        return

    print(f"Processing batch {batch_id}, rows: {batch_df.count()}")

    # Ensure model is loaded on driver (and not attempted to be pickled/broadcast)
    model = load_model_once()

    # The pipeline's tokenizer expects input column "text" (as in your training).
    # Incoming schema from fetcher contains "title" and "description".
    # We'll use "title" as the text field to predict on — rename to match pipeline input.
    df_for_predict = batch_df.withColumnRenamed("title", "text")

    # Run the Spark pipeline transformation (this executes distributed transformations correctly)
    transformed = model.transform(df_for_predict)

    # PipelineModel.transform will produce a "prediction" column (double: 0.0/1.0 etc.)
    # Map numeric prediction to human label (adjust mapping if your model uses different labels)
    # Here I assume 0.0 => negative, 1.0 => positive. Add neutral mapping if required.
    result = transformed.withColumn(
        "sentiment",
        when(col("prediction") == 1.0, "positive")
        .when(col("prediction") == 0.0, "negative")
        .otherwise("neutral")
    )

    # Keep useful columns and write to JSON (append)
    out = result.select("publishedAt", "source", "text", "description", "sentiment")


    # Write out in append mode to OUTPUT_DIR. Spark will create part files (json).
    out.write.mode("append").json(OUTPUT_DIR)

    print(f"Wrote processed batch {batch_id} to {OUTPUT_DIR}")

def main():
    spark = SparkSession.builder.appName("NewsSentimentStream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Schema matching fetcher JSON (strings)
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("publishedAt", StringType(), True),
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True)
    ])

    # Read streaming JSON files (files dropped into INPUT_DIR by fetcher)
    df = spark.readStream.schema(schema).json(INPUT_DIR)

    # Use foreachBatch pattern to apply Spark ML model on driver per micro-batch
    query = (
        df.writeStream
          .foreachBatch(process_batch)
          .option("checkpointLocation", CHECKPOINT)
          .trigger(processingTime="5 seconds")
          .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
