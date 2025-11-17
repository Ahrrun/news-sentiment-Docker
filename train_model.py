from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.pipeline import PipelineModel

spark = (
    SparkSession.builder
    .appName("train-sentiment")
    .getOrCreate()
)


# IMPORTANT — correct path for Docker container
train_path = "/opt/app/data/labeled_train.csv"

df = spark.read.csv(train_path, header=True, inferSchema=True).select("text", "label")
df = df.na.drop()

tokenizer = RegexTokenizer(inputCol="text", outputCol="tokens", pattern="\\W")
remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
hashing = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1<<16)
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)

pipeline = Pipeline(stages=[tokenizer, remover, hashing, idf, lr])
model = pipeline.fit(df)

model_path = "models/sentiment_pipeline"
model.write().overwrite().save(model_path)

print("Saved model to:", model_path)

# Load trained pipeline for streaming predictions


def predict_sentiment(text):
    """
    Predict sentiment label (0/1) for a single input string.
    Used inside Spark Structured Streaming UDF.
    """
    if text is None:
        return "neutral"

    from pyspark.sql import Row
    temp_df = spark.createDataFrame([Row(text=text)])
    result = loaded_model.transform(temp_df).select("prediction").collect()[0][0]
    return str(float(result))  # Return as string for JSON writing

spark.stop()
