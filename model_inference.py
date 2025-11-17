from pyspark.ml.pipeline import PipelineModel

MODEL_PATH = "/opt/app/models/sentiment_pipeline"
model = None

def predict_sentiment(text):
    global model
    if model is None:
        model = PipelineModel.load(MODEL_PATH)
    if text is None:
        return "neutral"
    df = model.sparkSession.createDataFrame([(text,)], ["text"])
    prediction = model.transform(df).select("prediction").first()[0]
    return str(prediction)
