from kafka import KafkaConsumer
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LoanPredictionConsumer") \
    .getOrCreate()

# Load the saved model
model_path = "/home/drissdo/Desktop/Scalable-Distributed-Systems/loan_default_lr_model"
lr_model = LogisticRegressionModel.load(model_path)

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'loan_application',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def preprocess_loan_data(loan_data):
    """
    Preprocess the loan data to extract features for prediction.
    The order of features must match the training data.
    """
    features = [
        loan_data['Age'],
        loan_data['Income'],
        loan_data['LoanAmount'],
        loan_data['CreditScore'],
        loan_data['MonthsEmployed'],
        loan_data['NumCreditLines'],
        loan_data['InterestRate'],
        loan_data['LoanTerm'],
        loan_data['DTIRatio']
    ]
    return Vectors.dense(features)

print("Waiting for messages...")
try:
    for message in consumer:
        loan_data = message.value
        print(f"Received loan application: {loan_data}")
        
        # Preprocess the loan data
        features = preprocess_loan_data(loan_data)
        
        # Create a Spark DataFrame for prediction
        loan_df = spark.createDataFrame([(features, )], ["features"])
        
        # Predict using the logistic regression model
        predictions = lr_model.transform(loan_df)
        
        for row in predictions.collect():
            print(f"LoanID: {loan_data['LoanID']} \n Prediction: {row.prediction}")
        if row.prediction == 1:
            print("Prediction: The borrower did not repay the loan as agreed.")
        else:
            print("Prediction: The borrower successfully repaid the loan as per the terms of the agreement.")
except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
    spark.stop()