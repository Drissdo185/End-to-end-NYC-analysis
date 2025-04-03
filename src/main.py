import streamlit as st
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Row

# Initialize Spark Session
spark = SparkSession.builder.appName("Loan Default Streamlit").getOrCreate()

# Load the Model
model = LogisticRegressionModel.load("/home/drissdo/Desktop/Scalable-Distributed-Systems/lr_model")

# Streamlit Interface
st.title("Loan Default Prediction")

# Input Fields
age = st.number_input("Age", min_value=18, max_value=100, value=30)
income = st.number_input("Income ($)", min_value=1000, max_value=100000, value=50000)
loan_amount = st.number_input("Loan Amount ($)", min_value=500, max_value=50000, value=10000)
credit_score = st.number_input("Credit Score", min_value=300, max_value=850, value=650)
months_employed = st.number_input("Months Employed", min_value=0, max_value=600, value=24)
num_credit_lines = st.number_input("Number of Credit Lines", min_value=1, max_value=20, value=5)
interest_rate = st.number_input("Interest Rate (%)", min_value=0.0, max_value=30.0, value=5.0)
loan_term = st.number_input("Loan Term (months)", min_value=6, max_value=360, value=60)
dti_ratio = st.number_input("Debt-to-Income Ratio (%)", min_value=0.0, max_value=100.0, value=30.0)
education = st.selectbox("Education", ['High School', 'Bachelor', 'Master', 'PhD'])
marital_status = st.selectbox("Martial Status", ['Single', 'Married', 'Divorced'])
loan_purposes = st.selectbox("Loan Purposes", ['Home', 'Other', 'Education', 'Business', 'Auto'])
has_mortagge = st.selectbox("Has Mortgage", ["Yes", "No"])
has_co_signer = st.selectbox("Has CoSigner", ["Yes", "No"])


# Predict Button
if st.button("Predict"):
    # Create DataFrame for Prediction
    input_data = spark.createDataFrame(
        [Row(
            Age=age, Income=income, LoanAmount=loan_amount, CreditScore=credit_score,
            MonthsEmployed=months_employed, NumCreditLines=num_credit_lines,
            InterestRate=interest_rate, LoanTerm=loan_term, DTIRatio=dti_ratio
        )]
    )
    
    # Assemble Features
    assembler = VectorAssembler(
        inputCols=[
            "Age", "Income", "LoanAmount", "CreditScore", "MonthsEmployed",
            "NumCreditLines", "InterestRate", "LoanTerm", "DTIRatio"
        ],
        outputCol="features"
    )
    input_data = assembler.transform(input_data)
    
    # Predict
    prediction = model.transform(input_data).select("prediction").collect()[0][0]
    
    # Display Result
    if prediction == 1.0:
        st.error("Prediction: The borrower did not repay the loan as agreed.")
    else:
        st.success("Prediction: The borrower successfully repaid the loan as per the terms of the agreement.")
        