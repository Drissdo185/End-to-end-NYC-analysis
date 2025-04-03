from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.sql.functions import col
from pyspark.ml import Pipeline



def preprocess_data(df):
        imputer = Imputer(
        inputCols=["Income", "MonthsEmployed", "NumCreditLines", "InterestRate", "LoanTerm", "DTIRatio"],
        outputCols=["Income_filled", "MonthsEmployed_filled", "NumCreditLines_filled", "InterestRate_filled", "LoanTerm_filled", "DTIRatio_filled"]
    )
        df_imputed = imputer.fit(df).transform(df)

        string_indexers = [
            StringIndexer(inputCol="Education", outputCol="Education_index"),
            StringIndexer(inputCol="EmploymentType", outputCol="EmploymentType_index"),
            StringIndexer(inputCol="MaritalStatus", outputCol="MaritalStatus_index"),
            StringIndexer(inputCol="HasMortgage", outputCol="HasMortgage_index"),
            StringIndexer(inputCol="HasDependents", outputCol="HasDependents_index"),
            StringIndexer(inputCol="LoanPurpose", outputCol="LoanPurpose_index"),
            StringIndexer(inputCol="HasCoSigner", outputCol="HasCoSigner_index")]
        
        pipeline_indexers = Pipeline(stages=string_indexers)
        df_indexed = pipeline_indexers.fit(df_imputed).transform(df_imputed)


        # scale data
        one_hot_encoders = [
        OneHotEncoder(inputCol="Education_index", outputCol="Education_vec"),
        OneHotEncoder(inputCol="EmploymentType_index", outputCol="EmploymentType_vec"),
        OneHotEncoder(inputCol="MaritalStatus_index", outputCol="MaritalStatus_vec"),
        OneHotEncoder(inputCol="HasMortgage_index", outputCol="HasMortgage_vec"),
        OneHotEncoder(inputCol="HasDependents_index", outputCol="HasDependents_vec"),
        OneHotEncoder(inputCol="LoanPurpose_index", outputCol="LoanPurpose_vec"),
        OneHotEncoder(inputCol="HasCoSigner_index", outputCol="HasCoSigner_vec")
    ]

        pipeline_encoders = Pipeline(stages=one_hot_encoders)
        df_encoded = pipeline_encoders.fit(df_indexed).transform(df_indexed)

        # Normalize numerical features
        numerical_cols = [
            "Age", "Income_filled", "LoanAmount", "CreditScore", "MonthsEmployed_filled",
            "NumCreditLines_filled", "InterestRate_filled", "LoanTerm_filled", "DTIRatio_filled"
        ]

        assembler = VectorAssembler(inputCols=numerical_cols, outputCol="features")
        df_assembled = assembler.transform(df_encoded)

        scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)

        scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)

        df_scaled = df_scaled.select(["LoanID", "scaled_features"])

        return df_scaled
