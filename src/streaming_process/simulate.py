from faker import Faker
import time 
from datetime import datetime
from kafka import KafkaProducer
import json 
import uuid
import random

class LoanSimulator:
    def __init__(self, kafka_bootstrap_servers='localhost:9092', topic='loan_application'):
        self.producer = KafkaProducer(
            bootstrap_servers = kafka_bootstrap_servers,
            # used to convert user-supplied message values to bytes
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = topic
        
        self.age_range = (18, 100)
        self.income_range = (1000, 100000)
        self.loan_amount_range = (500, 50000)
        self.credit_score_range = (300, 850)
        self.months_employed_range = (0, 600)
        self.num_credit_lines_range = (1, 20)
        self.interest_rate_range = (0, 30)
        self.loan_term_range = (6, 360)
        self.dti_ratio_range = (0, 100)
    
    def generate_loan_application(self):
        loan_data = {
            'LoanID': str(uuid.uuid4()),
            'Age': random.randint(*self.age_range),
            'Income': random.randint(*self.income_range),
            'LoanAmount': random.randint(*self.loan_amount_range),
            'CreditScore': random.randint(*self.credit_score_range),
            'MonthsEmployed': random.randint(*self.months_employed_range),
            'NumCreditLines': random.randint(*self.num_credit_lines_range),
            'InterestRate': random.uniform(*self.interest_rate_range),
            'LoanTerm': random.randint(*self.loan_term_range),
            'DTIRatio': random.uniform(*self.dti_ratio_range),
            
        }
        return loan_data

    def simulate_stream(self, interval=5, num_events=None):
        """
        Generate and send loan applications continuously
        interval: time between events in seconds
        num_events: optional limit on number of events to generate
        """
        count = 0
        try:
            while True:
                if num_events and count >= num_events:
                    break
                    
                loan_data = self.generate_loan_application()
                self.producer.send(self.topic, loan_data)
                print(f"Sent loan application {loan_data['LoanID']}")
                
                count += 1
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\nStopping simulation...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    simulator = LoanSimulator()
    simulator.simulate_stream(interval=10)
    