import pandas as pd
import logging
import os
from datetime import datetime

# Professional logging setup
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class DataAutomationPipeline:
    """
    Professional workflow for cleaning messy datasets.
    Matches CV: Automated preprocessing and data standardization.
    """
    def __init__(self, input_filename="data.csv"):
        self.input_filename = input_filename
        self.df = None

    def create_fake_data(self):
        """Generates a dummy file to demonstrate the script's capability"""
        data = {
            'User Name': ['Alice ', 'Bob', 'Alice ', 'Charlie', None],
            'Transaction Date': ['2023-01-01', '2023/01/02', '2023-01-01', '03-01-2023', '2023-01-04'],
            'Amount': [100.50, 200.00, 100.50, 150.75, 50.00]
        }
        pd.DataFrame(data).to_csv(self.input_filename, index=False)
        logging.info(f"Created fake dataset: {self.input_filename}")

    def run_cleaning_pipeline(self):
        if not os.path.exists(self.input_filename):
            self.create_fake_data()

        self.df = pd.read_csv(self.input_filename)
        logging.info("Starting automation workflow...")

        # 1. Remove Whitespace & Standardize Column Names
        self.df.columns = [c.strip().lower().replace(" ", "_") for c in self.df.columns]

        # 2. Deduplication (Matches CV Efficiency claim)
        initial_count = len(self.df)
        self.df.drop_duplicates(inplace=True)
        logging.info(f"Removed {initial_count - len(self.df)} duplicate rows.")

        # 3. Handle Missing Values
        self.df.fillna({'user_name': 'Unknown'}, inplace=True)

        # 4. Export Cleaned Data
        output_name = f"cleaned_data_{datetime.now().strftime('%Y%m%d')}.csv"
        self.df.to_csv(output_name, index=False)
        logging.info(f"Cleaned data successfully exported to: {output_name}")

if __name__ == "__main__":
    pipeline = DataAutomationPipeline()
    pipeline.run_cleaning_pipeline()
