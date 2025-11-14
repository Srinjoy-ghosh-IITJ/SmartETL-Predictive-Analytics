"""
Demo script showing how to process raw data using Smart ETL system.
"""

import pandas as pd
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_profiler import DataProfiler
from src.data_cleaner import DataCleaner
from src.feature_engineer import FeatureEngineer

def process_sales_data():
    """Demo: Process sales data using Smart ETL"""
    print("PROCESSING SALES DATA WITH SMART ETL")
    print("=" * 50)
    
    # Load raw data
    print("1. Loading raw data...")
    sales_data = pd.read_csv('data/raw/sample_sales_data.csv')
    print(f"   Raw data shape: {sales_data.shape}")
    
    # Data Profiling
    print("\n2. Data Profiling...")
    profiler = DataProfiler()
    profile = profiler.analyze(sales_data)
    print(profiler.generate_report())
    
    # Data Cleaning
    print("\n3. Data Cleaning...")
    cleaner = DataCleaner()
    cleaned_data = cleaner.clean_data(sales_data, profile)
    print(f" Cleaned data shape: {cleaned_data.shape}")
    print(cleaner.get_cleaning_summary())
    
    # Feature Engineering
    print("\n4. Feature Engineering...")
    engineer = FeatureEngineer(max_features=20)
    
    # Create target variable for demonstration (total sales > 100)
    cleaned_data['high_value_sale'] = (cleaned_data['quantity'] * cleaned_data['unit_price']) > 100
    
    engineered_data = engineer.create_features(cleaned_data, 'high_value_sale')
    print(f"   Engineered data shape: {engineered_data.shape}")
    print(engineer.get_feature_summary())
    
    # Save processed data
    print("\n5. Saving processed data...")
    engineered_data.to_csv('data/processed/processed_sales_data.csv', index=False)
    print(" Processed data saved to: data/processed/processed_sales_data.csv")
    
    return engineered_data

def process_customer_data():
    """Demo: Process customer data using Smart ETL"""
    print("\n PROCESSING CUSTOMER DATA WITH SMART ETL")
    print("=" * 50)
    
    # Load raw data
    print("1. Loading raw data...")
    customer_data = pd.read_csv('data/raw/sample_customer_data.csv')
    print(f" Raw data shape: {customer_data.shape}")
    
    # Data Profiling
    print("\n2. Data Profiling...")
    profiler = DataProfiler()
    profile = profiler.analyze(customer_data)
    print(profiler.generate_report())
    
    # Data Cleaning
    print("\n3. Data Cleaning...")
    cleaner = DataCleaner()
    cleaned_data = cleaner.clean_data(customer_data, profile)
    print(f"   Cleaned data shape: {cleaned_data.shape}")
    print(cleaner.get_cleaning_summary())
    
    # Feature Engineering
    print("\n4. Feature Engineering...")
    engineer = FeatureEngineer(max_features=15)
    
    # Convert dates for feature engineering
    cleaned_data['join_date'] = pd.to_datetime(cleaned_data['join_date'])
    cleaned_data['last_purchase'] = pd.to_datetime(cleaned_data['last_purchase'])
    
    engineered_data = engineer.create_features(cleaned_data, 'credit_score')
    print(f"   Engineered data shape: {engineered_data.shape}")
    print(engineer.get_feature_summary())
    
    # Save processed data
    print("\n5. Saving processed data...")
    engineered_data.to_csv('data/processed/processed_customer_data.csv', index=False)
    print(" Processed data saved to: data/processed/processed_customer_data.csv")
    
    return engineered_data

def main():
    """Main demo function"""
    print("SMART ETL DATA PROCESSING DEMO")
    print("This demo shows how to process raw data using the Smart ETL system")
    print("=" * 60)
    
    # Process both datasets
    processed_sales = process_sales_data()
    processed_customers = process_customer_data()
    
    print("\n" + "=" * 60)
    print("DEMO COMPLETED SUCCESSFULLY!")
    print(f"Processed sales data: {processed_sales.shape}")
    print(f" Processed customer data: {processed_customers.shape}")
    print("\n Generated files:")
    print("   - data/processed/processed_sales_data.csv")
    print("   - data/processed/processed_customer_data.csv")
    print("\n The data is now ready for machine learning!")

if __name__ == "__main__":
    main()