"""
Basic Usage Example for Smart ETL System
Simple demonstration of the core functionality.
"""

import pandas as pd
import numpy as np
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_profiler import DataProfiler
from src.data_cleaner import DataCleaner
from src.feature_engineer import FeatureEngineer

def main():
    print(" SMART ETL - BASIC USAGE EXAMPLE")
    print("=" * 50)
    
    # Create simple sample data
    print("\n1. Creating Sample Data...")
    data = pd.DataFrame({
        'age': [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
        'income': [50000, 60000, 70000, 80000, 90000, 100000, 110000, 120000, 130000, 140000],
        'city': ['NYC', 'London', 'Tokyo', 'NYC', 'London', 'Tokyo', 'NYC', 'London', 'Tokyo', 'NYC'],
        'purchase_amount': [100, 150, 200, 250, 300, 350, 400, 450, 500, 550],
        'is_premium': ['Yes', 'No', 'Yes', 'No', 'Yes', 'No', 'Yes', 'No', 'Yes', 'No']
    })
    
    print(f"Original data shape: {data.shape}")
    print("Data preview:")
    print(data.head())
    
    # 1. Data Profiling
    print("\n2. Data Profiling...")
    profiler = DataProfiler()
    profile = profiler.analyze(data)
    print(profiler.generate_report())
    
    # 2. Data Cleaning
    print("\n3. Data Cleaning...")
    cleaner = DataCleaner()
    cleaned_data = cleaner.clean_data(data, profile)
    print(f"Cleaned data shape: {cleaned_data.shape}")
    print("Cleaned data preview:")
    print(cleaned_data.head())
    print(cleaner.get_cleaning_summary())
    
    # 3. Feature Engineering
    print("\n4. Feature Engineering...")
    engineer = FeatureEngineer(max_features=8)
    
    # Create a target variable
    cleaned_data['high_spender'] = (cleaned_data['purchase_amount'] > 300).astype(int)
    
    final_data = engineer.create_features(cleaned_data, 'high_spender')
    print(f"Final data shape: {final_data.shape}")
    print("Final data columns:")
    print(list(final_data.columns))
    print(engineer.get_feature_summary())
    
    print("\n BASIC EXAMPLE COMPLETED!")
    print("The data is now ready for machine learning!")

if __name__ == "__main__":
    main()