"""
Advanced Usage Example for Smart ETL System
Demonstrates advanced features and customization.
"""

import pandas as pd
import numpy as np
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_profiler import DataProfiler
from src.data_cleaner import DataCleaner
from src.feature_engineer import FeatureEngineer
from src.pipeline_generator import PipelineGenerator
from src.utils import Config

def main():
    print(" SMART ETL - ADVANCED USAGE EXAMPLE")
    print("=" * 50)
    
    # Create complex sample data with realistic patterns
    print("\n1. Creating Advanced Sample Data...")
    np.random.seed(42)
    
    dates = pd.date_range('2023-01-01', periods=100, freq='D')
    data = pd.DataFrame({
        'transaction_id': range(1, 101),
        'customer_id': np.random.randint(1001, 1020, 100),
        'product_id': np.random.choice(['A001', 'A002', 'A003', 'B001', 'B002'], 100),
        'transaction_date': dates,
        'amount': np.random.normal(100, 30, 100),
        'quantity': np.random.poisson(3, 100),
        'customer_segment': np.random.choice(['Premium', 'Standard', 'Basic'], 100, p=[0.2, 0.5, 0.3]),
        'region': np.random.choice(['North', 'South', 'East', 'West'], 100),
        'weekday': dates.day_name(),
        # Add some missing values and outliers for demonstration
        'discount': np.where(np.random.random(100) > 0.1, np.random.uniform(0, 0.3, 100), np.nan),
        'rating': np.where(np.random.random(100) > 0.05, np.random.randint(1, 6, 100), 999)  # outliers
    })
    
    print(f"Advanced data shape: {data.shape}")
    print("Data preview:")
    print(data.head())
    
    # Custom configuration
    print("\n2. Custom Configuration...")
    config = Config()
    config.set('feature_engineering.max_features', 12)
    config.set('data_cleaning.outlier_threshold', 2.0)
    config.show_config()
    
    # 1. Advanced Data Profiling
    print("\n3.  Advanced Data Profiling...")
    profiler = DataProfiler()
    profile = profiler.analyze(data)
    
    # Show specific insights
    print("Data Types Found:")
    for col, dtype in profile['data_types'].items():
        print(f"  {col}: {dtype}")
    
    print(f"\nMissing Values Summary:")
    missing_info = profile['missing_values']
    print(f"  Total missing cells: {missing_info['total_missing']}")
    print(f"  Missing percentage: {missing_info['missing_percentage_total']:.2f}%")
    
    # 2. Advanced Data Cleaning
    print("\n4.  Advanced Data Cleaning...")
    cleaner = DataCleaner()
    cleaned_data = cleaner.clean_data(data, profile)
    print(f"Cleaned data shape: {cleaned_data.shape}")
    print("Cleaning operations applied:")
    print(f"  - Imputation strategies: {len(cleaner.imputation_strategies)}")
    print(f"  - Encoded variables: {len(cleaner.encoders)}")
    
    # 3. Advanced Feature Engineering
    print("\n5. Advanced Feature Engineering...")
    engineer = FeatureEngineer(max_features=12)
    
    # Create multiple target scenarios
    cleaned_data['large_transaction'] = (cleaned_data['amount'] > 120).astype(int)
    cleaned_data['high_quantity'] = (cleaned_data['quantity'] > 3).astype(int)
    
    engineered_data = engineer.create_features(cleaned_data, 'large_transaction')
    print(f"Engineered data shape: {engineered_data.shape}")
    print(f"Features created: {len(engineer.created_features)}")
    print(f"Features selected: {len(engineer.selected_features)}")
    
    # Show top features by importance
    if engineer.feature_importance:
        print("\nTop 5 Most Important Features:")
        top_features = sorted(engineer.feature_importance.items(), 
                           key=lambda x: x[1], reverse=True)[:5]
        for feature, importance in top_features:
            print(f"  {feature}: {importance:.4f}")
    
    # 4. Pipeline Generation
    print("\n6. Pipeline Generation...")
    pipeline = PipelineGenerator()
    
    # Record all steps
    pipeline.add_step(
        "Data Profiling",
        "data_profiling",
        {"columns_analyzed": len(data.columns), "rows_analyzed": len(data)}
    )
    
    pipeline.add_step(
        "Data Cleaning", 
        "data_cleaning",
        {"imputation_applied": len(cleaner.imputation_strategies)}
    )
    
    pipeline.add_step(
        "Feature Engineering",
        "feature_engineering", 
        {"features_created": len(engineer.created_features)}
    )
    
    # Generate pipeline report
    print(pipeline.generate_report())
    
    # Save the pipeline
    pipeline.save_pipeline("advanced_pipeline.py", format="python")
    print("Pipeline saved as 'advanced_pipeline.py'")
    
    print("\n ADVANCED EXAMPLE COMPLETED!")
    print("All advanced features demonstrated successfully!")

if __name__ == "__main__":
    main()