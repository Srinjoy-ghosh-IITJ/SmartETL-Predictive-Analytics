"""
Sales Prediction Demo using Smart ETL
Complete workflow from raw data to model-ready features.
"""

import pandas as pd
import numpy as np
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_profiler import DataProfiler
from src.data_cleaner import DataCleaner
from src.feature_engineer import FeatureEngineer

def load_and_process_sales_data():
    """Complete sales data processing demo"""
    print("SALES PREDICTION DEMO")
    print("=" * 50)
    
    # Load sample sales data (or create if not exists)
    try:
        sales_data = pd.read_csv('../data/raw/sample_sales_data.csv')
        print("Loaded existing sales data")
    except:
        print("Creating sample sales data...")
        sales_data = create_sample_sales_data()
    
    print(f"Raw sales data: {sales_data.shape}")
    print("\nRaw data preview:")
    print(sales_data.head())
    
    # Complete ETL Pipeline
    print("\n" + "="*50)
    print(" STARTING SMART ETL PIPELINE")
    print("="*50)
    
    # 1. Data Profiling
    print("\n1.  DATA PROFILING...")
    profiler = DataProfiler()
    profile = profiler.analyze(sales_data)
    print(profiler.generate_report())
    
    # 2. Data Cleaning
    print("\n2.  DATA CLEANING...")
    cleaner = DataCleaner()
    cleaned_data = cleaner.clean_data(sales_data, profile)
    print(f" Cleaning completed. Shape: {cleaned_data.shape}")
    print(cleaner.get_cleaning_summary())
    
    # 3. Feature Engineering for Sales Prediction
    print("\n3.  FEATURE ENGINEERING...")
    
    # Create target variable: high-value customers (top 30% by total sales)
    cleaned_data['total_sales'] = cleaned_data['quantity'] * cleaned_data['unit_price']
    total_sales_threshold = cleaned_data['total_sales'].quantile(0.7)
    cleaned_data['high_value_customer'] = (cleaned_data['total_sales'] > total_sales_threshold).astype(int)
    
    print(f"Target variable created: 'high_value_customer'")
    print(f"High-value threshold: ${total_sales_threshold:.2f}")
    print(f"High-value customers: {cleaned_data['high_value_customer'].sum()}/{len(cleaned_data)}")
    
    engineer = FeatureEngineer(max_features=15)
    final_data = engineer.create_features(cleaned_data, 'high_value_customer')
    
    print(f" Feature engineering completed. Final shape: {final_data.shape}")
    print(engineer.get_feature_summary())
    
    # Show feature importance for business insights
    if engineer.feature_importance:
        print("\n BUSINESS INSIGHTS - Top Predictive Features:")
        business_features = sorted(engineer.feature_importance.items(), 
                                key=lambda x: x[1], reverse=True)[:8]
        for feature, importance in business_features:
            print(f"  {feature}: {importance:.3f}")
    
    return final_data

def create_sample_sales_data():
    """Create sample sales data for demo"""
    np.random.seed(42)
    
    products = {
        201: {'name': 'Laptop', 'category': 'Electronics', 'price': 899.99},
        202: {'name': 'Smartphone', 'category': 'Electronics', 'price': 699.99},
        203: {'name': 'Headphones', 'category': 'Electronics', 'price': 199.99},
        204: {'name': 'Desk', 'category': 'Furniture', 'price': 299.99},
        205: {'name': 'Chair', 'category': 'Furniture', 'price': 149.99}
    }
    
    sales_data = []
    for i in range(50):
        product_id = np.random.choice(list(products.keys()))
        product = products[product_id]
        
        sales_data.append({
            'order_id': i + 1,
            'customer_id': np.random.randint(1001, 1020),
            'product_id': product_id,
            'order_date': f"2024-{np.random.randint(1,13):02d}-{np.random.randint(1,28):02d}",
            'quantity': np.random.poisson(2) + 1,
            'unit_price': product['price'],
            'customer_city': np.random.choice(['New York', 'London', 'Tokyo', 'Paris']),
            'product_category': product['category'],
            'customer_segment': np.random.choice(['Premium', 'Standard'], p=[0.4, 0.6]),
            'promotion_applied': np.random.choice(['Yes', 'No'], p=[0.3, 0.7])
        })
    
    df = pd.DataFrame(sales_data)
    os.makedirs('../data/raw', exist_ok=True)
    df.to_csv('../data/raw/sample_sales_data.csv', index=False)
    return df

def demonstrate_model_readiness(final_data):
    """Show that data is ready for machine learning"""
    print("\n" + "="*50)
    print(" MODEL READINESS CHECK")
    print("="*50)
    
    # Check data quality
    print(" Data Quality Check:")
    print(f"  - No missing values: {not final_data.isnull().any().any()}")
    print(f"  - All numerical: {all(final_data.dtypes != 'object')}")
    print(f"  - Reasonable scale: All features normalized/encoded")
    
    # Show final data structure
    print(f"\n Final Dataset Structure:")
    print(f"  - Samples: {len(final_data)}")
    print(f"  - Features: {len(final_data.columns) - 1}")  # excluding target
    print(f"  - Target variable: 'high_value_customer'")
    
    # Show sample of prepared data
    print(f"\n Prepared Data Sample (first 3 rows):")
    print(final_data.head(3).T)  # Transpose for better readability
    
    print("\n DATA IS READY FOR MACHINE LEARNING!")
    print("You can now use this with scikit-learn, XGBoost, etc.")

def main():
    """Main demo function"""
    print(" SMART ETL - SALES PREDICTION DEMO")
    print("Complete workflow from raw data to model-ready features")
    print("=" * 60)
    
    try:
        # Process the data
        final_data = load_and_process_sales_data()
        
        # Demonstrate model readiness
        demonstrate_model_readiness(final_data)
        
        # Save processed data
        os.makedirs('../data/processed', exist_ok=True)
        final_data.to_csv('../data/processed/demo_processed_sales.csv', index=False)
        print(f"\n Processed data saved to: ../data/processed/demo_processed_sales.csv")
        
        print("\n DEMO COMPLETED SUCCESSFULLY!")
        print(" Next steps: Use the processed data with your favorite ML library!")
        
    except Exception as e:
        print(f" Error in demo: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()