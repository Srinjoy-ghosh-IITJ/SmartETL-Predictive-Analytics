"""
Script to load and prepare sample data for Smart ETL system.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def create_sample_sales_data():
    """Create sample sales data with realistic patterns"""
    
    # Product catalog
    products = {
        201: {'name': 'Wireless Headphones', 'category': 'Electronics', 'price': 25.50},
        202: {'name': 'T-Shirt', 'category': 'Clothing', 'price': 15.75},
        203: {'name': 'Smart Watch', 'category': 'Electronics', 'price': 45.00},
        204: {'name': 'Coffee Mug', 'category': 'Home', 'price': 12.99},
        205: {'name': 'Laptop', 'category': 'Electronics', 'price': 89.99},
        206: {'name': 'Desk Lamp', 'category': 'Home', 'price': 34.50},
        207: {'name': 'Perfume', 'category': 'Beauty', 'price': 22.25},
        208: {'name': 'Jeans', 'category': 'Clothing', 'price': 18.75},
        209: {'name': 'Tablet', 'category': 'Electronics', 'price': 67.99},
        210: {'name': 'Throw Pillow', 'category': 'Home', 'price': 29.99},
        211: {'name': 'Face Cream', 'category': 'Beauty', 'price': 14.50},
        212: {'name': 'Smartphone', 'category': 'Electronics', 'price': 38.25},
        213: {'name': 'Blender', 'category': 'Home', 'price': 52.00},
        214: {'name': 'Jacket', 'category': 'Clothing', 'price': 27.75},
        215: {'name': 'Shampoo', 'category': 'Beauty', 'price': 19.99},
        216: {'name': 'Camera', 'category': 'Electronics', 'price': 41.50},
        217: {'name': 'Cookware Set', 'category': 'Home', 'price': 33.25},
        218: {'name': 'Dress', 'category': 'Clothing', 'price': 28.75},
        219: {'name': 'Makeup Kit', 'category': 'Beauty', 'price': 16.99},
        220: {'name': 'Gaming Console', 'category': 'Electronics', 'price': 75.00},
        221: {'name': 'Vacuum Cleaner', 'category': 'Home', 'price': 22.50},
        222: {'name': 'Sweater', 'category': 'Clothing', 'price': 31.75},
        223: {'name': 'Monitor', 'category': 'Electronics', 'price': 48.99},
        224: {'name': 'Body Lotion', 'category': 'Beauty', 'price': 12.25}
    }
    
    # Generate sales data
    sales_data = []
    order_id = 1
    start_date = datetime(2024, 1, 15)
    
    for i in range(100):  # 100 transactions
        product_id = np.random.choice(list(products.keys()))
        product = products[product_id]
        
        # Create realistic patterns
        customer_id = np.random.randint(101, 150)
        quantity = np.random.poisson(2) or 1  # At least 1
        order_date = start_date + timedelta(days=i)
        
        # Add some missing values and outliers for testing
        unit_price = product['price']
        if i == 5:  # Add an outlier
            unit_price = 999.99
        elif i == 10:  # Add missing value
            unit_price = np.nan
        
        sales_data.append({
            'order_id': order_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'order_date': order_date.strftime('%Y-%m-%d'),
            'quantity': quantity,
            'unit_price': unit_price,
            'customer_city': np.random.choice(['New York', 'London', 'Tokyo', 'Paris', 'Sydney']),
            'product_category': product['category'],
            'customer_segment': np.random.choice(['Premium', 'Standard'], p=[0.3, 0.7]),
            'promotion_applied': np.random.choice(['Yes', 'No'], p=[0.4, 0.6]),
            'total_sales': quantity * unit_price
        })
        
        order_id += 1
    
    return pd.DataFrame(sales_data)

def create_sample_customer_data():
    """Create sample customer data"""
    
    customer_data = []
    
    for customer_id in range(101, 151):
        age = np.random.randint(18, 65)
        income = np.random.normal(60000, 15000)
        join_date = datetime(2024, 1, 1) - timedelta(days=np.random.randint(1, 365*3))
        last_purchase = datetime(2024, 1, 1) + timedelta(days=np.random.randint(1, 60))
        
        # Add some missing values for testing
        credit_score = np.random.randint(600, 850)
        if customer_id % 15 == 0:  # Every 15th customer has missing credit score
            credit_score = np.nan
        
        customer_data.append({
            'customer_id': customer_id,
            'age': age,
            'income': max(20000, income),  # Ensure minimum income
            'join_date': join_date.strftime('%Y-%m-%d'),
            'last_purchase': last_purchase.strftime('%Y-%m-%d'),
            'city': np.random.choice(['New York', 'London', 'Tokyo', 'Paris', 'Sydney']),
            'country': np.random.choice(['USA', 'UK', 'Japan', 'France', 'Australia']),
            'credit_score': credit_score,
            'is_active': np.random.choice(['Yes', 'No'], p=[0.9, 0.1])
        })
    
    return pd.DataFrame(customer_data)

def main():
    """Main function to generate sample data"""
    print("Generating sample data for Smart ETL system...")
    
    # Create data directory if it doesn't exist
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    
    # Generate sample data
    sales_df = create_sample_sales_data()
    customer_df = create_sample_customer_data()
    
    # Save to CSV files
    sales_df.to_csv('data/raw/sample_sales_data.csv', index=False)
    customer_df.to_csv('data/raw/sample_customer_data.csv', index=False)
    
    print(f" Generated sales data: {sales_df.shape}")
    print(f" Generated customer data: {customer_df.shape}")
    print("📁 Files saved to data/raw/")
    print("   - sample_sales_data.csv")
    print("   - sample_customer_data.csv")

if __name__ == "__main__":
    main()