"""
Helper functions for Smart ETL system.
"""

import pandas as pd
import numpy as np
import pickle
import os
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

def validate_dataframe(df: pd.DataFrame, 
                      required_columns: Optional[List[str]] = None,
                      min_rows: int = 1) -> bool:
    """
    Validate DataFrame structure and content.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        min_rows: Minimum number of rows required
        
    Returns:
        True if validation passes, False otherwise
    """
    try:
        # Check if it's a DataFrame
        if not isinstance(df, pd.DataFrame):
            print("Input is not a pandas DataFrame")
            return False
        
        # Check minimum rows
        if len(df) < min_rows:
            print(f"DataFrame has fewer than {min_rows} rows")
            return False
        
        # Check required columns
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"Missing required columns: {missing_columns}")
                return False
        
        # Check for completely empty DataFrame
        if df.empty:
            print("DataFrame is completely empty")
            return False
        
        print("DataFrame validation passed")
        return True
        
    except Exception as e:
        print(f"DataFrame validation failed: {e}")
        return False

def save_pipeline(pipeline: Any, filepath: str) -> bool:
    """
    Save pipeline object to file.
    
    Args:
        pipeline: Pipeline object to save
        filepath: Path where to save the pipeline
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'wb') as file:
            pickle.dump(pipeline, file)
        
        print(f"Pipeline saved to {filepath}")
        return True
        
    except Exception as e:
        print(f"Failed to save pipeline: {e}")
        return False

def load_pipeline(filepath: str) -> Optional[Any]:
    """
    Load pipeline object from file.
    
    Args:
        filepath: Path to pipeline file
        
    Returns:
        Loaded pipeline object or None if failed
    """
    try:
        if not os.path.exists(filepath):
            print(f"Pipeline file not found: {filepath}")
            return None
        
        with open(filepath, 'rb') as file:
            pipeline = pickle.load(file)
        
        print(f"Pipeline loaded from {filepath}")
        return pipeline
        
    except Exception as e:
        print(f"Failed to load pipeline: {e}")
        return None

def get_memory_usage(df: pd.DataFrame) -> Dict[str, str]:
    """
    Get memory usage statistics for DataFrame.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with memory usage information
    """
    try:
        memory_bytes = df.memory_usage(deep=True).sum()
        
        return {
            'bytes': memory_bytes,
            'human_readable': format_bytes(memory_bytes),
            'per_column': {col: format_bytes(df[col].memory_usage(deep=True)) 
                          for col in df.columns}
        }
        
    except Exception as e:
        print(f"Failed to calculate memory usage: {e}")
        return {'bytes': 0, 'human_readable': '0B', 'per_column': {}}

def format_bytes(size: float) -> str:
    """
    Convert bytes to human-readable format.
    
    Args:
        size: Size in bytes
        
    Returns:
        Human-readable string
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def timer(func):
    """
    Decorator to measure function execution time.
    
    Args:
        func: Function to time
        
    Returns:
        Wrapped function
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{func.__name__} executed in {execution_time:.2f} seconds")
        return result
    return wrapper

def generate_report_filename(base_name: str = "smart_etl_report") -> str:
    """
    Generate a timestamped report filename.
    
    Args:
        base_name: Base name for the report
        
    Returns:
        Generated filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{timestamp}.html"

def check_column_uniqueness(df: pd.DataFrame, threshold: float = 0.9) -> List[str]:
    """
    Identify columns with high uniqueness (potential IDs).
    
    Args:
        df: DataFrame to check
        threshold: Uniqueness threshold (0.0 to 1.0)
        
    Returns:
        List of column names with high uniqueness
    """
    high_uniqueness_cols = []
    
    for column in df.columns:
        unique_ratio = df[column].nunique() / len(df)
        if unique_ratio >= threshold:
            high_uniqueness_cols.append(column)
    
    return high_uniqueness_cols

def safe_divide(numerator: pd.Series, denominator: pd.Series, default: float = 0.0) -> pd.Series:
    """
    Safe division handling division by zero.
    
    Args:
        numerator: Numerator series
        denominator: Denominator series
        default: Default value when division by zero
        
    Returns:
        Result series
    """
    return np.where(denominator != 0, numerator / denominator, default)

def get_dataframe_info(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get comprehensive DataFrame information.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with DataFrame information
    """
    info = {
        'shape': df.shape,
        'columns': list(df.columns),
        'data_types': df.dtypes.to_dict(),
        'memory_usage': get_memory_usage(df),
        'missing_values': df.isnull().sum().to_dict(),
        'missing_percentage': (df.isnull().sum() / len(df) * 100).to_dict(),
        'duplicate_rows': df.duplicated().sum(),
        'numeric_columns': df.select_dtypes(include=[np.number]).columns.tolist(),
        'categorical_columns': df.select_dtypes(include=['object']).columns.tolist(),
        'datetime_columns': df.select_dtypes(include=['datetime64']).columns.tolist()
    }
    
    return info

def print_dataframe_summary(df: pd.DataFrame, name: str = "DataFrame") -> None:
    """
    Print a comprehensive summary of DataFrame.
    
    Args:
        df: DataFrame to summarize
        name: Name for the DataFrame
    """
    info = get_dataframe_info(df)
    
    print(f"\n{'='*50}")
    print(f"{name} SUMMARY")
    print(f"{'='*50}")
    print(f"Shape: {info['shape']}")
    print(f"Memory: {info['memory_usage']['human_readable']}")
    print(f"Duplicate rows: {info['duplicate_rows']}")
    
    print(f"\nData Types:")
    for col, dtype in info['data_types'].items():
        print(f"  {col}: {dtype}")
    
    print(f"\nMissing Values:")
    for col, missing in info['missing_values'].items():
        if missing > 0:
            print(f"  {col}: {missing} ({info['missing_percentage'][col]:.1f}%)")