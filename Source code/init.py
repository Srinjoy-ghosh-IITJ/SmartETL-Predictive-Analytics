"""
Smart ETL for Predictive Analysis
An AI-powered framework for automated data cleaning and feature engineering.
"""

__version__ = "1.0.0"
__author__ = "Srinjoy Ghosh"
__email__ = "m25ai1054@iitj.ac.in"

from .data_profiler import DataProfiler
from .data_cleaner import DataCleaner
from .feature_engineer import FeatureEngineer
from .pipeline_generator import PipelineGenerator

__all__ = [
    "DataProfiler",
    "DataCleaner", 
    "FeatureEngineer",
    "PipelineGenerator"
]