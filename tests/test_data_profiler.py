"""
Tests for Data Profiler Module
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_profiler import DataProfiler

class TestDataProfiler:
    @pytest.fixture
    def sample_data(self):
        """Create sample test data"""
        data = pd.DataFrame({
            'numerical_1': [1, 2, 3, 4, 5, np.nan, 7, 8, 9, 10],
            'numerical_2': [10.5, 20.3, 30.1, 40.7, 50.2, 60.8, 70.4, 80.9, 90.1, 100.5],
            'categorical': ['A', 'B', 'A', 'C', 'B', 'A', 'C', 'B', 'A', 'C'],
            'text': ['hello', 'world', 'test', 'data', 'science', 'ai', 'ml', 'etl', 'smart', 'feature']
        })
        return data
    
    def test_profiler_initialization(self):
        """Test that profiler initializes correctly"""
        profiler = DataProfiler()
        assert profiler.profile_report == {}
        assert profiler.data_types == {}
    
    def test_data_analysis(self, sample_data):
        """Test comprehensive data analysis"""
        profiler = DataProfiler()
        profile = profiler.analyze(sample_data)
        
        assert 'overview' in profile
        assert 'data_types' in profile
        assert 'missing_values' in profile
        assert 'statistical_summary' in profile
        assert 'quality_metrics' in profile
    
    def test_data_type_inference(self, sample_data):
        """Test intelligent data type inference"""
        profiler = DataProfiler()
        profile = profiler.analyze(sample_data)
        data_types = profile['data_types']
        
        assert 'numerical' in data_types['numerical_1']
        assert 'numerical' in data_types['numerical_2'] 
        assert 'categorical' in data_types['categorical']
        assert 'text' in data_types['text']
    
    def test_missing_value_analysis(self, sample_data):
        """Test missing value detection"""
        profiler = DataProfiler()
        profile = profiler.analyze(sample_data)
        missing_info = profile['missing_values']
        
        assert missing_info['total_missing'] == 1
        assert 'numerical_1' in missing_info['columns_missing']
    
    def test_quality_metrics(self, sample_data):
        """Test quality metric calculation"""
        profiler = DataProfiler()
        profile = profiler.analyze(sample_data)
        quality_metrics = profile['quality_metrics']
        
        assert 'overall_quality_score' in quality_metrics
        assert 'completeness_score' in quality_metrics
        assert 'uniqueness_score' in quality_metrics
        assert 0 <= quality_metrics['overall_quality_score'] <= 100

if __name__ == "__main__":
    pytest.main([__file__, "-v"])