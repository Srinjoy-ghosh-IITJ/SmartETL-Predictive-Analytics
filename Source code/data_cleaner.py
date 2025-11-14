"""
Smart ETL - Data Profiling Module
Automated data analysis and profiling for intelligent ETL processing.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List
import warnings
warnings.filterwarnings('ignore')

class DataProfiler:
    def __init__(self):
        self.profile_report = {}
        self.data_types = {}
    
    def analyze(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Comprehensive data analysis and profiling
        
        Args:
            data: Input DataFrame for profiling
            
        Returns:
            Dictionary containing complete profile report
        """
        print("Starting data profiling...")
        
        profile = {
            'overview': self._get_overview(data),
            'data_types': self._infer_data_types(data),
            'missing_values': self._analyze_missing_values(data),
            'statistical_summary': self._generate_statistical_summary(data),
            'quality_metrics': self._calculate_quality_metrics(data)
        }
        
        self.profile_report = profile
        print(" Data profiling completed!")
        return profile
    
    def _get_overview(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Get basic dataset overview"""
        return {
            'num_rows': data.shape[0],
            'num_columns': data.shape[1],
            'memory_usage': f"{data.memory_usage(deep=True).sum() / 1024**2:.2f} MB",
            'duplicate_rows': data.duplicated().sum()
        }
    
    def _infer_data_types(self, data: pd.DataFrame) -> Dict[str, str]:
        """Intelligent data type inference"""
        type_mapping = {}
        
        for column in data.columns:
            col_data = data[column]
            
            # Check for datetime
            if self._is_datetime_column(col_data):
                type_mapping[column] = 'datetime'
            # Check for categorical
            elif self._is_categorical_column(col_data):
                type_mapping[column] = 'categorical'
            # Check for numerical
            elif pd.api.types.is_numeric_dtype(col_data):
                if col_data.nunique() < 10:
                    type_mapping[column] = 'categorical'
                else:
                    type_mapping[column] = 'numerical'
            else:
                type_mapping[column] = 'text'
                
        self.data_types = type_mapping
        return type_mapping
    
    def _is_datetime_column(self, series: pd.Series) -> bool:
        """Check if column contains datetime data"""
        if pd.api.types.is_datetime64_any_dtype(series):
            return True
        
        # Try to convert to datetime
        try:
            pd.to_datetime(series, errors='raise')
            return True
        except:
            return False
    
    def _is_categorical_column(self, series: pd.Series) -> bool:
        """Check if column is categorical"""
        if series.dtype == 'object':
            unique_ratio = series.nunique() / len(series)
            return unique_ratio < 0.5 and series.nunique() < 100
        return False
    
    def _analyze_missing_values(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Comprehensive missing value analysis"""
        missing_count = data.isnull().sum()
        missing_percentage = (missing_count / len(data)) * 100
        
        return {
            'total_missing': missing_count.sum(),
            'missing_percentage_total': (missing_count.sum() / (data.shape[0] * data.shape[1])) * 100,
            'columns_missing': missing_count[missing_count > 0].to_dict(),
            'columns_missing_percentage': missing_percentage[missing_percentage > 0].to_dict()
        }
    
    def _generate_statistical_summary(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Generate statistical summary for numerical columns"""
        numerical_cols = data.select_dtypes(include=[np.number]).columns
        
        if len(numerical_cols) == 0:
            return {}
        
        stats = data[numerical_cols].describe().to_dict()
        
        # Add additional statistics
        for col in numerical_cols:
            stats[col]['variance'] = data[col].var()
            stats[col]['skewness'] = data[col].skew()
            
        return stats
    
    def _calculate_quality_metrics(self, data: pd.DataFrame) -> Dict[str, float]:
        """Calculate data quality metrics"""
        total_cells = data.shape[0] * data.shape[1]
        missing_cells = data.isnull().sum().sum()
        
        quality_score = 100 * (1 - missing_cells / total_cells)
        
        # Penalize for duplicates
        duplicate_penalty = data.duplicated().sum() / len(data) * 10
        quality_score -= duplicate_penalty
        
        return {
            'overall_quality_score': max(0, quality_score),
            'completeness_score': 100 * (1 - missing_cells / total_cells),
            'uniqueness_score': 100 * (1 - data.duplicated().sum() / len(data))
        }
    
    def generate_report(self) -> str:
        """Generate a human-readable profile report"""
        if not self.profile_report:
            return "No profile data available. Run analyze() first."
        
        report = []
        report.append("=" * 50)
        report.append(" SMART ETL DATA PROFILE REPORT")
        report.append("=" * 50)
        
        # Overview
        overview = self.profile_report['overview']
        report.append(f"Dataset Overview:")
        report.append(f"  Rows: {overview['num_rows']:,}")
        report.append(f"  Columns: {overview['num_columns']}")
        report.append(f"  Memory: {overview['memory_usage']}")
        report.append(f"  Duplicates: {overview['duplicate_rows']}")
        
        # Data Types
        report.append(f"\nData Types:")
        for col, dtype in self.profile_report['data_types'].items():
            report.append(f"  {col}: {dtype}")
        
        # Missing Values
        missing = self.profile_report['missing_values']
        report.append(f"\nMissing Values:")
        report.append(f"  Total missing: {missing['total_missing']}")
        report.append(f"  Missing percentage: {missing['missing_percentage_total']:.2f}%")
        
        # Quality Metrics
        quality = self.profile_report['quality_metrics']
        report.append(f"\nQuality Metrics:")
        report.append(f"  Overall Quality Score: {quality['overall_quality_score']:.1f}/100")
        report.append(f"  Completeness Score: {quality['completeness_score']:.1f}/100")
        report.append(f"  Uniqueness Score: {quality['uniqueness_score']:.1f}/100")
        
        return "\n".join(report)