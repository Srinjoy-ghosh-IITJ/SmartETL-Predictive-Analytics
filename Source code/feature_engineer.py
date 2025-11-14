"""
Smart ETL - Feature Engineering Module
Automated feature creation and selection.
"""

import pandas as pd
import numpy as np
from sklearn.feature_selection import SelectKBest, f_classif, mutual_info_classif
from sklearn.decomposition import PCA
from typing import Dict, Any, List, Tuple
import featuretools as ft

class FeatureEngineer:
    def __init__(self, max_features: int = 50):
        self.max_features = max_features
        self.created_features = []
        self.selected_features = []
        self.feature_importance = {}
    
    def create_features(self, data: pd.DataFrame, target_column: str = None) -> pd.DataFrame:
        """
        Automated feature engineering
        
        Args:
            data: Cleaned input data
            target_column: Optional target for supervised feature selection
            
        Returns:
            DataFrame with engineered features
        """
        print(" Starting feature engineering...")
        
        engineered_data = data.copy()
        
        # Create temporal features
        engineered_data = self._create_temporal_features(engineered_data)
        
        # Create interaction features
        engineered_data = self._create_interaction_features(engineered_data)
        
        # Create statistical features
        engineered_data = self._create_statistical_features(engineered_data)
        
        # Select best features
        if target_column and target_column in engineered_data.columns:
            engineered_data = self._select_features(engineered_data, target_column)
        
        print(f" Feature engineering completed! Created {len(self.created_features)} new features")
        return engineered_data
    
    def _create_temporal_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create temporal features from datetime columns"""
        engineered_data = data.copy()
        
        # Check for datetime columns (you would extend this based on your data)
        datetime_columns = []
        for col in engineered_data.columns:
            if 'date' in col.lower() or 'time' in col.lower():
                datetime_columns.append(col)
        
        for col in datetime_columns:
            try:
                # Convert to datetime
                engineered_data[col] = pd.to_datetime(engineered_data[col])
                
                # Extract temporal features
                engineered_data[f'{col}_year'] = engineered_data[col].dt.year
                engineered_data[f'{col}_month'] = engineered_data[col].dt.month
                engineered_data[f'{col}_day'] = engineered_data[col].dt.day
                engineered_data[f'{col}_dayofweek'] = engineered_data[col].dt.dayofweek
                engineered_data[f'{col}_quarter'] = engineered_data[col].dt.quarter
                
                self.created_features.extend([
                    f'{col}_year', f'{col}_month', f'{col}_day',
                    f'{col}_dayofweek', f'{col}_quarter'
                ])
                print(f" Created temporal features from '{col}'")
                
            except:
                continue
        
        return engineered_data
    
    def _create_interaction_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create interaction features between numerical columns"""
        engineered_data = data.copy()
        numerical_cols = data.select_dtypes(include=[np.number]).columns.tolist()
        
        # Limit to top numerical columns to avoid combinatorial explosion
        if len(numerical_cols) > 5:
            numerical_cols = numerical_cols[:5]
        
        # Create interaction features
        for i, col1 in enumerate(numerical_cols):
            for col2 in numerical_cols[i+1:]:
                # Multiplication interaction
                engineered_data[f'{col1}_x_{col2}'] = engineered_data[col1] * engineered_data[col2]
                self.created_features.append(f'{col1}_x_{col2}')
                
                # Ratio interaction (avoid division by zero)
                if (engineered_data[col2] != 0).all():
                    engineered_data[f'{col1}_div_{col2}'] = engineered_data[col1] / engineered_data[col2]
                    self.created_features.append(f'{col1}_div_{col2}')
        
        print(f" Created {len(self.created_features)} interaction features")
        return engineered_data
    
    def _create_statistical_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create statistical aggregation features"""
        engineered_data = data.copy()
        numerical_cols = data.select_dtypes(include=[np.number]).columns.tolist()
        
        if len(numerical_cols) >= 3:
            # Create rolling statistics for first 3 numerical columns
            for col in numerical_cols[:3]:
                engineered_data[f'{col}_rolling_mean'] = engineered_data[col].rolling(window=3, min_periods=1).mean()
                engineered_data[f'{col}_rolling_std'] = engineered_data[col].rolling(window=3, min_periods=1).std()
                
                self.created_features.extend([
                    f'{col}_rolling_mean', f'{col}_rolling_std'
                ])
        
        print(f" Created statistical aggregation features")
        return engineered_data
    
    def _select_features(self, data: pd.DataFrame, target_column: str) -> pd.DataFrame:
        """Select most important features using statistical tests"""
        X = data.drop(columns=[target_column])
        y = data[target_column]
        
        # Ensure we have numerical data
        X_numeric = X.select_dtypes(include=[np.number])
        
        if X_numeric.shape[1] == 0:
            return data
        
        # Use mutual information for feature selection
        selector = SelectKBest(score_func=mutual_info_classif, k=min(self.max_features, X_numeric.shape[1]))
        X_selected = selector.fit_transform(X_numeric, y)
        
        # Get selected feature names
        selected_mask = selector.get_support()
        self.selected_features = X_numeric.columns[selected_mask].tolist()
        
        # Store feature importance scores
        for i, (feature, score) in enumerate(zip(X_numeric.columns, selector.scores_)):
            self.feature_importance[feature] = score
        
        # Create result with selected features + target
        result_data = X[self.selected_features].copy()
        result_data[target_column] = y
        
        print(f"   Selected {len(self.selected_features)} most important features")
        return result_data
    
    def get_feature_summary(self) -> str:
        """Get feature engineering summary"""
        summary = []
        summary.append("Feature Engineering Summary:")
        summary.append(f"  Total features created: {len(self.created_features)}")
        summary.append(f"  Features selected: {len(self.selected_features)}")
        
        if self.feature_importance:
            top_features = sorted(self.feature_importance.items(), key=lambda x: x[1], reverse=True)[:5]
            summary.append("  Top 5 most important features:")
            for feature, importance in top_features:
                summary.append(f"    {feature}: {importance:.4f}")
        
        return "\n".join(summary)