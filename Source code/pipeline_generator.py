"""
Smart ETL - Pipeline Generation Module
Export reproducible data processing pipelines.
"""

import pandas as pd
import pickle
import json
from datetime import datetime
from typing import Dict, Any, List
import os

class PipelineGenerator:
    """
    Generate and export reproducible data processing pipelines.
    """
    
    def __init__(self):
        self.pipeline_steps = []
        self.pipeline_metadata = {}
    
    def add_step(self, step_name: str, step_function: str, parameters: Dict[str, Any]) -> None:
        """
        Add a step to the pipeline.
        
        Args:
            step_name: Name of the step
            step_function: Function name or description
            parameters: Parameters used in this step
        """
        step = {
            'name': step_name,
            'function': step_function,
            'parameters': parameters,
            'timestamp': datetime.now().isoformat()
        }
        self.pipeline_steps.append(step)
    
    def generate_python_code(self) -> str:
        """
        Generate reproducible Python code for the entire pipeline.
        
        Returns:
            Python code as string
        """
        code_lines = [
            "# Smart ETL Auto-Generated Pipeline",
            f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "# This code reproduces the entire data processing pipeline",
            "",
            "import pandas as pd",
            "import numpy as np",
            "from sklearn.impute import SimpleImputer, KNNImputer",
            "from sklearn.preprocessing import StandardScaler, LabelEncoder",
            "from sklearn.feature_selection import SelectKBest, mutual_info_classif",
            "",
            "def run_smart_etl_pipeline(data):",
            "    \"\"\"",
            "    Execute the complete Smart ETL pipeline.",
            "    \"\"\"",
            "    processed_data = data.copy()",
            "    ",
        ]
        
        # Add each step as code
        for i, step in enumerate(self.pipeline_steps, 1):
            code_lines.append(f"    # Step {i}: {step['name']}")
            
            if step['function'] == 'data_profiling':
                code_lines.append("    # Data profiling completed")
                
            elif step['function'] == 'data_cleaning':
                code_lines.extend(self._generate_cleaning_code(step))
                
            elif step['function'] == 'feature_engineering':
                code_lines.extend(self._generate_feature_engineering_code(step))
                
            code_lines.append("")
        
        code_lines.extend([
            "    return processed_data",
            "",
            "# Execute pipeline",
            "# processed_data = run_smart_etl_pipeline(your_dataframe)",
        ])
        
        return "\n".join(code_lines)
    
    def _generate_cleaning_code(self, step: Dict) -> List[str]:
        """Generate code for data cleaning steps."""
        code_lines = []
        params = step['parameters']
        
        if 'imputation' in params:
            for col, strategy in params['imputation'].items():
                if strategy == 'median':
                    code_lines.append(f"    processed_data['{col}'].fillna(processed_data['{col}'].median(), inplace=True)")
                elif strategy == 'mean':
                    code_lines.append(f"    processed_data['{col}'].fillna(processed_data['{col}'].mean(), inplace=True)")
                elif strategy == 'mode':
                    code_lines.append(f"    processed_data['{col}'].fillna(processed_data['{col}'].mode()[0], inplace=True)")
        
        if 'encoding' in params:
            for col, method in params['encoding'].items():
                if method == 'one_hot':
                    code_lines.append(f"    {col}_encoded = pd.get_dummies(processed_data['{col}'], prefix='{col}')")
                    code_lines.append(f"    processed_data = pd.concat([processed_data, {col}_encoded], axis=1)")
                    code_lines.append(f"    processed_data.drop('{col}', axis=1, inplace=True)")
                elif method == 'label':
                    code_lines.append(f"    le = LabelEncoder()")
                    code_lines.append(f"    processed_data['{col}'] = le.fit_transform(processed_data['{col}'].astype(str))")
        
        return code_lines
    
    def _generate_feature_engineering_code(self, step: Dict) -> List[str]:
        """Generate code for feature engineering steps."""
        code_lines = []
        params = step['parameters']
        
        if 'temporal_features' in params:
            for col in params['temporal_features']:
                code_lines.append(f"    # Temporal features from {col}")
                code_lines.append(f"    processed_data['{col}'] = pd.to_datetime(processed_data['{col}'])")
                code_lines.append(f"    processed_data['{col}_year'] = processed_data['{col}'].dt.year")
                code_lines.append(f"    processed_data['{col}_month'] = processed_data['{col}'].dt.month")
                code_lines.append(f"    processed_data['{col}_day'] = processed_data['{col}'].dt.day")
        
        if 'interaction_features' in params:
            for feat1, feat2 in params['interaction_features']:
                code_lines.append(f"    processed_data['{feat1}_x_{feat2}'] = processed_data['{feat1}'] * processed_data['{feat2}']")
        
        return code_lines
    
    def save_pipeline(self, filepath: str, format: str = 'python') -> bool:
        """
        Save pipeline to file.
        
        Args:
            filepath: Path where to save the pipeline
            format: Format to save ('python', 'json', 'pickle')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            if format == 'python':
                code = self.generate_python_code()
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(code)
                    
            elif format == 'json':
                pipeline_data = {
                    'metadata': self.pipeline_metadata,
                    'steps': self.pipeline_steps,
                    'generated_at': datetime.now().isoformat()
                }
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(pipeline_data, f, indent=2)
                    
            elif format == 'pickle':
                with open(filepath, 'wb') as f:
                    pickle.dump(self, f)
            
            print(f"Pipeline saved to {filepath} (format: {format})")
            return True
            
        except Exception as e:
            print(f" Failed to save pipeline: {e}")
            return False
    
    def generate_report(self) -> str:
        """
        Generate a human-readable pipeline report.
        
        Returns:
            Pipeline report as string
        """
        report = []
        report.append("=" * 60)
        report.append(" SMART ETL PIPELINE REPORT")
        report.append("=" * 60)
        report.append(f"Total Steps: {len(self.pipeline_steps)}")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        for i, step in enumerate(self.pipeline_steps, 1):
            report.append(f"Step {i}: {step['name']}")
            report.append(f"  Function: {step['function']}")
            report.append(f"  Timestamp: {step['timestamp']}")
            
            if step['parameters']:
                report.append("  Parameters:")
                for key, value in step['parameters'].items():
                    report.append(f"    {key}: {value}")
            
            report.append("")
        
        return "\n".join(report)
    
    def get_pipeline_summary(self) -> Dict[str, Any]:
        """
        Get pipeline summary statistics.
        
        Returns:
            Dictionary with pipeline summary
        """
        return {
            'total_steps': len(self.pipeline_steps),
            'step_types': [step['function'] for step in self.pipeline_steps],
            'cleaning_steps': len([s for s in self.pipeline_steps if s['function'] == 'data_cleaning']),
            'feature_steps': len([s for s in self.pipeline_steps if s['function'] == 'feature_engineering']),
            'first_step': self.pipeline_steps[0]['timestamp'] if self.pipeline_steps else None,
            'last_step': self.pipeline_steps[-1]['timestamp'] if self.pipeline_steps else None
        }