"""
Configuration management for Smart ETL system.
"""

import os
import yaml
from typing import Dict, Any, Optional

class Config:
    """
    Configuration manager for Smart ETL system settings.
    """
    
    # Default configuration
    DEFAULTS = {
        'data_cleaning': {
            'max_missing_percentage': 50.0,
            'outlier_threshold': 1.5,
            'imputation_strategy': 'auto'
        },
        'feature_engineering': {
            'max_features': 50,
            'feature_selection_method': 'mutual_info',
            'interaction_depth': 2
        },
        'performance': {
            'parallel_processing': True,
            'chunk_size': 10000,
            'max_memory_usage': '2GB'
        },
        'output': {
            'save_pipeline': True,
            'generate_report': True,
            'export_code': True
        }
    }
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            config_file: Path to YAML configuration file
        """
        self.config = self.DEFAULTS.copy()
        
        if config_file and os.path.exists(config_file):
            self.load_config(config_file)
    
    def load_config(self, config_file: str) -> None:
        """
        Load configuration from YAML file.
        
        Args:
            config_file: Path to YAML configuration file
        """
        try:
            with open(config_file, 'r') as file:
                user_config = yaml.safe_load(file)
                self._update_config(self.config, user_config)
            print(f"Configuration loaded from {config_file}")
        except Exception as e:
            print(f"Could not load config file: {e}. Using defaults.")
    
    def _update_config(self, default: Dict, user: Dict) -> None:
        """
        Recursively update default config with user config.
        """
        for key, value in user.items():
            if key in default:
                if isinstance(value, dict) and isinstance(default[key], dict):
                    self._update_config(default[key], value)
                else:
                    default[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 'data_cleaning.max_missing_percentage')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value using dot notation.
        
        Args:
            key: Configuration key
            value: New value
        """
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save_config(self, config_file: str) -> None:
        """
        Save current configuration to YAML file.
        
        Args:
            config_file: Path to save configuration
        """
        try:
            os.makedirs(os.path.dirname(config_file), exist_ok=True)
            with open(config_file, 'w') as file:
                yaml.dump(self.config, file, default_flow_style=False)
            print(f"Configuration saved to {config_file}")
        except Exception as e:
            print(f"Could not save config file: {e}")
    
    def show_config(self) -> None:
        """
        Display current configuration.
        """
        print("Current Configuration:")
        print(yaml.dump(self.config, default_flow_style=False))

# Global configuration instance
config = Config()