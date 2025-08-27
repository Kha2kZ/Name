import json
import os
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self, config_dir: str = "configs"):
        self.config_dir = config_dir
        self.default_config_file = "default_config.json"
        
        # Create config directory if it doesn't exist
        os.makedirs(config_dir, exist_ok=True)
        
        # Load default configuration
        self.default_config = self._load_default_config()
        
    def _load_default_config(self) -> Dict[str, Any]:
        """Load the default configuration"""
        try:
            with open(self.default_config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Default config file not found, using hardcoded defaults")
            return self._get_hardcoded_defaults()
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing default config: {e}")
            return self._get_hardcoded_defaults()
    
    def _get_hardcoded_defaults(self) -> Dict[str, Any]:
        """Get hardcoded default configuration"""
        return {
            "enabled": True,
            "bot_detection": {
                "enabled": True,
                "min_account_age_days": 7,
                "check_profile_picture": True,
                "check_username_patterns": True,
                "suspicious_patterns": [
                    r"^[a-z]+\d{4,}$",  # lowercase letters followed by numbers
                    r"^.{1,3}$",        # very short usernames
                    r"discord\.gg",     # invite links in username
                    r"bit\.ly",         # suspicious short links
                ],
                "action": "quarantine"  # quarantine, kick, ban
            },
            "spam_detection": {
                "enabled": True,
                "max_messages_per_window": 5,
                "time_window_seconds": 10,
                "max_duplicate_messages": 3,
                "check_mention_spam": True,
                "max_mentions_per_message": 5,
                "check_link_spam": True,
                "action": "timeout"  # timeout, kick, ban
            },
            "raid_protection": {
                "enabled": True,
                "max_joins": 10,
                "time_window": 60,  # seconds
                "action": "lockdown"  # lockdown, alert
            },
            "verification": {
                "enabled": False,
                "require_for_new_accounts": True,
                "new_account_threshold_days": 7,
                "verification_timeout_minutes": 10,
                "quarantine_role": None
            },
            "logging": {
                "enabled": True,
                "channel_id": None,
                "log_joins": True,
                "log_kicks": True,
                "log_bans": True,
                "log_timeouts": True,
                "log_detections": True
            },
            "whitelist": {
                "users": [],
                "roles": []
            }
        }
    
    def get_guild_config(self, guild_id: str) -> Dict[str, Any]:
        """Get configuration for a specific guild"""
        config_file = os.path.join(self.config_dir, f"{guild_id}.json")
        
        try:
            with open(config_file, 'r') as f:
                guild_config = json.load(f)
                
            # Merge with defaults for any missing keys
            merged_config = self._merge_configs(self.default_config, guild_config)
            return merged_config
            
        except FileNotFoundError:
            # Return default config and save it
            self.save_guild_config(guild_id, self.default_config)
            return self.default_config.copy()
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing config for guild {guild_id}: {e}")
            return self.default_config.copy()
    
    def save_guild_config(self, guild_id: str, config: Dict[str, Any]) -> bool:
        """Save configuration for a specific guild"""
        config_file = os.path.join(self.config_dir, f"{guild_id}.json")
        
        try:
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
            logger.info(f"Saved config for guild {guild_id}")
            return True
        except Exception as e:
            logger.error(f"Error saving config for guild {guild_id}: {e}")
            return False
    
    def initialize_guild_config(self, guild_id: str) -> bool:
        """Initialize configuration for a new guild"""
        config_file = os.path.join(self.config_dir, f"{guild_id}.json")
        if not os.path.exists(config_file):
            return self.save_guild_config(guild_id, self.default_config.copy())
        return True
    
    def _merge_configs(self, default: Dict[str, Any], guild: Dict[str, Any]) -> Dict[str, Any]:
        """Merge guild config with default config"""
        merged = default.copy()
        
        for key, value in guild.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = self._merge_configs(merged[key], value)
            else:
                merged[key] = value
                
        return merged
    
    def update_guild_setting(self, guild_id: str, setting_path: str, value: Any) -> bool:
        """Update a specific setting in guild config"""
        config = self.get_guild_config(guild_id)
        
        # Navigate to the setting using dot notation
        keys = setting_path.split('.')
        current = config
        
        try:
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]
            
            current[keys[-1]] = value
            return self.save_guild_config(guild_id, config)
            
        except Exception as e:
            logger.error(f"Error updating setting {setting_path}: {e}")
            return False
    
    def get_guild_setting(self, guild_id: str, setting_path: str, default=None):
        """Get a specific setting from guild config"""
        config = self.get_guild_config(guild_id)
        
        keys = setting_path.split('.')
        current = config
        
        try:
            for key in keys:
                current = current[key]
            return current
        except (KeyError, TypeError):
            return default
