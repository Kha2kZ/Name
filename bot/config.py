"""
Configuration Management System
Handles guild-specific configuration settings
"""

import json
import os
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self, config_dir="config"):
        self.config_dir = config_dir
        self.default_config_path = os.path.join(config_dir, "default_config.json")
        self.guild_configs = {}
        
        # Ensure config directory exists
        os.makedirs(config_dir, exist_ok=True)
        
        # Load default configuration
        self.default_config = self.load_default_config()
    
    def load_default_config(self) -> Dict[str, Any]:
        """Load the default configuration"""
        try:
            if os.path.exists(self.default_config_path):
                with open(self.default_config_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading default config: {e}")
        
        # Return hardcoded defaults if file doesn't exist or fails to load
        return {
            "antibot": {
                "enabled": True,
                "auto_kick_threshold": 70,
                "auto_ban_threshold": 85,
                "auto_timeout": True,
                "raid_protection": True,
                "max_joins_per_minute": 5
            },
            "spam_detection": {
                "enabled": True,
                "max_messages_per_minute": 10,
                "max_mentions": 5,
                "spam_keywords": [
                    "discord.gg/",
                    "free nitro",
                    "discord nitro",
                    "@everyone",
                    "http://",
                    "https://bit.ly",
                    "https://tinyurl.com"
                ]
            },
            "verification": {
                "enabled": False,
                "role_name": "Unverified",
                "auto_verify_after_hours": 24
            },
            "logging": {
                "enabled": True,
                "log_channel": None,
                "log_joins": True,
                "log_leaves": True,
                "log_moderation": True
            },
            "moderation": {
                "log_actions": True,
                "require_reason": False,
                "max_mute_duration": 2419200
            }
        }
    
    def get_guild_config_path(self, guild_id: int) -> str:
        """Get the path to a guild's config file"""
        return os.path.join(self.config_dir, f"guild_{guild_id}.json")
    
    def load_guild_config(self, guild_id: int) -> Dict[str, Any]:
        """Load configuration for a specific guild"""
        config_path = self.get_guild_config_path(guild_id)
        
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    guild_config = json.load(f)
                    # Merge with defaults to ensure all keys exist
                    return self.merge_configs(self.default_config, guild_config)
        except Exception as e:
            logger.error(f"Error loading config for guild {guild_id}: {e}")
        
        # Return default config if loading fails
        return self.default_config.copy()
    
    def save_guild_config(self, guild_id: int, config: Dict[str, Any]) -> bool:
        """Save configuration for a specific guild"""
        config_path = self.get_guild_config_path(guild_id)
        
        try:
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=4)
            
            # Update cached config
            self.guild_configs[guild_id] = config
            logger.info(f"Saved config for guild {guild_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving config for guild {guild_id}: {e}")
            return False
    
    def get_guild_config(self, guild_id: int) -> Dict[str, Any]:
        """Get configuration for a guild (with caching)"""
        if guild_id not in self.guild_configs:
            self.guild_configs[guild_id] = self.load_guild_config(guild_id)
        
        return self.guild_configs[guild_id]
    
    def initialize_guild_config(self, guild_id: int) -> bool:
        """Initialize configuration for a new guild"""
        if guild_id not in self.guild_configs:
            config = self.default_config.copy()
            return self.save_guild_config(guild_id, config)
        return True
    
    def update_guild_config(self, guild_id: int, section: str, key: str, value: Any) -> bool:
        """Update a specific configuration value"""
        config = self.get_guild_config(guild_id)
        
        if section not in config:
            config[section] = {}
        
        config[section][key] = value
        return self.save_guild_config(guild_id, config)
    
    def merge_configs(self, default: Dict[str, Any], guild: Dict[str, Any]) -> Dict[str, Any]:
        """Merge guild config with default config"""
        result = default.copy()
        
        for section, settings in guild.items():
            if section in result and isinstance(result[section], dict) and isinstance(settings, dict):
                result[section].update(settings)
            else:
                result[section] = settings
        
        return result
    
    def reset_guild_config(self, guild_id: int) -> bool:
        """Reset guild configuration to defaults"""
        return self.save_guild_config(guild_id, self.default_config.copy())
    
    def get_all_guild_configs(self) -> Dict[int, Dict[str, Any]]:
        """Get all cached guild configurations"""
        return self.guild_configs.copy()
    
    def reload_guild_config(self, guild_id: int) -> Dict[str, Any]:
        """Force reload configuration for a guild"""
        if guild_id in self.guild_configs:
            del self.guild_configs[guild_id]
        return self.get_guild_config(guild_id)
