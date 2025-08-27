import discord
import re
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from config import ConfigManager

logger = logging.getLogger(__name__)

class BotDetector:
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        
    async def analyze_member(self, member: discord.Member) -> bool:
        """
        Analyze a member to determine if they are suspicious
        Returns True if member appears to be a bot/malicious
        """
        guild_id = str(member.guild.id)
        config = self.config_manager.get_guild_config(guild_id)
        
        if not config['bot_detection']['enabled']:
            return False
            
        # Check if member is whitelisted
        if self._is_whitelisted(member, config):
            return False
            
        suspicious_score = 0
        max_score = 0
        reasons = []
        
        # Check account age
        age_score, age_reason = self._check_account_age(member, config)
        suspicious_score += age_score
        max_score += 3
        if age_reason:
            reasons.append(age_reason)
        
        # Check profile picture
        if config['bot_detection']['check_profile_picture']:
            pic_score, pic_reason = self._check_profile_picture(member)
            suspicious_score += pic_score
            max_score += 2
            if pic_reason:
                reasons.append(pic_reason)
        
        # Check username patterns
        if config['bot_detection']['check_username_patterns']:
            name_score, name_reason = self._check_username_patterns(member, config)
            suspicious_score += name_score
            max_score += 3
            if name_reason:
                reasons.append(name_reason)
        
        # Check join behavior
        behavior_score, behavior_reason = self._check_join_behavior(member)
        suspicious_score += behavior_score
        max_score += 2
        if behavior_reason:
            reasons.append(behavior_reason)
        
        # Calculate suspicion percentage
        suspicion_percentage = (suspicious_score / max_score) * 100 if max_score > 0 else 0
        
        # Log analysis
        logger.info(f"Bot analysis for {member}: {suspicion_percentage:.1f}% suspicious ({suspicious_score}/{max_score})")
        if reasons:
            logger.info(f"Reasons: {', '.join(reasons)}")
        
        # Consider suspicious if score is above threshold (60%)
        return suspicion_percentage >= 60
    
    def _is_whitelisted(self, member: discord.Member, config: Dict[str, Any]) -> bool:
        """Check if member is whitelisted"""
        whitelist = config.get('whitelist', {})
        
        # Check user whitelist
        if str(member.id) in whitelist.get('users', []):
            return True
            
        # Check role whitelist
        member_role_ids = [str(role.id) for role in member.roles]
        whitelisted_roles = whitelist.get('roles', [])
        
        if any(role_id in member_role_ids for role_id in whitelisted_roles):
            return True
            
        return False
    
    def _check_account_age(self, member: discord.Member, config: Dict[str, Any]) -> tuple:
        """Check if account is too new"""
        min_age_days = config['bot_detection']['min_account_age_days']
        
        if not member.created_at:
            return 2, "No creation date available"
            
        account_age = datetime.utcnow() - member.created_at.replace(tzinfo=None)
        age_days = account_age.days
        
        if age_days < min_age_days:
            if age_days < 1:
                return 3, f"Very new account (created {account_age})"
            elif age_days < 3:
                return 2, f"New account ({age_days} days old)"
            else:
                return 1, f"Relatively new account ({age_days} days old)"
                
        return 0, None
    
    def _check_profile_picture(self, member: discord.Member) -> tuple:
        """Check profile picture characteristics"""
        if not member.avatar:
            return 2, "No profile picture"
            
        # Check if using default Discord avatar
        if member.display_avatar == member.default_avatar:
            return 1, "Using default Discord avatar"
            
        return 0, None
    
    def _check_username_patterns(self, member: discord.Member, config: Dict[str, Any]) -> tuple:
        """Check username for suspicious patterns"""
        username = member.name.lower()
        display_name = member.display_name.lower()
        
        suspicious_patterns = config['bot_detection'].get('suspicious_patterns', [])
        
        # Check against defined patterns
        for pattern in suspicious_patterns:
            try:
                if re.search(pattern, username) or re.search(pattern, display_name):
                    return 3, f"Username matches suspicious pattern"
            except re.error:
                logger.warning(f"Invalid regex pattern: {pattern}")
                continue
        
        # Additional heuristics
        score = 0
        reasons = []
        
        # Very short usernames
        if len(username) <= 2:
            score += 1
            reasons.append("Very short username")
        
        # Only numbers
        if username.isdigit():
            score += 2
            reasons.append("Username is only numbers")
        
        # Random character sequences
        if self._looks_random(username):
            score += 2
            reasons.append("Username appears random")
        
        # Common bot suffixes
        bot_suffixes = ['bot', 'auto', 'spam', 'promo', 'ad']
        if any(suffix in username for suffix in bot_suffixes):
            score += 1
            reasons.append("Username contains bot-like terms")
        
        return min(score, 3), "; ".join(reasons) if reasons else None
    
    def _check_join_behavior(self, member: discord.Member) -> tuple:
        """Check suspicious join behavior"""
        # Check if joined very recently after creation
        if member.created_at and member.joined_at:
            time_diff = member.joined_at - member.created_at
            
            if time_diff < timedelta(minutes=5):
                return 2, "Joined very quickly after account creation"
            elif time_diff < timedelta(hours=1):
                return 1, "Joined shortly after account creation"
                
        return 0, None
    
    def _looks_random(self, text: str) -> bool:
        """Determine if text looks like random characters"""
        if len(text) < 4:
            return False
            
        # Check for alternating patterns
        consonants = "bcdfghjklmnpqrstvwxyz"
        vowels = "aeiou"
        
        consonant_count = sum(1 for char in text if char in consonants)
        vowel_count = sum(1 for char in text if char in vowels)
        
        # If mostly consonants or no vowels, likely random
        if vowel_count == 0 and len(text) > 3:
            return True
            
        if consonant_count > len(text) * 0.8:
            return True
            
        # Check for repeated patterns
        if len(set(text)) < len(text) * 0.4:  # Low character diversity
            return True
            
        return False
    
    def add_to_whitelist(self, guild_id: str, user_id: str) -> bool:
        """Add user to whitelist"""
        config = self.config_manager.get_guild_config(guild_id)
        
        if 'whitelist' not in config:
            config['whitelist'] = {'users': [], 'roles': []}
            
        if str(user_id) not in config['whitelist']['users']:
            config['whitelist']['users'].append(str(user_id))
            return self.config_manager.save_guild_config(guild_id, config)
            
        return True
    
    def remove_from_whitelist(self, guild_id: str, user_id: str) -> bool:
        """Remove user from whitelist"""
        config = self.config_manager.get_guild_config(guild_id)
        
        if 'whitelist' in config and str(user_id) in config['whitelist']['users']:
            config['whitelist']['users'].remove(str(user_id))
            return self.config_manager.save_guild_config(guild_id, config)
            
        return True
