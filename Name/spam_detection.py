import discord
import re
import time
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, Any, List, Set
from config import ConfigManager

logger = logging.getLogger(__name__)

class SpamDetector:
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        
        # Track user message history
        self.user_messages = defaultdict(lambda: deque(maxlen=50))
        self.user_message_times = defaultdict(lambda: deque(maxlen=50))
        self.duplicate_messages = defaultdict(lambda: defaultdict(int))
        
    async def check_message(self, message: discord.Message) -> bool:
        """
        Check if a message is spam
        Returns True if message is detected as spam
        """
        if not message.guild:
            return False
            
        guild_id = str(message.guild.id)
        config = self.config_manager.get_guild_config(guild_id)
        
        if not config['spam_detection']['enabled']:
            return False
            
        # Check if user is whitelisted
        if self._is_whitelisted(message.author, config):
            return False
            
        spam_score = 0
        max_score = 0
        reasons = []
        
        # Check message rate limiting
        rate_score, rate_reason = self._check_rate_limit(message, config)
        spam_score += rate_score
        max_score += 3
        if rate_reason:
            reasons.append(rate_reason)
        
        # Check for duplicate messages
        dup_score, dup_reason = self._check_duplicate_content(message, config)
        spam_score += dup_score
        max_score += 3
        if dup_reason:
            reasons.append(dup_reason)
        
        # Check mention spam
        if config['spam_detection']['check_mention_spam']:
            mention_score, mention_reason = self._check_mention_spam(message, config)
            spam_score += mention_score
            max_score += 2
            if mention_reason:
                reasons.append(mention_reason)
        
        # Check link spam
        if config['spam_detection']['check_link_spam']:
            link_score, link_reason = self._check_link_spam(message)
            spam_score += link_score
            max_score += 2
            if link_reason:
                reasons.append(link_reason)
        
        # Check message content patterns
        content_score, content_reason = self._check_content_patterns(message)
        spam_score += content_score
        max_score += 2
        if content_reason:
            reasons.append(content_reason)
        
        # Update message history
        self._update_message_history(message)
        
        # Calculate spam percentage
        spam_percentage = (spam_score / max_score) * 100 if max_score > 0 else 0
        
        # Log analysis if spam detected
        if spam_percentage >= 70:  # Threshold for spam detection
            logger.warning(f"Spam detected from {message.author} in {message.guild.name}: {spam_percentage:.1f}% ({spam_score}/{max_score})")
            if reasons:
                logger.warning(f"Reasons: {', '.join(reasons)}")
            return True
            
        return False
    
    def _is_whitelisted(self, member: discord.Member, config: Dict[str, Any]) -> bool:
        """Check if member is whitelisted"""
        whitelist = config.get('whitelist', {})
        
        # Check user whitelist
        if str(member.id) in whitelist.get('users', []):
            return True
            
        # Check role whitelist
        member_role_ids = [str(role.id) for role in member.roles]
        whitelisted_roles = whitelist.get('roles', [])
        
        return any(role_id in member_role_ids for role_id in whitelisted_roles)
    
    def _check_rate_limit(self, message: discord.Message, config: Dict[str, Any]) -> tuple:
        """Check if user is sending messages too quickly"""
        user_id = str(message.author.id)
        current_time = time.time()
        
        max_messages = config['spam_detection']['max_messages_per_window']
        time_window = config['spam_detection']['time_window_seconds']
        
        # Clean old timestamps
        cutoff_time = current_time - time_window
        while self.user_message_times[user_id] and self.user_message_times[user_id][0] < cutoff_time:
            self.user_message_times[user_id].popleft()
        
        # Add current message time
        self.user_message_times[user_id].append(current_time)
        
        message_count = len(self.user_message_times[user_id])
        
        if message_count > max_messages:
            excess = message_count - max_messages
            if excess >= 5:
                return 3, f"Severe rate limit exceeded ({message_count} messages in {time_window}s)"
            elif excess >= 3:
                return 2, f"Rate limit exceeded ({message_count} messages in {time_window}s)"
            else:
                return 1, f"High message rate ({message_count} messages in {time_window}s)"
                
        return 0, None
    
    def _check_duplicate_content(self, message: discord.Message, config: Dict[str, Any]) -> tuple:
        """Check for duplicate message content"""
        user_id = str(message.author.id)
        content = message.content.strip().lower()
        
        if not content:  # Skip empty messages
            return 0, None
            
        max_duplicates = config['spam_detection']['max_duplicate_messages']
        
        # Count this message
        self.duplicate_messages[user_id][content] += 1
        duplicate_count = self.duplicate_messages[user_id][content]
        
        # Clean old duplicate tracking
        if len(self.duplicate_messages[user_id]) > 20:
            oldest_content = min(self.duplicate_messages[user_id].keys(), 
                               key=lambda k: self.duplicate_messages[user_id][k])
            del self.duplicate_messages[user_id][oldest_content]
        
        if duplicate_count > max_duplicates:
            if duplicate_count >= max_duplicates + 3:
                return 3, f"Excessive duplicate messages ({duplicate_count} times)"
            elif duplicate_count >= max_duplicates + 1:
                return 2, f"Multiple duplicate messages ({duplicate_count} times)"
            else:
                return 1, f"Duplicate message detected ({duplicate_count} times)"
                
        return 0, None
    
    def _check_mention_spam(self, message: discord.Message, config: Dict[str, Any]) -> tuple:
        """Check for excessive mentions"""
        max_mentions = config['spam_detection']['max_mentions_per_message']
        
        total_mentions = len(message.mentions) + len(message.role_mentions)
        
        if message.mention_everyone:
            total_mentions += 10  # Heavy penalty for @everyone/@here
            
        if total_mentions > max_mentions:
            if total_mentions >= max_mentions + 5:
                return 2, f"Excessive mentions ({total_mentions} mentions)"
            else:
                return 1, f"High mention count ({total_mentions} mentions)"
                
        return 0, None
    
    def _check_link_spam(self, message: discord.Message) -> tuple:
        """Check for suspicious links"""
        content = message.content.lower()
        
        # Common spam link patterns
        suspicious_domains = [
            'discord.gg',  # Invite links (context dependent)
            'bit.ly', 'tinyurl.com', 'ow.ly',  # URL shorteners
            'free-discord-nitro', 'discord-nitro',  # Fake nitro scams
            'steam-gift', 'free-csgo', 'free-game'  # Gaming scams
        ]
        
        # Find URLs in message
        url_pattern = r'https?://(?:[-\w.])+(?:\:[0-9]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:\#(?:[\w.])*)?)?'
        urls = re.findall(url_pattern, content)
        
        if not urls:
            return 0, None
            
        spam_score = 0
        reasons = []
        
        # Check for excessive number of links
        if len(urls) > 3:
            spam_score += 1
            reasons.append(f"Multiple links ({len(urls)} URLs)")
        
        # Check for suspicious domains
        for url in urls:
            for domain in suspicious_domains:
                if domain in url:
                    spam_score += 2
                    reasons.append(f"Suspicious domain: {domain}")
                    break
        
        return min(spam_score, 2), "; ".join(reasons) if reasons else None
    
    def _check_content_patterns(self, message: discord.Message) -> tuple:
        """Check message content for spam patterns"""
        content = message.content.lower()
        
        if not content.strip():
            return 0, None
            
        spam_score = 0
        reasons = []
        
        # Check for excessive caps
        if len(content) > 10:
            caps_count = sum(1 for c in message.content if c.isupper())
            caps_ratio = caps_count / len(message.content)
            if caps_ratio > 0.7:
                spam_score += 1
                reasons.append("Excessive capital letters")
        
        # Check for spam keywords
        spam_keywords = [
            'free nitro', 'discord nitro free', 'free discord',
            'click here', 'limited time', 'act now',
            'congratulations', 'you have won', 'claim now'
        ]
        
        for keyword in spam_keywords:
            if keyword in content:
                spam_score += 1
                reasons.append(f"Spam keyword detected")
                break
        
        return min(spam_score, 2), "; ".join(reasons) if reasons else None
    
    def _update_message_history(self, message: discord.Message):
        """Update message history for user"""
        user_id = str(message.author.id)
        
        # Store message content (limited length to prevent memory issues)
        content = message.content[:500] if message.content else ""
        self.user_messages[user_id].append({
            'content': content,
            'timestamp': time.time(),
            'channel_id': message.channel.id,
            'guild_id': message.guild.id if message.guild else None
        })
    
    def clear_user_data(self, user_id: str):
        """Clear tracking data for a user"""
        user_id = str(user_id)
        
        if user_id in self.user_messages:
            del self.user_messages[user_id]
        if user_id in self.user_message_times:
            del self.user_message_times[user_id]
        if user_id in self.duplicate_messages:
            del self.duplicate_messages[user_id]
