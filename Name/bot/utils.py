"""
Utility functions for the Discord anti-bot moderation system
"""

import discord
import re
import time
import json
from datetime import datetime, timedelta
from typing import List, Optional, Union, Dict, Any
import logging

logger = logging.getLogger(__name__)

def format_duration(seconds: int) -> str:
    """Format duration in seconds to human readable string"""
    if seconds < 60:
        return f"{seconds} second{'s' if seconds != 1 else ''}"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    elif seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''}"
    else:
        days = seconds // 86400
        return f"{days} day{'s' if days != 1 else ''}"

def parse_duration(duration_str: str) -> Optional[int]:
    """Parse duration string like '5m', '1h', '2d' to seconds"""
    if not duration_str:
        return None
    
    # Remove spaces and convert to lowercase
    duration_str = duration_str.replace(" ", "").lower()
    
    # Regex pattern to match number followed by time unit
    pattern = r'(\d+)([smhd]?)'
    match = re.match(pattern, duration_str)
    
    if not match:
        return None
    
    amount_str, unit = match.groups()
    amount = int(amount_str)
    
    # Default to seconds if no unit specified
    multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400, '': 1}
    multiplier = multipliers.get(unit, 1)
    
    return amount * multiplier

def is_valid_discord_invite(url: str) -> bool:
    """Check if a URL is a Discord invite link"""
    discord_patterns = [
        r'(?:https?://)?(?:www\.)?discord\.gg/[a-zA-Z0-9]+',
        r'(?:https?://)?(?:www\.)?discordapp\.com/invite/[a-zA-Z0-9]+',
        r'(?:https?://)?(?:www\.)?discord\.com/invite/[a-zA-Z0-9]+'
    ]
    
    return any(re.match(pattern, url) for pattern in discord_patterns)

def extract_user_id(user_str: str) -> Optional[int]:
    """Extract user ID from mention, username, or ID string"""
    if not user_str:
        return None
    
    # Check if it's a mention (<@123456789> or <@!123456789>)
    mention_pattern = r'<@!?(\d+)>'
    match = re.match(mention_pattern, user_str.strip())
    if match:
        return int(match.group(1))
    
    # Check if it's just a user ID (digits only)
    if user_str.strip().isdigit():
        return int(user_str.strip())
    
    return None

def get_member_info_embed(member: discord.Member) -> discord.Embed:
    """Create a detailed embed with member information"""
    embed = discord.Embed(
        title=f"Member Information",
        color=member.color if member.color != discord.Color.default() else discord.Color.blue(),
        timestamp=datetime.utcnow()
    )
    
    # Set thumbnail to member's avatar
    if member.avatar:
        embed.set_thumbnail(url=member.avatar.url)
    else:
        embed.set_thumbnail(url=member.default_avatar.url)
    
    # Basic info
    embed.add_field(
        name="👤 User", 
        value=f"{member.mention}\n`{member.name}#{member.discriminator}`", 
        inline=True
    )
    embed.add_field(name="🆔 ID", value=f"`{member.id}`", inline=True)
    embed.add_field(name="🤖 Bot", value="✅ Yes" if member.bot else "❌ No", inline=True)
    
    # Account creation
    account_age = datetime.utcnow() - member.created_at.replace(tzinfo=None)
    embed.add_field(
        name="📅 Account Created", 
        value=f"{discord.utils.format_dt(member.created_at, style='F')}\n({account_age.days} days ago)", 
        inline=True
    )
    
    # Server join date
    if member.joined_at:
        join_age = datetime.utcnow() - member.joined_at.replace(tzinfo=None)
        embed.add_field(
            name="📥 Joined Server", 
            value=f"{discord.utils.format_dt(member.joined_at, style='F')}\n({join_age.days} days ago)", 
            inline=True
        )
    
    # Status and activity
    status_emoji = {
        discord.Status.online: "🟢",
        discord.Status.idle: "🟡", 
        discord.Status.dnd: "🔴",
        discord.Status.offline: "⚫"
    }
    embed.add_field(
        name="📊 Status", 
        value=f"{status_emoji.get(member.status, '❓')} {member.status.name.title()}", 
        inline=True
    )
    
    # Roles (exclude @everyone)
    if len(member.roles) > 1:
        roles = [role.mention for role in reversed(member.roles[1:])]  # Skip @everyone, highest first
        roles_text = ", ".join(roles[:10])  # Limit to 10 roles to avoid embed limits
        if len(member.roles) > 11:
            roles_text += f"\n*... and {len(member.roles) - 11} more roles*"
        embed.add_field(name=f"🎭 Roles ({len(member.roles) - 1})", value=roles_text, inline=False)
    
    # Permissions summary
    perms = get_key_permissions(member)
    if perms:
        embed.add_field(name="🔑 Key Permissions", value=", ".join(perms), inline=False)
    
    return embed

def get_key_permissions(member: discord.Member) -> List[str]:
    """Get a list of key permissions for a member"""
    perms = member.guild_permissions
    key_perms = []
    
    perm_mapping = {
        'administrator': 'Administrator',
        'manage_guild': 'Manage Server',
        'manage_roles': 'Manage Roles', 
        'manage_channels': 'Manage Channels',
        'kick_members': 'Kick Members',
        'ban_members': 'Ban Members',
        'manage_messages': 'Manage Messages',
        'moderate_members': 'Timeout Members',
        'mention_everyone': 'Mention Everyone',
        'manage_webhooks': 'Manage Webhooks',
        'view_audit_log': 'View Audit Log'
    }
    
    for perm_name, display_name in perm_mapping.items():
        if getattr(perms, perm_name, False):
            key_perms.append(display_name)
    
    return key_perms

def get_permission_level(member: discord.Member) -> str:
    """Get the general permission level of a member"""
    perms = member.guild_permissions
    
    if perms.administrator:
        return "Administrator"
    elif perms.manage_guild:
        return "Manager"
    elif perms.kick_members or perms.ban_members or perms.moderate_members:
        return "Moderator"
    elif perms.manage_messages:
        return "Helper"
    else:
        return "Member"

def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """Split a list into chunks of specified size"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def sanitize_filename(filename: str) -> str:
    """Sanitize a string to be safe for use as a filename"""
    # Remove invalid characters for most file systems
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Remove control characters
    filename = ''.join(char for char in filename if ord(char) >= 32)
    
    # Remove leading/trailing spaces and dots
    filename = filename.strip('. ')
    
    # Limit length to 255 characters (common file system limit)
    if len(filename) > 255:
        filename = filename[:252] + "..."
    
    return filename or "unnamed_file"

def escape_markdown(text: str) -> str:
    """Escape Discord markdown characters in text"""
    markdown_chars = ['*', '_', '`', '~', '|', '>', '#']
    escaped_text = text
    
    for char in markdown_chars:
        escaped_text = escaped_text.replace(char, f'\\{char}')
    
    return escaped_text

def truncate_text(text: str, max_length: int = 2000, suffix: str = "...") -> str:
    """Truncate text to fit within Discord's limits"""
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix

def create_embed(title: str, description: str = None, color: discord.Color = None, **kwargs) -> discord.Embed:
    """Create a standardized embed with common styling"""
    if color is None:
        color = discord.Color.blue()
    
    embed = discord.Embed(title=title, description=description, color=color, **kwargs)
    return embed

def create_error_embed(title: str, description: str) -> discord.Embed:
    """Create a standardized error embed"""
    return create_embed(f"❌ {title}", description, discord.Color.red())

def create_success_embed(title: str, description: str) -> discord.Embed:
    """Create a standardized success embed"""
    return create_embed(f"✅ {title}", description, discord.Color.green())

def create_warning_embed(title: str, description: str) -> discord.Embed:
    """Create a standardized warning embed"""
    return create_embed(f"⚠️ {title}", description, discord.Color.orange())

def create_info_embed(title: str, description: str) -> discord.Embed:
    """Create a standardized info embed"""
    return create_embed(f"ℹ️ {title}", description, discord.Color.blue())

class RateLimiter:
    """Rate limiter for commands and actions"""
    
    def __init__(self, max_uses: int, time_window: int):
        """
        Initialize rate limiter
        
        Args:
            max_uses: Maximum number of uses within time window
            time_window: Time window in seconds
        """
        self.max_uses = max_uses
        self.time_window = time_window
        self.usage_history: Dict[int, List[float]] = {}
    
    def is_rate_limited(self, user_id: int) -> bool:
        """Check if user is currently rate limited"""
        current_time = time.time()
        
        # Initialize user history if not exists
        if user_id not in self.usage_history:
            self.usage_history[user_id] = []
        
        # Clean old entries
        cutoff_time = current_time - self.time_window
        self.usage_history[user_id] = [
            timestamp for timestamp in self.usage_history[user_id]
            if timestamp > cutoff_time
        ]
        
        # Check if over limit
        if len(self.usage_history[user_id]) >= self.max_uses:
            return True
        
        # Add current usage
        self.usage_history[user_id].append(current_time)
        return False
    
    def get_reset_time(self, user_id: int) -> Optional[int]:
        """Get time in seconds until rate limit resets for user"""
        if user_id not in self.usage_history or not self.usage_history[user_id]:
            return None
        
        oldest_usage = min(self.usage_history[user_id])
        reset_time = oldest_usage + self.time_window
        current_time = time.time()
        
        return max(0, int(reset_time - current_time))
    
    def clear_user(self, user_id: int):
        """Clear rate limit history for a user"""
        if user_id in self.usage_history:
            del self.usage_history[user_id]

def is_url(text: str) -> bool:
    """Check if text contains a URL"""
    url_pattern = r'https?://(?:[-\w.])+(?:\:[0-9]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:\#(?:[\w.])*)?)?'
    return bool(re.search(url_pattern, text))

def extract_urls(text: str) -> List[str]:
    """Extract all URLs from text"""
    url_pattern = r'https?://(?:[-\w.])+(?:\:[0-9]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:\#(?:[\w.])*)?)?'
    return re.findall(url_pattern, text)

def format_user_list(users: List[Union[discord.Member, discord.User]], max_length: int = 1000) -> str:
    """Format a list of users for display"""
    if not users:
        return "None"
    
    user_strings = [f"{user.mention} (`{user.id}`)" for user in users]
    result = ", ".join(user_strings)
    
    if len(result) > max_length:
        # Truncate and add count
        truncated = result[:max_length - 20]
        last_comma = truncated.rfind(", ")
        if last_comma > 0:
            truncated = truncated[:last_comma]
        result = f"{truncated}... and {len(users) - len(truncated.split(', '))} more"
    
    return result

def validate_config_value(value: Any, expected_type: type, min_val: Any = None, max_val: Any = None) -> bool:
    """Validate a configuration value"""
    if not isinstance(value, expected_type):
        return False
    
    if expected_type in (int, float):
        if min_val is not None and value < min_val:
            return False
        if max_val is not None and value > max_val:
            return False
    elif expected_type == str:
        if min_val is not None and len(value) < min_val:
            return False
        if max_val is not None and len(value) > max_val:
            return False
    elif expected_type == list:
        if min_val is not None and len(value) < min_val:
            return False
        if max_val is not None and len(value) > max_val:
            return False
    
    return True

class ConfigValidator:
    """Validator for bot configuration"""
    
    @staticmethod
    def validate_bot_detection_config(config: Dict[str, Any]) -> List[str]:
        """Validate bot detection configuration"""
        errors = []
        
        # Check required fields
        if 'min_account_age_days' not in config:
            errors.append("Missing 'min_account_age_days' in bot_detection config")
        elif not validate_config_value(config['min_account_age_days'], int, 0, 365):
            errors.append("'min_account_age_days' must be an integer between 0 and 365")
        
        if 'action' not in config:
            errors.append("Missing 'action' in bot_detection config")
        elif config['action'] not in ['quarantine', 'kick', 'ban']:
            errors.append("'action' must be one of: quarantine, kick, ban")
        
        return errors
    
    @staticmethod
    def validate_spam_detection_config(config: Dict[str, Any]) -> List[str]:
        """Validate spam detection configuration"""
        errors = []
        
        if 'max_messages_per_window' not in config:
            errors.append("Missing 'max_messages_per_window' in spam_detection config")
        elif not validate_config_value(config['max_messages_per_window'], int, 1, 100):
            errors.append("'max_messages_per_window' must be an integer between 1 and 100")
        
        if 'time_window_seconds' not in config:
            errors.append("Missing 'time_window_seconds' in spam_detection config")
        elif not validate_config_value(config['time_window_seconds'], int, 1, 3600):
            errors.append("'time_window_seconds' must be an integer between 1 and 3600")
        
        if 'action' not in config:
            errors.append("Missing 'action' in spam_detection config")
        elif config['action'] not in ['timeout', 'kick', 'ban']:
            errors.append("'action' must be one of: timeout, kick, ban")
        
        return errors
    
    @staticmethod
    def validate_full_config(config: Dict[str, Any]) -> List[str]:
        """Validate entire bot configuration"""
        errors = []
        
        # Validate bot detection section
        if 'bot_detection' in config:
            errors.extend(ConfigValidator.validate_bot_detection_config(config['bot_detection']))
        
        # Validate spam detection section
        if 'spam_detection' in config:
            errors.extend(ConfigValidator.validate_spam_detection_config(config['spam_detection']))
        
        return errors

def log_command_usage(ctx, success: bool = True, error: str = None):
    """Log command usage for analytics"""
    logger.info(
        f"Command used: {ctx.command.name} by {ctx.author} ({ctx.author.id}) "
        f"in {ctx.guild.name if ctx.guild else 'DM'} - "
        f"{'Success' if success else f'Failed: {error}'}"
    )
