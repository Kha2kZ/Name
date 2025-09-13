import discord
import re
import time
from datetime import datetime, timedelta
from typing import List, Optional, Union

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
    """Parse duration string to seconds"""
    if not duration_str:
        return None
    
    # Remove spaces and convert to lowercase
    duration_str = duration_str.replace(" ", "").lower()
    
    # Regex pattern to match number followed by time unit
    pattern = r'(\d+)([smhd])'
    matches = re.findall(pattern, duration_str)
    
    if not matches:
        return None
    
    total_seconds = 0
    multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    
    for amount, unit in matches:
        total_seconds += int(amount) * multipliers.get(unit, 0)
    
    return total_seconds if total_seconds > 0 else None

def is_valid_discord_invite(url: str) -> bool:
    """Check if a URL is a Discord invite"""
    discord_invite_pattern = r'(?:https?://)?(?:www\.)?(?:discord\.gg|discordapp\.com/invite)/[a-zA-Z0-9]+'
    return bool(re.match(discord_invite_pattern, url))

def extract_user_id(user_str: str) -> Optional[int]:
    """Extract user ID from mention or ID string"""
    # Check if it's a mention
    mention_pattern = r'<@!?(\d+)>'
    match = re.match(mention_pattern, user_str)
    if match:
        return int(match.group(1))
    
    # Check if it's just a user ID
    if user_str.isdigit():
        return int(user_str)
    
    return None

def get_member_info_embed(member: discord.Member) -> discord.Embed:
    """Create an embed with member information"""
    embed = discord.Embed(
        title=f"Member Info: {member.display_name}",
        color=member.color if member.color != discord.Color.default() else discord.Color.blue()
    )
    
    if member.avatar:
        embed.set_thumbnail(url=member.avatar.url)
    
    embed.add_field(
        name="Username", 
        value=f"{member.name}#{member.discriminator}", 
        inline=True
    )
    embed.add_field(name="ID", value=str(member.id), inline=True)
    embed.add_field(name="Bot", value="Yes" if member.bot else "No", inline=True)
    
    # Account creation
    account_age = datetime.utcnow() - member.created_at.replace(tzinfo=None)
    embed.add_field(
        name="Account Created", 
        value=f"{member.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}\n({account_age.days} days ago)", 
        inline=True
    )
    
    # Server join date
    if member.joined_at:
        join_age = datetime.utcnow() - member.joined_at.replace(tzinfo=None)
        embed.add_field(
            name="Joined Server", 
            value=f"{member.joined_at.strftime('%Y-%m-%d %H:%M:%S UTC')}\n({join_age.days} days ago)", 
            inline=True
        )
    
    # Roles
    if len(member.roles) > 1:  # Exclude @everyone
        roles = [role.mention for role in member.roles[1:]]  # Skip @everyone
        roles_text = ", ".join(roles[:10])  # Limit to 10 roles
        if len(member.roles) > 11:
            roles_text += f" and {len(member.roles) - 11} more..."
        embed.add_field(name="Roles", value=roles_text, inline=False)
    
    return embed

def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """Split a list into chunks of specified size"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def sanitize_filename(filename: str) -> str:
    """Sanitize a string to be safe for use as a filename"""
    # Remove invalid characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '')
    
    # Remove leading/trailing spaces and dots
    filename = filename.strip('. ')
    
    # Limit length
    if len(filename) > 255:
        filename = filename[:255]
    
    return filename or "unnamed"

def get_permission_level(member: discord.Member) -> str:
    """Get the permission level of a member"""
    if member.guild_permissions.administrator:
        return "Administrator"
    elif member.guild_permissions.manage_guild:
        return "Moderator"
    elif member.guild_permissions.kick_members or member.guild_permissions.ban_members:
        return "Staff"
    else:
        return "Member"

def format_permissions(permissions: discord.Permissions) -> List[str]:
    """Format permissions object into readable list"""
    perm_list = []
    
    important_perms = [
        ('administrator', 'Administrator'),
        ('manage_guild', 'Manage Server'),
        ('manage_roles', 'Manage Roles'),
        ('manage_channels', 'Manage Channels'),
        ('kick_members', 'Kick Members'),
        ('ban_members', 'Ban Members'),
        ('manage_messages', 'Manage Messages'),
        ('moderate_members', 'Timeout Members'),
        ('mention_everyone', 'Mention Everyone'),
        ('manage_webhooks', 'Manage Webhooks')
    ]
    
    for perm_name, display_name in important_perms:
        if getattr(permissions, perm_name):
            perm_list.append(display_name)
    
    return perm_list

class RateLimiter:
    """Simple rate limiter for commands"""
    
    def __init__(self, max_uses: int, time_window: int):
        self.max_uses = max_uses
        self.time_window = time_window
        self.usage_history = {}
    
    def is_rate_limited(self, user_id: int) -> bool:
        """Check if user is rate limited"""
        current_time = time.time()
        
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
        """Get time until rate limit resets"""
        if user_id not in self.usage_history or not self.usage_history[user_id]:
            return None
        
        oldest_usage = min(self.usage_history[user_id])
        reset_time = oldest_usage + self.time_window
        current_time = time.time()
        
        if reset_time > current_time:
            return int(reset_time - current_time)
        return None

def create_error_embed(title: str, description: str) -> discord.Embed:
    """Create a standardized error embed"""
    embed = discord.Embed(
        title=f"❌ {title}",
        description=f"**{description}**",
        color=discord.Color.red()
    )
    return embed

def create_success_embed(title: str, description: str) -> discord.Embed:
    """Create a standardized success embed"""
    embed = discord.Embed(
        title=f"✅ {title}",
        description=f"**{description}**",
        color=discord.Color.green()
    )
    return embed

def create_warning_embed(title: str, description: str) -> discord.Embed:
    """Create a standardized warning embed"""
    embed = discord.Embed(
        title=f"⚠️ {title}",
        description=f"**{description}**",
        color=discord.Color.orange()
    )
    return embed
