"""
Discord Logging System
Handles logging of moderation actions and events to Discord channels
"""

import discord
import logging
from datetime import datetime
from typing import Optional, Union
import json
import os

logger = logging.getLogger(__name__)

class BotLogger:
    def __init__(self, bot, config_manager):
        self.bot = bot
        self.config_manager = config_manager
        
    async def log_event(self, guild_id: int, event_type: str, description: str, user: Optional[discord.User] = None):
        """Log a general event"""
        config = self.config_manager.get_guild_config(guild_id)
        
        if not config['logging']['enabled']:
            return
            
        # Log to file
        self._log_to_file(guild_id, event_type, description, user)
        
        # Log to Discord channel if configured
        if event_type in ['member_join', 'member_leave'] and not config['logging'].get(f'log_{event_type.split("_")[1]}s', True):
            return
            
        await self._log_to_discord(guild_id, event_type, description, user)
    
    async def log_action(self, guild_id: int, action: str, description: str, moderator: discord.User, target: Optional[discord.User] = None):
        """Log a moderation action"""
        config = self.config_manager.get_guild_config(guild_id)
        
        if not config['logging']['enabled'] or not config['logging'].get('log_moderation', True):
            return
            
        # Log to file
        self._log_to_file(guild_id, f'action_{action}', description, moderator, target)
        
        # Log to Discord channel
        await self._log_action_to_discord(guild_id, action, description, moderator, target)
    
    def _log_to_file(self, guild_id: int, event_type: str, description: str, user: Optional[discord.User] = None, target: Optional[discord.User] = None):
        """Log event to file"""
        try:
            log_dir = "logs"
            os.makedirs(log_dir, exist_ok=True)
            
            log_file = os.path.join(log_dir, f"guild_{guild_id}.log")
            
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            user_info = f" by {user} ({user.id})" if user else ""
            target_info = f" targeting {target} ({target.id})" if target else ""
            
            log_entry = f"[{timestamp}] {event_type.upper()}: {description}{user_info}{target_info}\n"
            
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry)
                
        except Exception as e:
            logger.error(f"Failed to write to log file: {e}")
    
    async def _log_to_discord(self, guild_id: int, event_type: str, description: str, user: Optional[discord.User] = None):
        """Log event to Discord channel"""
        try:
            config = self.config_manager.get_guild_config(guild_id)
            channel_id = config['logging'].get('channel_id')
            
            if not channel_id:
                return
                
            guild = self.bot.get_guild(guild_id)
            if not guild:
                return
                
            log_channel = guild.get_channel(int(channel_id))
            if not log_channel:
                return
            
            # Create embed based on event type
            embed = self._create_event_embed(event_type, description, user)
            
            await log_channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to log to Discord channel: {e}")
    
    async def _log_action_to_discord(self, guild_id: int, action: str, description: str, moderator: discord.User, target: Optional[discord.User] = None):
        """Log moderation action to Discord channel"""
        try:
            config = self.config_manager.get_guild_config(guild_id)
            channel_id = config['logging'].get('channel_id')
            
            if not channel_id:
                return
                
            guild = self.bot.get_guild(guild_id)
            if not guild:
                return
                
            log_channel = guild.get_channel(int(channel_id))
            if not log_channel:
                return
            
            # Create embed for moderation action
            embed = self._create_action_embed(action, description, moderator, target)
            
            await log_channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to log action to Discord channel: {e}")
    
    def _create_event_embed(self, event_type: str, description: str, user: Optional[discord.User] = None) -> discord.Embed:
        """Create embed for general events"""
        color_map = {
            'member_join': discord.Color.green(),
            'member_leave': discord.Color.orange(),
            'verification_applied': discord.Color.blue(),
            'raid_detected': discord.Color.red(),
            'spam_detected': discord.Color.yellow()
        }
        
        emoji_map = {
            'member_join': '📥',
            'member_leave': '📤',
            'verification_applied': '🔐',
            'raid_detected': '🚨',
            'spam_detected': '🚫'
        }
        
        color = color_map.get(event_type, discord.Color.greyple())
        emoji = emoji_map.get(event_type, '📝')
        
        embed = discord.Embed(
            title=f"{emoji} {event_type.replace('_', ' ').title()}",
            description=f"**{description}**",
            color=color,
            timestamp=datetime.utcnow()
        )
        
        if user:
            embed.add_field(name="User", value=f"{user.mention}\n`{user.id}`", inline=True)
            if hasattr(user, 'created_at'):
                account_age = (datetime.utcnow() - user.created_at.replace(tzinfo=None)).days
                embed.add_field(name="Account Age", value=f"**{account_age} days**", inline=True)
        return embed
    
    def _create_action_embed(self, action: str, description: str, moderator: discord.User, target: Optional[discord.User] = None) -> discord.Embed:
        """Create embed for moderation actions"""
        color_map = {
            'kick': discord.Color.orange(),
            'ban': discord.Color.red(),
            'unban': discord.Color.green(),
            'timeout': discord.Color.yellow(),
            'untimeout': discord.Color.green(),
            'mute': discord.Color.dark_grey(),
            'unmute': discord.Color.green(),
            'quarantine': discord.Color.purple(),
            'auto_ban': discord.Color.dark_red(),
            'auto_kick': discord.Color.dark_orange(),
            'auto_timeout': discord.Color.gold(),
            'spam_delete': discord.Color.yellow(),
            'purge': discord.Color.blue(),
            'manual_verification': discord.Color.green()
        }
        
        emoji_map = {
            'kick': '👢',
            'ban': '🔨',
            'unban': '🔓',
            'timeout': '⏰',
            'untimeout': '⏰',
            'mute': '🔇',
            'unmute': '🔊',
            'quarantine': '🚧',
            'auto_ban': '🤖🔨',
            'auto_kick': '🤖👢',
            'auto_timeout': '🤖⏰',
            'spam_delete': '🗑️',
            'purge': '🧹',
            'manual_verification': '✅'
        }
        
        color = color_map.get(action, discord.Color.greyple())
        emoji = emoji_map.get(action, '⚖️')
        
        embed = discord.Embed(
            title=f"{emoji} {action.replace('_', ' ').title()}",
            description=f"**{description}**",
            color=color,
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(name="Moderator", value=f"{moderator.mention}\n`{moderator.id}`", inline=True)
        
        if target:
            embed.add_field(name="Target", value=f"{target.mention}\n`{target.id}`", inline=True)
            if hasattr(target, 'created_at'):
                account_age = (datetime.utcnow() - target.created_at.replace(tzinfo=None)).days
                embed.add_field(name="Target Account Age", value=f"**{account_age} days**", inline=True)
        return embed
    
    async def get_recent_logs(self, guild_id: int, limit: int = 10) -> list:
        """Get recent log entries from file"""
        try:
            log_file = os.path.join("logs", f"guild_{guild_id}.log")
            
            if not os.path.exists(log_file):
                return []
            
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Return last 'limit' lines
            return lines[-limit:] if len(lines) > limit else lines
            
        except Exception as e:
            logger.error(f"Failed to read log file: {e}")
            return []
    
    async def clear_logs(self, guild_id: int) -> bool:
        """Clear log file for a guild"""
        try:
            log_file = os.path.join("logs", f"guild_{guild_id}.log")
            
            if os.path.exists(log_file):
                os.remove(log_file)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear log file: {e}")
            return False
    
    async def export_logs(self, guild_id: int, format: str = 'txt') -> Optional[str]:
        """Export logs in specified format"""
        try:
            log_file = os.path.join("logs", f"guild_{guild_id}.log")
            
            if not os.path.exists(log_file):
                return None
            
            if format.lower() == 'json':
                return await self._export_logs_json(guild_id)
            else:
                # Return text format
                with open(log_file, 'r', encoding='utf-8') as f:
                    return f.read()
                    
        except Exception as e:
            logger.error(f"Failed to export logs: {e}")
            return None
    
    async def _export_logs_json(self, guild_id: int) -> str:
        """Export logs in JSON format"""
        try:
            log_file = os.path.join("logs", f"guild_{guild_id}.log")
            logs = []
            
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        # Parse log line (basic parsing)
                        parts = line.strip().split('] ', 1)
                        if len(parts) == 2:
                            timestamp_part = parts[0][1:]  # Remove leading [
                            content_part = parts[1]
                            
                            log_entry = {
                                'timestamp': timestamp_part,
                                'content': content_part
                            }
                            logs.append(log_entry)
            
            return json.dumps(logs, indent=2)
            
        except Exception as e:
            logger.error(f"Failed to export logs as JSON: {e}")
            return "[]"

