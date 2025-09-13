import discord
import logging
from datetime import datetime, timedelta
from typing import Optional, Union

logger = logging.getLogger(__name__)

class ModerationTools:
    def __init__(self, bot):
        self.bot = bot
        
    async def kick_member(self, member: discord.Member, reason: str = "No reason provided") -> bool:
        """Kick a member from the guild"""
        try:
            await member.kick(reason=reason)
            logger.info(f"Kicked {member} from {member.guild.name}: {reason}")
            
            # Log the action
            await self._log_moderation_action(
                member.guild,
                "Kick",
                member,
                reason,
                self.bot.user
            )
            
            # Send DM to user if possible
            await self._send_moderation_dm(member, "kicked", reason, member.guild.name)
            
            return True
            
        except discord.Forbidden:
            logger.error(f"No permission to kick {member}")
            return False
        except discord.HTTPException as e:
            logger.error(f"Failed to kick {member}: {e}")
            return False
    
    async def ban_member(self, member: Union[discord.Member, discord.User], reason: str = "No reason provided", delete_message_days: int = 1) -> bool:
        """Ban a member from the guild"""
        try:
            if isinstance(member, discord.Member):
                guild = member.guild
                # Send DM before banning
                await self._send_moderation_dm(member, "banned", reason, guild.name)
                await guild.ban(member, reason=reason, delete_message_days=delete_message_days)
            else:
                # User object (for ban by ID)
                guild = self.bot.guilds[0] if self.bot.guilds else None
                if guild:
                    await guild.ban(member, reason=reason, delete_message_days=delete_message_days)
                else:
                    return False
            
            logger.info(f"Banned {member} from {guild.name}: {reason}")
            
            # Log the action
            await self._log_moderation_action(
                guild,
                "Ban",
                member,
                reason,
                self.bot.user
            )
            
            return True
            
        except discord.Forbidden:
            logger.error(f"No permission to ban {member}")
            return False
        except discord.HTTPException as e:
            logger.error(f"Failed to ban {member}: {e}")
            return False
    
    async def timeout_member(self, member: discord.Member, duration: int, reason: str = "No reason provided") -> bool:
        """Timeout a member (duration in seconds)"""
        try:
            # Convert duration to timedelta  
            timeout_until = discord.utils.utcnow() + timedelta(seconds=duration)
            
            await member.timeout(timeout_until, reason=reason)
            logger.info(f"Timed out {member} for {duration} seconds: {reason}")
            
            # Log the action
            await self._log_moderation_action(
                member.guild,
                "Timeout",
                member,
                f"{reason} (Duration: {duration} seconds)",
                self.bot.user
            )
            
            # Send DM to user
            await self._send_moderation_dm(member, "timed out", f"{reason} (Duration: {duration} seconds)", member.guild.name)
            
            return True
            
        except discord.Forbidden:
            logger.error(f"No permission to timeout {member}")
            return False
        except discord.HTTPException as e:
            logger.error(f"Failed to timeout {member}: {e}")
            return False
    
    async def quarantine_member(self, member: discord.Member) -> bool:
        """Quarantine a member by removing roles and restricting access"""
        try:
            guild_id = str(member.guild.id)
            config = self.bot.config_manager.get_guild_config(guild_id)
            
            # Create or get quarantine role
            quarantine_role = await self._get_or_create_quarantine_role(member.guild)
            if not quarantine_role:
                logger.error(f"Could not create quarantine role in {member.guild.name}")
                return False
            
            # Store original roles (except @everyone)
            original_roles = [role for role in member.roles if role != member.guild.default_role]
            
            # Remove all roles except @everyone
            if original_roles:
                try:
                    await member.remove_roles(*original_roles, reason="Quarantined for suspicious activity")
                except discord.Forbidden:
                    logger.warning(f"Could not remove some roles from {member}")
            
            # Add quarantine role
            await member.add_roles(quarantine_role, reason="Quarantined for suspicious activity")
            
            logger.info(f"Quarantined {member} in {member.guild.name}")
            
            # Log the action
            await self._log_moderation_action(
                member.guild,
                "Quarantine",
                member,
                "Suspicious activity detected",
                self.bot.user
            )
            
            # Send DM to user
            await self._send_moderation_dm(
                member, 
                "quarantined", 
                "Suspicious activity detected. Please contact server administrators.", 
                member.guild.name
            )
            
            return True
            
        except discord.Forbidden:
            logger.error(f"No permission to quarantine {member}")
            return False
        except discord.HTTPException as e:
            logger.error(f"Failed to quarantine {member}: {e}")
            return False
    
    async def remove_quarantine(self, member: discord.Member) -> bool:
        """Remove quarantine from a member and restore their roles"""
        try:
            guild_id = str(member.guild.id)
            
            # Find quarantine role
            quarantine_role = discord.utils.get(member.guild.roles, name="Quarantined")
            if not quarantine_role:
                logger.warning(f"No quarantine role found in {member.guild.name}")
                return True  # Consider it successful if no quarantine role exists
            
            # Remove quarantine role
            if quarantine_role in member.roles:
                await member.remove_roles(quarantine_role, reason="Verification completed")
                logger.info(f"Removed quarantine from {member} in {member.guild.name}")
                
                # Log the action
                await self._log_moderation_action(
                    member.guild,
                    "Unquarantine",
                    member,
                    "Verification completed successfully",
                    self.bot.user
                )
                
                return True
            else:
                logger.info(f"{member} was not quarantined")
                return True
                
        except discord.Forbidden:
            logger.error(f"No permission to remove quarantine from {member}")
            return False
        except discord.HTTPException as e:
            logger.error(f"Failed to remove quarantine from {member}: {e}")
            return False
    
    async def _get_or_create_quarantine_role(self, guild: discord.Guild) -> Optional[discord.Role]:
        """Get or create quarantine role"""
        # Look for existing quarantine role
        quarantine_role = discord.utils.get(guild.roles, name="Quarantined")
        
        if not quarantine_role:
            try:
                # Create quarantine role with restricted permissions
                quarantine_role = await guild.create_role(
                    name="Quarantined",
                    permissions=discord.Permissions(read_messages=True, send_messages=False, speak=False),
                    reason="Anti-bot quarantine role"
                )
                
                # Set channel permissions for quarantine role
                for channel in guild.channels:
                    try:
                        if isinstance(channel, discord.TextChannel):
                            await channel.set_permissions(
                                quarantine_role,
                                send_messages=False,
                                add_reactions=False,
                                create_public_threads=False,
                                create_private_threads=False
                            )
                        elif isinstance(channel, discord.VoiceChannel):
                            await channel.set_permissions(
                                quarantine_role,
                                speak=False,
                                connect=False
                            )
                    except discord.Forbidden:
                        continue
                        
            except discord.Forbidden:
                logger.error(f"No permission to create quarantine role in {guild.name}")
                return None
        
        return quarantine_role
    
    async def _send_moderation_dm(self, member: discord.Member, action: str, reason: str, guild_name: str):
        """Send a DM to inform user about moderation action"""
        try:
            embed = discord.Embed(
                title=f"Moderation Action: {action.title()}",
                description=f"You have been **{action}** from **{guild_name}**",
                color=discord.Color.red(),
                timestamp=datetime.utcnow()
            )
            
            embed.add_field(name="Reason", value=f"**{reason}**", inline=False)
            
            if action in ["warned", "timed out"]:
                embed.add_field(
                    name="Next Steps", 
                    value="Please review the server rules and contact moderators if you have questions.",
                    inline=False
                )
            elif action == "kicked":
                embed.add_field(
                    name="Rejoining", 
                    value="You may rejoin the server, but please ensure you follow the rules.",
                    inline=False
                )
            elif action == "quarantined":
                embed.add_field(
                    name="What to do",
                    value="Contact server administrators to resolve this issue.",
                    inline=False
                )
            
            dm_channel = await member.create_dm()
            await dm_channel.send(embed=embed)
            
        except discord.Forbidden:
            logger.warning(f"Could not send DM to {member}")
        except Exception as e:
            logger.error(f"Error sending DM to {member}: {e}")
    
    async def _log_moderation_action(self, guild: discord.Guild, action: str, target: Union[discord.Member, discord.User], reason: str, moderator: discord.User):
        """Log moderation action to configured log channel"""
        try:
            guild_id = str(guild.id)
            config = self.bot.config_manager.get_guild_config(guild_id)
            
            if not config['logging']['enabled']:
                return
                
            log_channel_id = config['logging']['channel_id']
            if not log_channel_id:
                return
                
            log_channel = guild.get_channel(int(log_channel_id))
            if not log_channel or not isinstance(log_channel, discord.TextChannel):
                return
            
            # Create embed
            embed = discord.Embed(
                title=f"🔨 {action}",
                color=self._get_action_color(action),
                timestamp=datetime.utcnow()
            )
            
            embed.add_field(name="Target", value=f"{target} (`{target.id}`)", inline=True)
            embed.add_field(name="Moderator", value=f"{moderator} (`{moderator.id}`)", inline=True)
            embed.add_field(name="Reason", value=f"**{reason}**", inline=False)
            
            if isinstance(target, discord.Member):
                embed.add_field(name="Account Created", value=target.created_at.strftime("%Y-%m-%d %H:%M:%S UTC"), inline=True)
                if target.joined_at:
                    embed.add_field(name="Joined Server", value=target.joined_at.strftime("%Y-%m-%d %H:%M:%S UTC"), inline=True)
            await log_channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to log moderation action: {e}")
    
    def _get_action_color(self, action: str) -> discord.Color:
        """Get color for moderation action"""
        colors = {
            "Kick": discord.Color.orange(),
            "Ban": discord.Color.red(),
            "Timeout": discord.Color.yellow(),
            "Quarantine": discord.Color.purple(),
            "Unquarantine": discord.Color.green()
        }
        return colors.get(action, discord.Color.greyple())
