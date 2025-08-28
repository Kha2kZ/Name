import discord
from discord.ext import commands
import asyncio
import json
import os
import logging
from datetime import datetime, timedelta

from config import ConfigManager
from bot_detection import BotDetector
from spam_detection import SpamDetector
from moderation import ModerationTools
from logging_setup import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

class AntiSpamBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True
        intents.guild_messages = True
        
        super().__init__(
            command_prefix='?',
            intents=intents,
            help_command=None
        )
        
        # Initialize components
        self.config_manager = ConfigManager()
        self.bot_detector = BotDetector(self.config_manager)
        self.spam_detector = SpamDetector(self.config_manager)
        self.moderation = ModerationTools(self)
        
        # Track member joins for raid detection
        self.recent_joins = {}
        
    async def setup_hook(self):
        """Called when the bot is starting up"""
        logger.info("Bot is starting up...")
        
    async def on_ready(self):
        """Called when the bot is ready"""
        logger.info(f'{self.user} has connected to Discord!')
        logger.info(f'Bot is in {len(self.guilds)} guilds')
        
        # Set bot status
        await self.change_presence(
            status=discord.Status.online,
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="for suspicious activity"
            )
        )
        
    async def on_guild_join(self, guild):
        """Handle bot joining a new guild"""
        logger.info(f"Joined new guild: {guild.name} ({guild.id})")
        # Initialize configuration for new guild
        self.config_manager.initialize_guild_config(str(guild.id))
        
    async def on_member_join(self, member):
        """Handle new member joins"""
        guild_id = str(member.guild.id)
        config = self.config_manager.get_guild_config(guild_id)
        
        if not config['enabled']:
            return
            
        logger.info(f"New member joined {member.guild.name}: {member} ({member.id})")
        
        # Check for raid protection
        await self._check_raid_protection(member)
        
        # Run bot detection
        is_suspicious = await self.bot_detector.analyze_member(member)
        
        if is_suspicious:
            await self._handle_suspicious_member(member)
        elif config['verification']['enabled']:
            await self._start_verification(member)
            
    async def on_message(self, message):
        """Handle message events for spam detection"""
        if message.author.bot:
            await self.process_commands(message)
            return
            
        guild_id = str(message.guild.id) if message.guild else None
        if not guild_id:
            await self.process_commands(message)
            return
            
        config = self.config_manager.get_guild_config(guild_id)
        if not config['enabled']:
            await self.process_commands(message)
            return
            
        # Check for spam
        is_spam = await self.spam_detector.check_message(message)
        
        if is_spam:
            await self._handle_spam_message(message)
            return
            
        await self.process_commands(message)
        
    async def _check_raid_protection(self, member):
        """Check for mass join attacks"""
        guild_id = str(member.guild.id)
        config = self.config_manager.get_guild_config(guild_id)
        
        if not config['raid_protection']['enabled']:
            return
            
        now = datetime.utcnow()
        if guild_id not in self.recent_joins:
            self.recent_joins[guild_id] = []
            
        # Clean old joins
        cutoff = now - timedelta(seconds=config['raid_protection']['time_window'])
        self.recent_joins[guild_id] = [
            join_time for join_time in self.recent_joins[guild_id]
            if join_time > cutoff
        ]
        
        # Add current join
        self.recent_joins[guild_id].append(now)
        
        # Check if threshold exceeded
        if len(self.recent_joins[guild_id]) >= config['raid_protection']['max_joins']:
            await self._handle_raid_detected(member.guild)
            
    async def _handle_raid_detected(self, guild):
        """Handle detected raid"""
        logger.warning(f"Raid detected in {guild.name}")
        
        config = self.config_manager.get_guild_config(str(guild.id))
        action = config['raid_protection']['action']
        
        if action == 'lockdown':
            # Enable verification for all new members temporarily
            config['verification']['enabled'] = True
            self.config_manager.save_guild_config(str(guild.id), config)
            
        # Log the event
        await self._log_action(guild, "Raid Protection", f"Raid detected - {action} activated")
        
    async def _handle_suspicious_member(self, member):
        """Handle members flagged as suspicious"""
        guild_id = str(member.guild.id)
        config = self.config_manager.get_guild_config(guild_id)
        action = config['bot_detection']['action']
        
        logger.warning(f"Suspicious member detected: {member} in {member.guild.name}")
        
        if action == 'kick':
            await self.moderation.kick_member(member, "Suspicious bot-like behavior")
        elif action == 'ban':
            await self.moderation.ban_member(member, "Suspicious bot-like behavior")
        elif action == 'quarantine':
            await self.moderation.quarantine_member(member)
            
        await self._log_action(
            member.guild,
            "Bot Detection",
            f"Suspicious member {member} - Action: {action}"
        )
        
    async def _start_verification(self, member):
        """Start verification process for new member"""
        try:
            embed = discord.Embed(
                title="Welcome! Please verify your account",
                description="To gain access to the server, please react with ✅ below.",
                color=discord.Color.blue()
            )
            
            # Send DM to member
            dm_channel = await member.create_dm()
            message = await dm_channel.send(embed=embed)
            await message.add_reaction("✅")
            
            logger.info(f"Verification started for {member}")
            
        except discord.Forbidden:
            logger.warning(f"Could not send verification DM to {member}")
            
    async def _handle_spam_message(self, message):
        """Handle detected spam message"""
        logger.warning(f"Spam detected from {message.author} in {message.guild.name}")
        
        # Delete the message
        try:
            await message.delete()
        except discord.NotFound:
            pass
            
        # Apply action to user
        config = self.config_manager.get_guild_config(str(message.guild.id))
        action = config['spam_detection']['action']
        
        if action == 'timeout':
            await self.moderation.timeout_member(message.author, duration=300)  # 5 minutes
        elif action == 'kick':
            await self.moderation.kick_member(message.author, "Spamming")
        elif action == 'ban':
            await self.moderation.ban_member(message.author, "Spamming")
            
        await self._log_action(
            message.guild,
            "Spam Detection",
            f"Spam from {message.author} - Action: {action}"
        )
        
    async def _log_action(self, guild, action_type, description):
        """Log moderation actions"""
        guild_id = str(guild.id)
        config = self.config_manager.get_guild_config(guild_id)
        
        if not config['logging']['enabled']:
            return
            
        log_channel_id = config['logging']['channel_id']
        if not log_channel_id:
            return
            
        try:
            log_channel = guild.get_channel(int(log_channel_id))
            if log_channel:
                embed = discord.Embed(
                    title=f"🛡️ {action_type}",
                    description=description,
                    color=discord.Color.orange(),
                    timestamp=datetime.utcnow()
                )
                
                await log_channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Failed to log action: {e}")

# Main execution
async def main():
    """Main bot execution"""
    bot = AntiSpamBot()
    
    # Configuration commands
    @bot.group(name='antispam')
    @commands.has_permissions(administrator=True)
    async def antispam(ctx):
        """Anti-spam configuration commands"""
        if ctx.invoked_subcommand is None:
            embed = discord.Embed(
                title="Anti-Spam Bot Configuration",
                description="Use subcommands to configure the bot",
                color=discord.Color.blue()
            )
            embed.add_field(
                name="Commands", 
                value=(
                    "`?antispam config` - View current config\n"
                    "`?antispam enable/disable` - Toggle bot\n"
                    "`?antispam logchannel` - Set logging channel\n"
                    "`?antispam whitelist <user>` - Add user to whitelist\n"
                    "`?antispam stats` - View detection statistics"
                ), 
                inline=False
            )
            await ctx.send(embed=embed)
    
    @antispam.command(name='config')
    async def show_config(ctx):
        """Show current configuration"""
        config = bot.config_manager.get_guild_config(str(ctx.guild.id))
        
        embed = discord.Embed(
            title="Current Configuration",
            color=discord.Color.green()
        )
        
        embed.add_field(
            name="Status",
            value="Enabled" if config['enabled'] else "Disabled",
            inline=True
        )
        
        embed.add_field(
            name="Bot Detection",
            value=f"Action: {config['bot_detection']['action']}\nMin Age: {config['bot_detection']['min_account_age_days']} days",
            inline=True
        )
        
        embed.add_field(
            name="Spam Detection",
            value=f"Action: {config['spam_detection']['action']}\nMax Messages: {config['spam_detection']['max_messages_per_window']}",
            inline=True
        )
        
        await ctx.send(embed=embed)
    
    @antispam.command(name='enable')
    async def enable_bot(ctx):
        """Enable anti-spam protection"""
        config = bot.config_manager.get_guild_config(str(ctx.guild.id))
        config['enabled'] = True
        bot.config_manager.save_guild_config(str(ctx.guild.id), config)
        
        await ctx.send("✅ Anti-spam protection enabled!")
    
    @antispam.command(name='disable')
    async def disable_bot(ctx):
        """Disable anti-spam protection"""
        config = bot.config_manager.get_guild_config(str(ctx.guild.id))
        config['enabled'] = False
        bot.config_manager.save_guild_config(str(ctx.guild.id), config)
        
        await ctx.send("❌ Anti-spam protection disabled!")
    
    @antispam.command(name='logchannel')
    async def set_log_channel(ctx, channel: discord.TextChannel = None):
        """Set the logging channel"""
        if channel is None:
            channel = ctx.channel
            
        config = bot.config_manager.get_guild_config(str(ctx.guild.id))
        config['logging']['channel_id'] = str(channel.id)
        config['logging']['enabled'] = True
        bot.config_manager.save_guild_config(str(ctx.guild.id), config)
        
        await ctx.send(f"✅ Logging channel set to {channel.mention}")
    
    @antispam.command(name='whitelist')
    async def whitelist_user(ctx, member: discord.Member):
        """Add a user to the whitelist"""
        success = bot.bot_detector.add_to_whitelist(str(ctx.guild.id), str(member.id))
        if success:
            await ctx.send(f"✅ Added {member.mention} to whitelist")
        else:
            await ctx.send("❌ Failed to add user to whitelist")
    
    @antispam.command(name='stats')
    async def show_stats(ctx):
        """Show detection statistics"""
        embed = discord.Embed(
            title="Anti-Spam Statistics",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(name="Status", value="🟢 Active", inline=True)
        embed.add_field(name="Guild", value=ctx.guild.name, inline=True)
        embed.add_field(name="Members", value=str(ctx.guild.member_count), inline=True)
        
        await ctx.send(embed=embed)
    
    # Basic moderation commands
    @bot.command(name='kick')
    @commands.has_permissions(kick_members=True)
    async def kick_command(ctx, member: discord.Member, *, reason="No reason provided"):
        """Kick a member"""
        success = await bot.moderation.kick_member(member, reason)
        if success:
            await ctx.send(f"✅ Kicked {member.mention} - Reason: {reason}")
        else:
            await ctx.send("❌ Failed to kick member")
    
    @bot.command(name='ban')
    @commands.has_permissions(ban_members=True)
    async def ban_command(ctx, member: discord.Member, *, reason="No reason provided"):
        """Ban a member"""
        success = await bot.moderation.ban_member(member, reason)
        if success:
            await ctx.send(f"✅ Banned {member.mention} - Reason: {reason}")
        else:
            await ctx.send("❌ Failed to ban member")
    
    @bot.command(name='timeout')
    @commands.has_permissions(moderate_members=True)
    async def timeout_command(ctx, member: discord.Member, duration: int = 300, *, reason="No reason provided"):
        """Timeout a member (duration in seconds)"""
        success = await bot.moderation.timeout_member(member, duration, reason)
        if success:
            await ctx.send(f"✅ Timed out {member.mention} for {duration} seconds - Reason: {reason}")
        else:
            await ctx.send("❌ Failed to timeout member")
    
    @bot.command(name='quarantine')
    @commands.has_permissions(manage_roles=True)
    async def quarantine_command(ctx, member: discord.Member):
        """Quarantine a suspicious member"""
        success = await bot.moderation.quarantine_member(member)
        if success:
            await ctx.send(f"✅ Quarantined {member.mention}")
        else:
            await ctx.send("❌ Failed to quarantine member")
    
    # Error handling
    @bot.event
    async def on_command_error(ctx, error):
        """Handle command errors"""
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("❌ You don't have permission to use this command.")
        elif isinstance(error, commands.BotMissingPermissions):
            await ctx.send("❌ I don't have the required permissions to execute this command.")
        elif isinstance(error, commands.CommandNotFound):
            return  # Ignore command not found errors
        else:
            logger.error(f"Command error: {error}")
            await ctx.send("❌ An error occurred while executing the command.")
    
    # Get bot token from environment
    token = os.getenv('DISCORD_BOT_TOKEN')
    if not token:
        logger.error("DISCORD_BOT_TOKEN environment variable not set!")
        print("Please set the DISCORD_BOT_TOKEN environment variable")
        return
    
    try:
        await bot.start(token)
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == "__main__":
    asyncio.run(main())
