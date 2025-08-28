import discord
from discord.ext import commands
import asyncio
import json
import os
import logging
import random
import string
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
        
        # Track pending verifications
        self.pending_verifications = {}
        
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
        """Handle message events for spam detection and verification"""
        if message.author.bot:
            await self.process_commands(message)
            return
        
        # Handle DM verification responses
        if isinstance(message.channel, discord.DMChannel):
            await self._handle_verification_response(message)
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
        """Start captcha verification process for new member"""
        try:
            # Generate simple math captcha
            num1 = random.randint(1, 10)
            num2 = random.randint(1, 10)
            answer = num1 + num2
            
            # Store the verification data
            verification_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
            self.pending_verifications[member.id] = {
                'answer': answer,
                'verification_id': verification_id,
                'attempts': 0,
                'timestamp': datetime.utcnow()
            }
            
            embed = discord.Embed(
                title="🔐 Account Verification Required",
                description=f"Welcome to **{member.guild.name}**!\n\n🤖 To verify you're human and gain access to the server, please solve this simple math problem:",
                color=0x5865f2
            )
            embed.add_field(
                name="📊 Math Challenge", 
                value=f"**What is {num1} + {num2}?**\n\nReply with just the number (e.g., `{answer}`)", 
                inline=False
            )
            embed.add_field(
                name="⏰ Time Limit", 
                value="You have 5 minutes to complete verification", 
                inline=True
            )
            embed.add_field(
                name="🆔 Verification ID", 
                value=f"`{verification_id}`", 
                inline=True
            )
            embed.set_footer(text="AntiBot Protection • Reply with the answer to this DM")
            
            # Send DM to member
            dm_channel = await member.create_dm()
            await dm_channel.send(embed=embed)
            
            # Apply quarantine role temporarily
            await self.moderation.quarantine_member(member)
            
            logger.info(f"Captcha verification started for {member} - Answer: {answer}")
            
            # Set timeout to remove verification after 5 minutes
            asyncio.create_task(self._verification_timeout(member.id, dm_channel, member))
            
        except discord.Forbidden:
            logger.warning(f"Could not send verification DM to {member}")
            # If can't DM, don't quarantine - might be a legitimate user with DMs disabled
        except Exception as e:
            logger.error(f"Error starting verification for {member}: {e}")
            
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
    
    async def _handle_verification_response(self, message):
        """Handle verification responses in DMs"""
        user_id = message.author.id
        
        if user_id not in self.pending_verifications:
            return
        
        verification_data = self.pending_verifications[user_id]
        
        try:
            user_answer = int(message.content.strip())
            correct_answer = verification_data['answer']
            
            if user_answer == correct_answer:
                # Correct answer - verify the user
                del self.pending_verifications[user_id]
                
                # Find the member in all guilds
                member = None
                for guild in self.guilds:
                    member = guild.get_member(user_id)
                    if member:
                        break
                
                if member:
                    # Remove quarantine
                    await self.moderation.remove_quarantine(member)
                    
                    success_embed = discord.Embed(
                        title="✅ Verification Successful!",
                        description=f"Welcome to **{member.guild.name}**!\n\n🎉 You now have full access to the server.",
                        color=0x00ff88
                    )
                    success_embed.set_footer(text="Thank you for keeping our server safe!")
                    await message.channel.send(embed=success_embed)
                    
                    # Log successful verification
                    await self._log_action(
                        member.guild,
                        "Verification",
                        f"✅ {member} successfully completed captcha verification"
                    )
                    
                    logger.info(f"User {member} successfully verified")
            else:
                # Wrong answer
                verification_data['attempts'] += 1
                
                if verification_data['attempts'] >= 3:
                    # Too many failed attempts
                    del self.pending_verifications[user_id]
                    
                    fail_embed = discord.Embed(
                        title="❌ Verification Failed",
                        description="Too many incorrect attempts. You will be removed from the server.\n\nIf you believe this is an error, please contact server administrators.",
                        color=0xff4444
                    )
                    await message.channel.send(embed=fail_embed)
                    
                    # Find and kick the member
                    for guild in self.guilds:
                        member = guild.get_member(user_id)
                        if member:
                            await self.moderation.kick_member(member, "Failed captcha verification (3 attempts)")
                            await self._log_action(
                                guild,
                                "Verification",
                                f"❌ {member} failed captcha verification (3 attempts)"
                            )
                            break
                else:
                    # Give another chance
                    attempts_left = 3 - verification_data['attempts']
                    retry_embed = discord.Embed(
                        title="❌ Incorrect Answer",
                        description=f"That's not correct. You have **{attempts_left}** attempts remaining.\n\nPlease try again with just the number.",
                        color=0xffa500
                    )
                    await message.channel.send(embed=retry_embed)
                    
        except ValueError:
            # Not a number
            error_embed = discord.Embed(
                title="⚠️ Invalid Response",
                description="Please respond with just the number (e.g., `15`).\n\nDon't include any other text.",
                color=0xffa500
            )
            await message.channel.send(embed=error_embed)
        except Exception as e:
            logger.error(f"Error handling verification response: {e}")
    
    async def _verification_timeout(self, user_id: int, dm_channel, member: discord.Member):
        """Handle verification timeout after 5 minutes"""
        await asyncio.sleep(300)  # 5 minutes
        if user_id in self.pending_verifications:
            del self.pending_verifications[user_id]
            try:
                fail_embed = discord.Embed(
                    title="⏰ Verification Timeout",
                    description="Your verification has expired. Please rejoin the server to try again.",
                    color=0xff4444
                )
                await dm_channel.send(embed=fail_embed)
                await self.moderation.kick_member(member, "Failed to complete verification within time limit")
            except Exception as e:
                logger.error(f"Error handling verification timeout: {e}")
        
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
                action_colors = {
                    "Bot Detection": 0xff6b6b,
                    "Spam Detection": 0xffa726,
                    "Raid Protection": 0xff5722,
                    "Verification": 0x5865f2
                }
                action_icons = {
                    "Bot Detection": "🤖",
                    "Spam Detection": "🚫",
                    "Raid Protection": "⚡",
                    "Verification": "🔐"
                }
                
                embed = discord.Embed(
                    title=f"{action_icons.get(action_type, '🛡️')} {action_type}",
                    description=f"**Security Alert**\n{description}",
                    color=action_colors.get(action_type, 0xff9500),
                    timestamp=datetime.utcnow()
                )
                embed.set_footer(text="AntiBot Protection System", icon_url=guild.me.display_avatar.url if guild.me else None)
                
                # Add verification to action colors/icons
                if action_type == "Verification":
                    embed.color = 0x00ff88 if "✅" in description else 0xff4444
                    embed.title = f"🔐 {action_type}"
                
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
                title="🛡️ Anti-Bot Protection System",
                description="⚙️ **Configure your server's protection settings**\n\n🔧 Use the commands below to customize detection and responses",
                color=0x2b2d31
            )
            embed.set_thumbnail(url="https://cdn.discordapp.com/emojis/1234567890123456789.png")
            embed.add_field(
                name="Commands", 
                value=(
                    "📊 `?antispam config` - View current settings\n"
                    "🔄 `?antispam enable/disable` - Toggle protection\n"
                    "📝 `?antispam logchannel` - Set logging channel\n"
                    "✅ `?antispam whitelist <user>` - Trust a user\n"
                    "📈 `?antispam stats` - View server statistics"
                ), 
                inline=False
            )
            await ctx.send(embed=embed)
    
    @antispam.command(name='config')
    async def show_config(ctx):
        """Show current configuration"""
        config = bot.config_manager.get_guild_config(str(ctx.guild.id))
        
        embed = discord.Embed(
            title="📊 Server Protection Status",
            description=f"🏛️ **{ctx.guild.name}** security configuration",
            color=0x00ff88
        )
        embed.set_footer(text=f"Requested by {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url)
        
        status_emoji = "🟢" if config['enabled'] else "🔴"
        status_text = "**ACTIVE**" if config['enabled'] else "**DISABLED**"
        embed.add_field(
            name="🛡️ Protection Status",
            value=f"{status_emoji} {status_text}",
            inline=True
        )
        
        action_emoji = {"kick": "👢", "ban": "🔨", "quarantine": "🔒"}.get(config['bot_detection']['action'], "⚠️")
        embed.add_field(
            name="🤖 Bot Detection",
            value=f"{action_emoji} **Action:** {config['bot_detection']['action'].title()}\n📅 **Min Age:** {config['bot_detection']['min_account_age_days']} days",
            inline=True
        )
        
        spam_emoji = {"timeout": "⏰", "kick": "👢", "ban": "🔨"}.get(config['spam_detection']['action'], "⚠️")
        embed.add_field(
            name="🚫 Spam Detection",
            value=f"{spam_emoji} **Action:** {config['spam_detection']['action'].title()}\n💬 **Max Messages:** {config['spam_detection']['max_messages_per_window']}",
            inline=True
        )
        
        await ctx.send(embed=embed)
    
    @antispam.command(name='enable')
    async def enable_bot(ctx):
        """Enable anti-spam protection"""
        config = bot.config_manager.get_guild_config(str(ctx.guild.id))
        config['enabled'] = True
        bot.config_manager.save_guild_config(str(ctx.guild.id), config)
        
        embed = discord.Embed(
            title="🟢 Protection Activated",
            description="🛡️ **Anti-bot protection is now ACTIVE**\n\nYour server is now protected from:\n🤖 Malicious bots\n🚫 Spam attacks\n⚡ Mass raids",
            color=0x00ff88
        )
        await ctx.send(embed=embed)
    
    @antispam.command(name='disable')
    async def disable_bot(ctx):
        """Disable anti-spam protection"""
        config = bot.config_manager.get_guild_config(str(ctx.guild.id))
        config['enabled'] = False
        bot.config_manager.save_guild_config(str(ctx.guild.id), config)
        
        embed = discord.Embed(
            title="🔴 Protection Disabled",
            description="⚠️ **Anti-bot protection is now INACTIVE**\n\nYour server is no longer protected.\nUse `?antispam enable` to reactivate.",
            color=0xff4444
        )
        await ctx.send(embed=embed)
    
    @antispam.command(name='logchannel')
    async def set_log_channel(ctx, channel: discord.TextChannel = None):
        """Set the logging channel"""
        if channel is None:
            channel = ctx.channel
            
        config = bot.config_manager.get_guild_config(str(ctx.guild.id))
        config['logging']['channel_id'] = str(channel.id)
        config['logging']['enabled'] = True
        bot.config_manager.save_guild_config(str(ctx.guild.id), config)
        
        embed = discord.Embed(
            title="📝 Logging Channel Updated",
            description=f"📍 **Channel:** {channel.mention}\n\n🔍 All moderation actions will be logged here",
            color=0x5865f2
        )
        await ctx.send(embed=embed)
    
    @antispam.command(name='whitelist')
    async def whitelist_user(ctx, member: discord.Member):
        """Add a user to the whitelist"""
        success = bot.bot_detector.add_to_whitelist(str(ctx.guild.id), str(member.id))
        if success:
            embed = discord.Embed(
                title="✅ User Whitelisted",
                description=f"🛡️ **{member.display_name}** is now trusted\n\nThey will bypass all detection systems.",
                color=0x00ff88
            )
            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ Failed to add user to whitelist")
    
    @antispam.command(name='verification')
    async def toggle_verification(ctx, enabled: bool = None):
        """Enable or disable captcha verification for new members"""
        config = bot.config_manager.get_guild_config(str(ctx.guild.id))
        
        if enabled is None:
            # Show current status
            status = "🟢 ENABLED" if config['verification']['enabled'] else "🔴 DISABLED"
            embed = discord.Embed(
                title="🔐 Captcha Verification Status",
                description=f"**Current Status:** {status}\n\n📝 Use `?antispam verification true/false` to change",
                color=0x5865f2
            )
            await ctx.send(embed=embed)
        else:
            # Change status
            config['verification']['enabled'] = enabled
            bot.config_manager.save_guild_config(str(ctx.guild.id), config)
            
            status_text = "ENABLED" if enabled else "DISABLED"
            status_emoji = "🟢" if enabled else "🔴"
            color = 0x00ff88 if enabled else 0xff4444
            
            description = (
                f"🔐 **Captcha verification is now {status_text}**\n\n"
                f"{'New members will need to solve a math problem to gain access.' if enabled else 'New members will have immediate access.'}"
            )
            
            embed = discord.Embed(
                title=f"{status_emoji} Verification {status_text}",
                description=description,
                color=color
            )
            await ctx.send(embed=embed)
    
    @antispam.command(name='verify')
    async def manual_verify(ctx, member: discord.Member):
        """Manually send verification challenge to a member"""
        if member.bot:
            embed = discord.Embed(
                title="⚠️ Cannot Verify Bot",
                description="Bots cannot be verified through the captcha system.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return
        
        # Start verification for the member
        await bot._start_verification(member)
        
        embed = discord.Embed(
            title="📬 Verification Sent",
            description=f"Captcha verification has been sent to **{member.display_name}**.\n\nThey have 5 minutes to complete it.",
            color=0x5865f2
        )
        await ctx.send(embed=embed)
    
    @antispam.command(name='stats')
    async def show_stats(ctx):
        """Show detection statistics"""
        embed = discord.Embed(
            title="📈 Server Protection Analytics",
            description=f"📊 **Real-time statistics for {ctx.guild.name}**",
            color=0x5865f2,
            timestamp=datetime.utcnow()
        )
        embed.set_thumbnail(url=ctx.guild.icon.url if ctx.guild.icon else None)
        
        embed.add_field(name="🛡️ Protection Status", value="🟢 **ACTIVE & MONITORING**", inline=True)
        embed.add_field(name="🏛️ Server", value=f"**{ctx.guild.name}**", inline=True)
        embed.add_field(name="👥 Total Members", value=f"**{ctx.guild.member_count:,}**", inline=True)
        embed.add_field(name="🤖 Bots Detected", value="**0** *(last 24h)*", inline=True)
        embed.add_field(name="🚫 Spam Blocked", value="**0** *(last 24h)*", inline=True)
        embed.add_field(name="⚡ Raids Stopped", value="**0** *(last 24h)*", inline=True)
        embed.set_footer(text=f"AntiBot Protection • Requested by {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url)
        
        await ctx.send(embed=embed)
    
    # Basic moderation commands
    @bot.command(name='kick')
    @commands.has_permissions(kick_members=True)
    async def kick_command(ctx, member: discord.Member, *, reason="No reason provided"):
        """Kick a member"""
        success = await bot.moderation.kick_member(member, reason)
        if success:
            embed = discord.Embed(
                title="👢 Member Kicked",
                description=f"**{member.display_name}** has been removed from the server",
                color=0xff9500
            )
            embed.add_field(name="📝 Reason", value=reason, inline=False)
            embed.set_footer(text=f"Action by {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url)
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Kick Failed",
                description="Unable to kick this member. Check permissions.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
    
    @bot.command(name='ban')
    @commands.has_permissions(ban_members=True)
    async def ban_command(ctx, member: discord.Member, *, reason="No reason provided"):
        """Ban a member"""
        success = await bot.moderation.ban_member(member, reason)
        if success:
            embed = discord.Embed(
                title="🔨 Member Banned",
                description=f"**{member.display_name}** has been permanently banned",
                color=0xff0000
            )
            embed.add_field(name="📝 Reason", value=reason, inline=False)
            embed.set_footer(text=f"Action by {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url)
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Ban Failed",
                description="Unable to ban this member. Check permissions.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
    
    @bot.command(name='timeout')
    @commands.has_permissions(moderate_members=True)
    async def timeout_command(ctx, member: discord.Member, duration: int = 300, *, reason="No reason provided"):
        """Timeout a member (duration in seconds)"""
        success = await bot.moderation.timeout_member(member, duration, reason)
        if success:
            embed = discord.Embed(
                title="⏰ Member Timed Out",
                description=f"**{member.display_name}** cannot send messages temporarily",
                color=0xffa500
            )
            embed.add_field(name="⏱️ Duration", value=f"{duration} seconds", inline=True)
            embed.add_field(name="📝 Reason", value=reason, inline=False)
            embed.set_footer(text=f"Action by {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url)
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Timeout Failed",
                description="Unable to timeout this member. Check permissions.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
    
    @bot.command(name='quarantine')
    @commands.has_permissions(manage_roles=True)
    async def quarantine_command(ctx, member: discord.Member):
        """Quarantine a suspicious member"""
        success = await bot.moderation.quarantine_member(member)
        if success:
            embed = discord.Embed(
                title="🔒 Member Quarantined",
                description=f"**{member.display_name}** has been moved to quarantine",
                color=0x9932cc
            )
            embed.add_field(name="🔍 Status", value="Under review for suspicious activity", inline=False)
            embed.set_footer(text=f"Action by {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url)
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Quarantine Failed",
                description="Unable to quarantine this member. Check permissions.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
    
    # Error handling
    @bot.event
    async def on_command_error(ctx, error):
        """Handle command errors"""
        if isinstance(error, commands.MissingPermissions):
            embed = discord.Embed(
                title="🚫 Access Denied",
                description="You don't have permission to use this command.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
        elif isinstance(error, commands.BotMissingPermissions):
            embed = discord.Embed(
                title="⚠️ Missing Permissions",
                description="I don't have the required permissions to execute this command.",
                color=0xffa500
            )
            await ctx.send(embed=embed)
        elif isinstance(error, commands.CommandNotFound):
            return  # Ignore command not found errors
        else:
            logger.error(f"Command error: {error}")
            embed = discord.Embed(
                title="💥 Command Error",
                description="An unexpected error occurred while executing the command.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
    
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
