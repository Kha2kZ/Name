"""
Anti-Bot Detection System
Implements heuristics and detection algorithms for identifying malicious bots
"""

import asyncio
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Optional
import discord
from discord.ext import commands, tasks
import logging

logger = logging.getLogger(__name__)

class AntiBotCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
        # Rate limiting tracking
        self.user_message_times = defaultdict(lambda: deque(maxlen=50))
        self.guild_join_times = defaultdict(lambda: deque(maxlen=100))
        self.user_join_times = defaultdict(lambda: deque(maxlen=20))
        
        # Suspicious activity tracking
        self.suspicious_users = defaultdict(int)
        self.verified_users = set()
        
        # Start cleanup task
        self.cleanup_task.start()
    
    async def cog_unload(self):
        """Clean up when cog is unloaded"""
        self.cleanup_task.cancel()
    
    @tasks.loop(minutes=10)
    async def cleanup_task(self):
        """Periodic cleanup of old tracking data"""
        current_time = time.time()
        cutoff_time = current_time - 3600  # 1 hour ago
        
        # Clean up old message times
        for user_id in list(self.user_message_times.keys()):
            times = self.user_message_times[user_id]
            while times and times[0] < cutoff_time:
                times.popleft()
            if not times:
                del self.user_message_times[user_id]
        
        # Clean up old join times
        for guild_id in list(self.guild_join_times.keys()):
            times = self.guild_join_times[guild_id]
            while times and times[0] < cutoff_time:
                times.popleft()
        
        # Decay suspicious scores
        for user_id in list(self.suspicious_users.keys()):
            self.suspicious_users[user_id] = max(0, self.suspicious_users[user_id] - 1)
            if self.suspicious_users[user_id] == 0:
                del self.suspicious_users[user_id]
    
    @cleanup_task.before_loop
    async def before_cleanup(self):
        await self.bot.wait_until_ready()
    
    async def detect_bot_patterns(self, member):
        """
        Analyze a member for bot-like patterns
        Returns suspicion score (0-100)
        """
        score = 0
        reasons = []
        
        # Check account age (new accounts are suspicious)
        account_age = (datetime.utcnow() - member.created_at).days
        if account_age < 1:
            score += 40
            reasons.append("Very new account (< 1 day)")
        elif account_age < 7:
            score += 20
            reasons.append("New account (< 7 days)")
        
        # Check username patterns
        username = member.display_name.lower()
        
        # Generic bot-like names
        bot_indicators = [
            'discord', 'nitro', 'free', 'gift', 'official', 'support',
            'moderator', 'admin', 'staff', 'team', 'bot', 'auto'
        ]
        
        if any(indicator in username for indicator in bot_indicators):
            score += 25
            reasons.append("Suspicious username pattern")
        
        # Check for random character patterns
        if len([c for c in username if c.isdigit()]) > len(username) * 0.5:
            score += 15
            reasons.append("Username has many numbers")
        
        # Check avatar (default avatars are suspicious for bots)
        if member.avatar is None:
            score += 10
            reasons.append("No custom avatar")
        
        # Check join pattern (rapid joins)
        guild_id = member.guild.id
        join_times = self.guild_join_times[guild_id]
        current_time = time.time()
        join_times.append(current_time)
        
        # Count joins in last 5 minutes
        recent_joins = sum(1 for t in join_times if current_time - t < 300)
        if recent_joins > 10:
            score += 30
            reasons.append("Mass join event detected")
        elif recent_joins > 5:
            score += 15
            reasons.append("Rapid join pattern")
        
        return min(score, 100), reasons
    
    async def check_message_spam(self, message):
        """
        Check if a message appears to be spam
        Returns True if spam detected
        """
        user_id = message.author.id
        current_time = time.time()
        
        # Track message times
        message_times = self.user_message_times[user_id]
        message_times.append(current_time)
        
        # Get config for this guild
        config = self.bot.config_manager.get_guild_config(message.guild.id)
        
        # Check message rate
        recent_messages = sum(1 for t in message_times if current_time - t < 60)  # Last minute
        if recent_messages > config['spam_detection']['max_messages_per_minute']:
            return True
        
        # Check for duplicate messages
        if hasattr(message.author, '_last_message'):
            if message.content == message.author._last_message and len(message.content) > 10:
                return True
        message.author._last_message = message.content
        
        # Check for excessive mentions
        if len(message.mentions) > config['spam_detection']['max_mentions']:
            return True
        
        # Check for spam keywords
        spam_keywords = config['spam_detection']['spam_keywords']
        content_lower = message.content.lower()
        if any(keyword in content_lower for keyword in spam_keywords):
            return True
        
        return False
    
    @commands.Cog.listener()
    async def on_member_join(self, member):
        """Handle new member joins"""
        guild_config = self.bot.config_manager.get_guild_config(member.guild.id)
        
        if not guild_config['antibot']['enabled']:
            return
        
        # Analyze for bot patterns
        suspicion_score, reasons = await self.detect_bot_patterns(member)
        
        # Log the join
        await self.bot.bot_logger.log_event(
            member.guild.id,
            'member_join',
            f'{member.mention} joined (Suspicion: {suspicion_score}%) - {", ".join(reasons) if reasons else "No issues detected"}'
        )
        
        # Take action based on suspicion score
        action_taken = False
        
        if suspicion_score >= guild_config['antibot']['auto_ban_threshold']:
            try:
                await member.ban(reason=f"Automatic ban - Bot detection score: {suspicion_score}%")
                action_taken = True
                await self.bot.bot_logger.log_action(
                    member.guild.id,
                    'auto_ban',
                    f'Auto-banned {member.mention} (Score: {suspicion_score}%)',
                    moderator=self.bot.user
                )
            except discord.Forbidden:
                logger.warning(f"Cannot ban {member} - insufficient permissions")
        
        elif suspicion_score >= guild_config['antibot']['auto_kick_threshold']:
            try:
                await member.kick(reason=f"Automatic kick - Bot detection score: {suspicion_score}%")
                action_taken = True
                await self.bot.bot_logger.log_action(
                    member.guild.id,
                    'auto_kick',
                    f'Auto-kicked {member.mention} (Score: {suspicion_score}%)',
                    moderator=self.bot.user
                )
            except discord.Forbidden:
                logger.warning(f"Cannot kick {member} - insufficient permissions")
        
        # If verification is enabled and no action was taken
        if not action_taken and guild_config['verification']['enabled']:
            await self.apply_verification(member)
    
    @commands.Cog.listener()
    async def on_message(self, message):
        """Handle message events for spam detection"""
        if message.author.bot:
            return
        
        if not message.guild:
            return
        
        guild_config = self.bot.config_manager.get_guild_config(message.guild.id)
        
        if not guild_config['spam_detection']['enabled']:
            return
        
        # Check for spam
        is_spam = await self.check_message_spam(message)
        
        if is_spam:
            try:
                await message.delete()
                await self.bot.bot_logger.log_action(
                    message.guild.id,
                    'spam_delete',
                    f'Deleted spam message from {message.author.mention}',
                    moderator=self.bot.user
                )
                
                # Increase suspicion score
                self.suspicious_users[message.author.id] += 10
                
                # Take action if suspicion is too high
                if self.suspicious_users[message.author.id] >= 50:
                    await self.handle_suspicious_user(message.author)
                    
            except discord.Forbidden:
                logger.warning(f"Cannot delete message - insufficient permissions")
    
    async def apply_verification(self, member):
        """Apply verification role/restrictions to new member"""
        guild_config = self.bot.config_manager.get_guild_config(member.guild.id)
        
        if not guild_config['verification']['enabled']:
            return
        
        # Get or create verification role
        verification_role = discord.utils.get(member.guild.roles, name=guild_config['verification']['role_name'])
        
        if not verification_role:
            try:
                verification_role = await member.guild.create_role(
                    name=guild_config['verification']['role_name'],
                    permissions=discord.Permissions(read_messages=True, send_messages=False),
                    reason="Anti-bot verification role"
                )
            except discord.Forbidden:
                logger.warning("Cannot create verification role - insufficient permissions")
                return
        
        try:
            await member.add_roles(verification_role, reason="New member verification")
            await self.bot.bot_logger.log_event(
                member.guild.id,
                'verification_applied',
                f'Applied verification to {member.mention}'
            )
        except discord.Forbidden:
            logger.warning(f"Cannot add verification role to {member} - insufficient permissions")
    
    async def handle_suspicious_user(self, user):
        """Handle a user who has accumulated too much suspicion"""
        guild = user.guild
        guild_config = self.bot.config_manager.get_guild_config(guild.id)
        
        try:
            if guild_config['antibot']['auto_timeout']:
                # Timeout for 10 minutes
                timeout_until = discord.utils.utcnow() + timedelta(minutes=10)
                await user.timeout(timeout_until, reason="Suspicious activity detected")
                
                await self.bot.bot_logger.log_action(
                    guild.id,
                    'auto_timeout',
                    f'Auto-timed out {user.mention} for suspicious activity',
                    moderator=self.bot.user
                )
        except discord.Forbidden:
            logger.warning(f"Cannot timeout {user} - insufficient permissions")
    
    @commands.command(name='verify')
    async def manual_verify(self, ctx, member: Optional[discord.Member] = None):
        """Manually verify a member (removes verification restrictions)"""
        if not ctx.author.guild_permissions.manage_roles:
            await ctx.send("❌ You don't have permission to verify members.")
            return
        
        if member is None:
            member = ctx.author
        
        guild_config = self.bot.config_manager.get_guild_config(ctx.guild.id)
        verification_role = discord.utils.get(ctx.guild.roles, name=guild_config['verification']['role_name'])
        
        if verification_role and verification_role in member.roles:
            try:
                await member.remove_roles(verification_role, reason=f"Manually verified by {ctx.author}")
                self.verified_users.add(member.id)
                
                await ctx.send(f"✅ {member.mention} has been verified!")
                await self.bot.bot_logger.log_action(
                    ctx.guild.id,
                    'manual_verification',
                    f'{member.mention} manually verified by {ctx.author.mention}',
                    moderator=ctx.author
                )
            except discord.Forbidden:
                await ctx.send("❌ I don't have permission to remove roles.")
        else:
            await ctx.send("❌ User is not pending verification.")
    
    @commands.command(name='suspicion')
    @commands.has_permissions(manage_guild=True)
    async def check_suspicion(self, ctx, member: Optional[discord.Member] = None):
        """Check suspicion score for a member"""
        if member is None:
            member = ctx.author
        
        score, reasons = await self.detect_bot_patterns(member)
        current_suspicion = self.suspicious_users.get(member.id, 0)
        
        embed = discord.Embed(
            title=f"Suspicion Analysis for {member.display_name}",
            color=discord.Color.orange() if score > 50 else discord.Color.green()
        )
        
        embed.add_field(name="Bot Detection Score", value=f"{score}%", inline=True)
        embed.add_field(name="Current Suspicion", value=f"{current_suspicion}", inline=True)
        embed.add_field(name="Account Age", value=f"{(datetime.utcnow() - member.created_at).days} days", inline=True)
        
        if reasons:
            embed.add_field(name="Detection Reasons", value="\n".join(f"• {reason}" for reason in reasons), inline=False)
        
        await ctx.send(embed=embed)
