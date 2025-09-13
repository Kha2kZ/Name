"""
Basic Moderation Commands
Provides standard moderation functionality (kick, ban, mute, etc.)
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional
import discord
from discord.ext import commands
import logging

logger = logging.getLogger(__name__)

class ModerationCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.muted_users = {}  # Track muted users and their unmute times
    
    async def get_mute_role(self, guild):
        """Get or create the mute role for a guild"""
        mute_role = discord.utils.get(guild.roles, name="Muted")
        
        if not mute_role:
            try:
                mute_role = await guild.create_role(
                    name="Muted",
                    permissions=discord.Permissions(send_messages=False, speak=False),
                    reason="Mute role for moderation"
                )
                
                # Update channel permissions
                for channel in guild.channels:
                    try:
                        if isinstance(channel, discord.TextChannel):
                            await channel.set_permissions(
                                mute_role,
                                send_messages=False,
                                add_reactions=False
                            )
                        elif isinstance(channel, discord.VoiceChannel):
                            await channel.set_permissions(
                                mute_role,
                                speak=False,
                                connect=True
                            )
                    except discord.Forbidden:
                        continue
                        
            except discord.Forbidden:
                return None
        
        return mute_role
    
    @commands.command(name='kick')
    @commands.has_permissions(kick_members=True)
    @commands.bot_has_permissions(kick_members=True)
    async def kick_member(self, ctx, member: discord.Member, *, reason="No reason provided"):
        """Kick a member from the server"""
        if member.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send("❌ You cannot kick someone with equal or higher role.")
            return
        
        if member.top_role >= ctx.guild.me.top_role:
            await ctx.send("❌ I cannot kick someone with equal or higher role than me.")
            return
        
        try:
            await member.kick(reason=f"Kicked by {ctx.author}: {reason}")
            
            embed = discord.Embed(
                title="Member Kicked",
                description=f"**{member.display_name}** has been kicked from the server.",
                color=discord.Color.orange(),
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Reason", value=f"**{reason}**", inline=False)
            embed.add_field(name="Moderator", value=f"{ctx.author.mention} (`{ctx.author}`)", inline=True)
            
            await ctx.send(embed=embed)
            
            # Log the action
            await self.bot.bot_logger.log_action(
                ctx.guild.id,
                'kick',
                f'{member.mention} kicked by {ctx.author.mention} - Reason: {reason}',
                moderator=ctx.author
            )
            
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to kick this member.")
        except Exception as e:
            await ctx.send(f"❌ An error occurred: {e}")
    
    @commands.command(name='ban')
    @commands.has_permissions(ban_members=True)
    @commands.bot_has_permissions(ban_members=True)
    async def ban_member(self, ctx, member: discord.Member, *, reason="No reason provided"):
        """Ban a member from the server"""
        if member.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send("❌ You cannot ban someone with equal or higher role.")
            return
        
        if member.top_role >= ctx.guild.me.top_role:
            await ctx.send("❌ I cannot ban someone with equal or higher role than me.")
            return
        
        try:
            await member.ban(reason=f"Banned by {ctx.author}: {reason}")
            
            embed = discord.Embed(
                title="Member Banned",
                description=f"**{member.display_name}** has been banned from the server.",
                color=discord.Color.red(),
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Reason", value=f"**{reason}**", inline=False)
            embed.add_field(name="Moderator", value=f"{ctx.author.mention} (`{ctx.author}`)", inline=True)
            
            await ctx.send(embed=embed)
            
            # Log the action
            await self.bot.bot_logger.log_action(
                ctx.guild.id,
                'ban',
                f'{member.mention} banned by {ctx.author.mention} - Reason: {reason}',
                moderator=ctx.author
            )
            
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to ban this member.")
        except Exception as e:
            await ctx.send(f"❌ An error occurred: {e}")
    
    @commands.command(name='unban')
    @commands.has_permissions(ban_members=True)
    @commands.bot_has_permissions(ban_members=True)
    async def unban_member(self, ctx, user_id: int, *, reason="No reason provided"):
        """Unban a user by their ID"""
        try:
            user = await self.bot.fetch_user(user_id)
            await ctx.guild.unban(user, reason=f"Unbanned by {ctx.author}: {reason}")
            
            embed = discord.Embed(
                title="Member Unbanned",
                description=f"**{user.name}** has been unbanned.",
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Reason", value=f"**{reason}**", inline=False)
            embed.add_field(name="Moderator", value=f"{ctx.author.mention} (`{ctx.author}`)", inline=True)
            
            await ctx.send(embed=embed)
            
            # Log the action
            await self.bot.bot_logger.log_action(
                ctx.guild.id,
                'unban',
                f'{user.mention} unbanned by {ctx.author.mention} - Reason: {reason}',
                moderator=ctx.author
            )
            
        except discord.NotFound:
            await ctx.send("❌ User not found or not banned.")
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to unban members.")
        except Exception as e:
            await ctx.send(f"❌ An error occurred: {e}")
    
    @commands.command(name='mute')
    @commands.has_permissions(manage_roles=True)
    @commands.bot_has_permissions(manage_roles=True)
    async def mute_member(self, ctx, member: discord.Member, duration: Optional[int] = None, *, reason="No reason provided"):
        """Mute a member (duration in minutes, leave empty for permanent)"""
        if member.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send("❌ You cannot mute someone with equal or higher role.")
            return
        
        mute_role = await self.get_mute_role(ctx.guild)
        if not mute_role:
            await ctx.send("❌ I couldn't create or find the mute role.")
            return
        
        if mute_role in member.roles:
            await ctx.send("❌ This member is already muted.")
            return
        
        try:
            await member.add_roles(mute_role, reason=f"Muted by {ctx.author}: {reason}")
            
            embed = discord.Embed(
                title="Member Muted",
                description=f"**{member.display_name}** has been muted.",
                color=discord.Color.dark_grey(),
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Reason", value=f"**{reason}**", inline=False)
            embed.add_field(name="Moderator", value=f"{ctx.author.mention} (`{ctx.author}`)", inline=True)
            
            if duration:
                unmute_time = datetime.utcnow() + timedelta(minutes=duration)
                self.muted_users[member.id] = unmute_time
                embed.add_field(name="Duration", value=f"{duration} minutes", inline=True)
                
                # Schedule unmute
                asyncio.create_task(self.auto_unmute(member, duration * 60))
            else:
                embed.add_field(name="Duration", value="Permanent", inline=True)
            
            await ctx.send(embed=embed)
            
            # Log the action
            duration_str = f" for {duration} minutes" if duration else " permanently"
            await self.bot.bot_logger.log_action(
                ctx.guild.id,
                'mute',
                f'{member.mention} muted by {ctx.author.mention}{duration_str} - Reason: {reason}',
                moderator=ctx.author
            )
            
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to manage roles.")
        except Exception as e:
            await ctx.send(f"❌ An error occurred: {e}")
    
    @commands.command(name='unmute')
    @commands.has_permissions(manage_roles=True)
    @commands.bot_has_permissions(manage_roles=True)
    async def unmute_member(self, ctx, member: discord.Member, *, reason="No reason provided"):
        """Unmute a member"""
        mute_role = discord.utils.get(ctx.guild.roles, name="Muted")
        
        if not mute_role or mute_role not in member.roles:
            await ctx.send("❌ This member is not muted.")
            return
        
        try:
            await member.remove_roles(mute_role, reason=f"Unmuted by {ctx.author}: {reason}")
            
            # Remove from tracking
            if member.id in self.muted_users:
                del self.muted_users[member.id]
            
            embed = discord.Embed(
                title="Member Unmuted",
                description=f"**{member.display_name}** has been unmuted.",
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Reason", value=f"**{reason}**", inline=False)
            embed.add_field(name="Moderator", value=f"{ctx.author.mention} (`{ctx.author}`)", inline=True)
            
            await ctx.send(embed=embed)
            
            # Log the action
            await self.bot.bot_logger.log_action(
                ctx.guild.id,
                'unmute',
                f'{member.mention} unmuted by {ctx.author.mention} - Reason: {reason}',
                moderator=ctx.author
            )
            
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to manage roles.")
        except Exception as e:
            await ctx.send(f"❌ An error occurred: {e}")
    
    async def auto_unmute(self, member, delay):
        """Automatically unmute a member after a delay"""
        await asyncio.sleep(delay)
        
        # Check if member is still in guild and still muted
        if member.guild and member in member.guild.members:
            mute_role = discord.utils.get(member.guild.roles, name="Muted")
            if mute_role and mute_role in member.roles:
                try:
                    await member.remove_roles(mute_role, reason="Automatic unmute - duration expired")
                    
                    # Log the action
                    await self.bot.bot_logger.log_action(
                        member.guild.id,
                        'auto_unmute',
                        f'{member.mention} automatically unmuted - duration expired',
                        moderator=self.bot.user
                    )
                except discord.Forbidden:
                    pass
        
        # Remove from tracking
        if member.id in self.muted_users:
            del self.muted_users[member.id]
    
    @commands.command(name='timeout')
    @commands.has_permissions(moderate_members=True)
    @commands.bot_has_permissions(moderate_members=True)
    async def timeout_member(self, ctx, member: discord.Member, duration: int, *, reason="No reason provided"):
        """Timeout a member (duration in minutes, max 2419200 minutes/28 days)"""
        if member.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send("❌ You cannot timeout someone with equal or higher role.")
            return
        
        if duration > 2419200:  # Discord's max timeout
            await ctx.send("❌ Maximum timeout duration is 2,419,200 minutes (28 days).")
            return
        
        try:
            timeout_until = discord.utils.utcnow() + timedelta(minutes=duration)
            await member.timeout(timeout_until, reason=f"Timed out by {ctx.author}: {reason}")
            
            embed = discord.Embed(
                title="Member Timed Out",
                description=f"**{member.display_name}** has been timed out.",
                color=discord.Color.dark_red(),
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Duration", value=f"**{duration} minutes** (expires <t:{int((datetime.utcnow() + timedelta(minutes=duration)).timestamp())}:R>)", inline=True)
            embed.add_field(name="Reason", value=f"**{reason}**", inline=False)
            embed.add_field(name="Moderator", value=f"{ctx.author.mention} (`{ctx.author}`)", inline=True)
            
            await ctx.send(embed=embed)
            
            # Log the action
            await self.bot.bot_logger.log_action(
                ctx.guild.id,
                'timeout',
                f'{member.mention} timed out by {ctx.author.mention} for {duration} minutes - Reason: {reason}',
                moderator=ctx.author
            )
            
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to timeout members.")
        except Exception as e:
            await ctx.send(f"❌ An error occurred: {e}")
    
    @commands.command(name='untimeout')
    @commands.has_permissions(moderate_members=True)
    @commands.bot_has_permissions(moderate_members=True)
    async def untimeout_member(self, ctx, member: discord.Member, *, reason="No reason provided"):
        """Remove timeout from a member"""
        if member.timed_out_until is None:
            await ctx.send("❌ This member is not timed out.")
            return
        
        try:
            await member.timeout(None, reason=f"Timeout removed by {ctx.author}: {reason}")
            
            embed = discord.Embed(
                title="Timeout Removed",
                description=f"Timeout removed from **{member.display_name}**.",
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Reason", value=f"**{reason}**", inline=False)
            embed.add_field(name="Moderator", value=f"{ctx.author.mention} (`{ctx.author}`)", inline=True)
            
            await ctx.send(embed=embed)
            
            # Log the action
            await self.bot.bot_logger.log_action(
                ctx.guild.id,
                'untimeout',
                f'Timeout removed from {member.mention} by {ctx.author.mention} - Reason: {reason}',
                moderator=ctx.author
            )
            
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to manage timeouts.")
        except Exception as e:
            await ctx.send(f"❌ An error occurred: {e}")
    
    @commands.command(name='purge', aliases=['clear'])
    @commands.has_permissions(manage_messages=True)
    @commands.bot_has_permissions(manage_messages=True)
    async def purge_messages(self, ctx, amount: int, member: Optional[discord.Member] = None):
        """Delete messages (up to 100, optionally from specific member)"""
        if amount > 100:
            await ctx.send("❌ Cannot purge more than 100 messages at once.")
            return
        
        if amount < 1:
            await ctx.send("❌ Must purge at least 1 message.")
            return
        
        try:
            def check(message):
                if member:
                    return message.author == member
                return True
            
            deleted = await ctx.channel.purge(limit=amount + 1, check=check)  # +1 for command message
            deleted_count = len(deleted) - 1  # Subtract the command message
            
            embed = discord.Embed(
                title="Messages Purged",
                description=f"Deleted **{deleted_count} messages**.",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Channel", value=f"{ctx.channel.mention} (`#{ctx.channel.name}`)", inline=True)
            embed.add_field(name="Moderator", value=f"{ctx.author.mention} (`{ctx.author}`)", inline=True)
            if member:
                embed.add_field(name="Target User", value=f"{member.mention} (`{member}`)", inline=True)
            
            # Send confirmation and delete it after a few seconds
            confirm_msg = await ctx.send(embed=embed)
            await asyncio.sleep(5)
            await confirm_msg.delete()
            
            # Log the action
            target_info = f" from {member.mention}" if member else ""
            await self.bot.bot_logger.log_action(
                ctx.guild.id,
                'purge',
                f'{ctx.author.mention} purged {deleted_count} messages{target_info} in {ctx.channel.mention}',
                moderator=ctx.author
            )
            
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to delete messages.")
        except Exception as e:
            await ctx.send(f"❌ An error occurred: {e}")
