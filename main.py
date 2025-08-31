import discord
from discord.ext import commands
import asyncio
import json
import os
import logging
import random
import string
from datetime import datetime, timedelta
from typing import Optional

from config import ConfigManager
from bot_detection import BotDetector
from spam_detection import SpamDetector
from moderation import ModerationTools
from logging_setup import setup_logging
from monitor import BotMonitor

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
        self.monitor = BotMonitor(self)
        
        # Track member joins for raid detection
        self.recent_joins = {}
        
        # Track pending verifications
        self.pending_verifications = {}
        
        # Game system tracking
        self.active_games = {}
        self.leaderboard = {}
        
    async def setup_hook(self):
        """Called when the bot is starting up"""
        logger.info("Bot is starting up...")
        # Start monitoring
        self.monitor.start_monitoring()
        
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
        
        # Record member join event
        self.monitor.record_member_event('join', guild_id, str(member.id))
        
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
            
        # Check for trivia game answers
        await self._check_trivia_answer(message)
        
        # Check for spam
        is_spam = await self.spam_detector.check_message(message)
        
        if is_spam:
            await self._handle_spam_message(message)
            return
            
        await self.process_commands(message)
        
    async def on_member_remove(self, member):
        """Handle member leaving the server"""
        guild_id = str(member.guild.id)
        self.monitor.record_member_event('leave', guild_id, str(member.id))
        logger.info(f"Member left {member.guild.name}: {member} ({member.id})")
    
    async def _check_trivia_answer(self, message):
        """Check if message is a QNA game answer"""
        guild_id = str(message.guild.id)
        
        if guild_id not in self.active_games:
            return
        
        game = self.active_games[guild_id]
        current_question = game['current_question']
        user_id = str(message.author.id)
        
        # Check if answer is correct
        user_answer = message.content.lower().strip()
        correct_answer = current_question['answer'].lower()
        
        # Check if answer matches (flexible matching)
        is_correct = False
        if correct_answer == user_answer or correct_answer in user_answer or user_answer in correct_answer:
            is_correct = True
        
        if is_correct:
            # Mark question as answered
            game['question_answered'] = True
            
            # Award points
            if user_id not in game['players']:
                game['players'][user_id] = 0
            game['players'][user_id] += 10
            
            embed = discord.Embed(
                title="🎯 Correct Answer!",
                description=f"**{message.author.display_name}** got it right!\n\n+10 points awarded!",
                color=0x00ff88
            )
            embed.add_field(
                name="✅ Answer",
                value=f"**{current_question['answer'].title()}**",
                inline=True
            )
            embed.add_field(
                name="🏆 Your Score",
                value=f"**{game['players'][user_id]} points**",
                inline=True
            )
            
            await message.channel.send(embed=embed)
        
    async def _end_game_from_message(self, message, guild_id):
        """End game from message context"""
        game = self.active_games[guild_id]
        players = game['players']
        
        if not players:
            embed = discord.Embed(
                title="🎮 Game Ended",
                description="Game finished with no players!",
                color=0xff4444
            )
            await message.channel.send(embed=embed)
        else:
            # Update leaderboard
            if guild_id not in self.leaderboard:
                self.leaderboard[guild_id] = {}
            
            for user_id, score in players.items():
                if user_id not in self.leaderboard[guild_id]:
                    self.leaderboard[guild_id][user_id] = 0
                self.leaderboard[guild_id][user_id] += score
            
            # Show final results
            sorted_players = sorted(players.items(), key=lambda x: x[1], reverse=True)
            
            embed = discord.Embed(
                title="🎮 Game Finished!",
                description="🏁 **Final Results**",
                color=0x00ff88
            )
            
            for i, (user_id, score) in enumerate(sorted_players[:5]):
                try:
                    user = await self.fetch_user(int(user_id))
                    rank_emoji = ["🥇", "🥈", "🥉"][i] if i < 3 else f"{i+1}."
                    embed.add_field(
                        name=f"{rank_emoji} {user.display_name}",
                        value=f"🎯 {score} points",
                        inline=True
                    )
                except:
                    continue
            
            embed.set_footer(text="Great game everyone! Use ?leaderboard to see all-time scores")
            await message.channel.send(embed=embed)
        
        # Clean up game data
        del self.active_games[guild_id]
    
    async def _qna_question_loop(self, guild_id):
        """Continuously show new questions every 5 seconds with 30s timeout"""
        import random
        
        while guild_id in self.active_games and self.active_games[guild_id]['running']:
            try:
                # Wait for either answer or timeout
                game = self.active_games[guild_id]
                timeout_occurred = False
                
                # Check for 30-second timeout on current question
                if not game['question_answered']:
                    time_elapsed = (datetime.utcnow() - game['question_start_time']).total_seconds()
                    if time_elapsed >= 30:
                        timeout_occurred = True
                        
                        # Show correct answer due to timeout
                        embed = discord.Embed(
                            title="⏰ Time's Up!",
                            description="Nobody got it right in 30 seconds!",
                            color=0xffa500
                        )
                        embed.add_field(
                            name="✅ Correct Answer",
                            value=f"**{game['current_question']['answer'].title()}**",
                            inline=False
                        )
                        embed.set_footer(text="Better luck with the next question!")
                        
                        await game['channel'].send(embed=embed)
                
                # Wait 5 seconds or until question is answered
                if not game['question_answered'] and not timeout_occurred:
                    await asyncio.sleep(5)
                    continue
                
                # Reset for new question
                await asyncio.sleep(3)  # Brief pause after answer/timeout
                
                if guild_id not in self.active_games or not self.active_games[guild_id]['running']:
                    break
                    
                # Select next question (prioritize new questions, avoid repeats)
                available_questions = []
                
                # First, try new generated questions
                if game['new_questions']:
                    available_questions = game['new_questions']
                    current_question = random.choice(available_questions)
                    game['new_questions'].remove(current_question)
                else:
                    # Use original questions, but avoid already shown ones
                    available_questions = [q for q in game['questions'] if q not in game['shown_questions']]
                    
                    if not available_questions:
                        # If all questions shown, reset and use all questions again
                        available_questions = game['questions']
                        game['shown_questions'] = []
                    
                    current_question = random.choice(available_questions)
                
                # Track that this question was shown
                game['shown_questions'].append(current_question)
                game['current_question'] = current_question
                game['question_number'] += 1
                game['last_question_time'] = datetime.utcnow()
                game['question_answered'] = False
                game['question_start_time'] = datetime.utcnow()
                
                embed = discord.Embed(
                    title="🤔 Next Question",
                    description=f"**Question #{game['question_number']}**",
                    color=0x5865f2
                )
                embed.add_field(
                    name="❓ Question",
                    value=f"**{current_question['question']}**",
                    inline=False
                )
                embed.set_footer(text="Answer directly in chat • Use ?stop to end • ?skip if stuck")
                
                await game['channel'].send(embed=embed)
                
            except Exception as e:
                logger.error(f"Error in QNA question loop: {e}")
                break
    
    async def _qna_generation_loop(self, guild_id):
        """Generate new questions every 30 seconds"""
        import random
        
        question_templates = [
            ("What is the capital of {country}?", "geography"),
            ("What is {num1} + {num2}?", "math"),
            ("In what year was {event}?", "history"),
            ("What does {acronym} stand for?", "technology"),
            ("Who wrote {book}?", "literature"),
            ("What is the largest {category}?", "general"),
            ("How many {unit} are in a {larger_unit}?", "conversion")
        ]
        
        geography_data = [
            ("Japan", "tokyo"), ("Germany", "berlin"), ("Italy", "rome"), 
            ("Spain", "madrid"), ("Canada", "ottawa"), ("Australia", "canberra"),
            ("Brazil", "brasilia"), ("India", "new delhi")
        ]
        
        events_data = [
            ("World Wide Web invented", "1991"), ("Google founded", "1998"),
            ("YouTube launched", "2005"), ("iPhone released", "2007"),
            ("Bitcoin created", "2009"), ("Instagram launched", "2010")
        ]
        
        while guild_id in self.active_games and self.active_games[guild_id]['running']:
            try:
                await asyncio.sleep(30)  # Generate new question every 30 seconds
                
                if guild_id not in self.active_games or not self.active_games[guild_id]['running']:
                    break
                
                game = self.active_games[guild_id]
                template = random.choice(question_templates)
                
                # Generate question based on template
                if template[1] == "geography":
                    country, capital = random.choice(geography_data)
                    question = template[0].format(country=country)
                    answer = capital
                elif template[1] == "math":
                    num1, num2 = random.randint(1, 50), random.randint(1, 50)
                    question = template[0].format(num1=num1, num2=num2)
                    answer = str(num1 + num2)
                elif template[1] == "history":
                    event, year = random.choice(events_data)
                    question = template[0].format(event=event)
                    answer = year
                else:
                    continue  # Skip other templates for now
                
                # Add to new questions pool
                new_question = {"question": question, "answer": answer.lower()}
                game['new_questions'].append(new_question)
                game['last_generation_time'] = datetime.utcnow()
                
                logger.info(f"Generated new QNA question: {question}")
                
            except Exception as e:
                logger.error(f"Error in QNA generation loop: {e}")
                break
        
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
            # Record raid detection
            self.monitor.record_detection('raid', guild_id, {'joins_count': len(self.recent_joins[guild_id])})
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
        
        # Record detection event
        self.monitor.record_detection('bot', guild_id, {'member_id': str(member.id), 'member_name': str(member)})
        
        if action == 'kick':
            await self.moderation.kick_member(member, "Suspicious bot-like behavior")
            self.monitor.record_action('kick', guild_id, str(member), "Suspicious bot-like behavior")
        elif action == 'ban':
            await self.moderation.ban_member(member, "Suspicious bot-like behavior")
            self.monitor.record_action('ban', guild_id, str(member), "Suspicious bot-like behavior")
        elif action == 'quarantine':
            await self.moderation.quarantine_member(member)
            self.monitor.record_action('quarantine', guild_id, str(member), "Suspicious bot-like behavior")
            
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
        
        # Record spam detection
        self.monitor.record_detection('spam', str(message.guild.id), {'user_id': str(message.author.id), 'content': message.content[:100]})
        
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
            self.monitor.record_action('timeout', str(message.guild.id), str(message.author), "Spamming")
        elif action == 'kick':
            await self.moderation.kick_member(message.author, "Spamming")
            self.monitor.record_action('kick', str(message.guild.id), str(message.author), "Spamming")
        elif action == 'ban':
            await self.moderation.ban_member(message.author, "Spamming")
            self.monitor.record_action('ban', str(message.guild.id), str(message.author), "Spamming")
            
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
                    
                    # Record successful verification
                    self.monitor.record_verification(str(member.guild.id), True, str(member.id))
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
                            # Record failed verification
                            self.monitor.record_verification(str(guild.id), False, str(member.id))
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
    async def set_log_channel(ctx, channel: Optional[discord.TextChannel] = None):
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
    async def toggle_verification(ctx, enabled: Optional[bool] = None):
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
        # Use monitor to generate stats embed
        embed = await bot.monitor.generate_stats_embed(str(ctx.guild.id))
        embed.set_footer(text=f"AntiBot Protection • Requested by {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url if ctx.author.display_avatar else None)
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
    
    # Utility Commands
    @bot.command(name='help')
    async def help_command(ctx):
        """Show all available commands"""
        embed = discord.Embed(
            title="🛡️ Master Security Bot",
            description="**Your complete Discord protection and entertainment system**\n\n*Keeping your server safe while having fun!*",
            color=0x7289da
        )
        embed.set_thumbnail(url="https://cdn.discordapp.com/emojis/1234567890.png")
        embed.set_author(name="Command Center", icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
        
        embed.add_field(
            name="🛡️ Security & Protection",
            value=(
                "```fix\n"
                "?antispam          → Main protection hub\n"
                "?antispam config   → View settings\n"
                "?antispam stats    → Server analytics\n"
                "?status            → System health\n"
                "```"
            ),
            inline=True
        )
        
        embed.add_field(
            name="🔨 Moderation Arsenal",
            value=(
                "```diff\n"
                "+ ?kick <user>      → Remove member\n"
                "+ ?ban <user>       → Permanent ban\n"
                "+ ?timeout <user>   → Temporary mute\n"
                "+ ?quarantine <user> → Isolate threat\n"
                "```"
            ),
            inline=True
        )
        
        embed.add_field(
            name="🎮 Entertainment Hub",
            value=(
                "```yaml\n"
                "?games:       Start trivia challenge\n"
                "?skip:        Skip question\n"
                "?stop:        End game session\n"
                "?leaderboard: View champions\n"
                "```"
            ),
            inline=True
        )
        
        embed.add_field(
            name="🔧 Utility Tools",
            value=(
                "```css\n"
                "?echo [message]  → Echo chamber\n"
                "?help           → This menu\n"
                "```"
            ),
            inline=True
        )
        
        embed.add_field(
            name="\u200b",
            value="**🌟 Pro Tips**\n> Use `?antispam` for detailed security settings\n> Try `?games` for interactive trivia fun!\n> Check `?status` for real-time bot health",
            inline=False
        )
        embed.set_footer(text=f"Serving {len(bot.guilds)} servers • Requested by {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url if ctx.author.display_avatar else None)
        await ctx.send(embed=embed)
    
    @bot.command(name='status')
    async def status_command(ctx):
        """Show bot status and system information"""
        embed = discord.Embed(
            title="📊 System Dashboard",
            description="**🛡️ Master Security Bot • Real-time Status**\n\n*Monitoring and protecting your community 24/7*",
            color=0x00d4aa,
            timestamp=datetime.utcnow()
        )
        embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user and bot.user.display_avatar else None)
        
        # Bot info
        embed.add_field(
            name="🤖 Bot Information",
            value=f"**Name:** {bot.user.name if bot.user else 'Unknown'}\n**ID:** {bot.user.id if bot.user else 'Unknown'}\n**Ping:** {round(bot.latency * 1000)}ms",
            inline=True
        )
        
        # Server stats
        total_members = sum(guild.member_count for guild in bot.guilds if guild.member_count)
        embed.add_field(
            name="🏛️ Server Stats",
            value=f"**Servers:** {len(bot.guilds)}\n**Total Members:** {total_members:,}\n**Active Games:** {len(bot.active_games)}",
            inline=True
        )
        
        # Protection status for this guild
        config = bot.config_manager.get_guild_config(str(ctx.guild.id))
        protection_status = "🟢 ACTIVE" if config['enabled'] else "🔴 DISABLED"
        embed.add_field(
            name="🛡️ Protection Status",
            value=f"**Status:** {protection_status}\n**Verification:** {'🟢 ON' if config['verification']['enabled'] else '🔴 OFF'}",
            inline=True
        )
        
        embed.set_footer(text="All systems operational", icon_url=bot.user.display_avatar.url if bot.user and bot.user.display_avatar else None)
        await ctx.send(embed=embed)
    
    @bot.command(name='echo')
    async def echo_command(ctx, *, message):
        """Repeat the user's message"""
        embed = discord.Embed(
            title="📢 Echo Chamber",
            description=f"**“{message}”**",
            color=0x9966cc
        )
        embed.set_author(name=f"{ctx.author.display_name} says...", icon_url=ctx.author.display_avatar.url if ctx.author.display_avatar else None)
        embed.set_footer(text="✨ Message echoed successfully")
        await ctx.send(embed=embed)
    
    # Game Commands
    @bot.command(name='qna')
    async def start_game(ctx):
        """Start a QNA game"""
        guild_id = str(ctx.guild.id)
        
        if guild_id in bot.active_games:
            embed = discord.Embed(
                title="🎮 QNA Already Active",
                description="A QNA game is already running in this server!\n\nUse `?stop` to end it.",
                color=0xffa500
            )
            await ctx.send(embed=embed)
            return
        
        # Start new QNA game
        questions = [
            {"question": "What is the capital of France?", "answer": "paris"},
            {"question": "What is 2 + 2?", "answer": "4"},
            {"question": "Which planet is closest to the Sun?", "answer": "mercury"},
            {"question": "What year was Discord founded?", "answer": "2015"},
            {"question": "What does 'HTTP' stand for?", "answer": "hypertext transfer protocol"}
        ]
        
        import random
        current_question = random.choice(questions)
        
        bot.active_games[guild_id] = {
            'questions': questions,
            'current_question': current_question,
            'question_number': 1,
            'players': {},
            'start_time': datetime.utcnow(),
            'running': True,
            'channel': ctx.channel,
            'last_question_time': datetime.utcnow(),
            'last_generation_time': datetime.utcnow(),
            'question_answered': False,
            'question_start_time': datetime.utcnow(),
            'shown_questions': [current_question],
            'new_questions': []
        }
        
        embed = discord.Embed(
            title="🤔 QNA Challenge Activated!",
            description="**🧠 Question & Answer Arena**\n\n*Test your knowledge with continuous questions!*\n\n✨ **Ready to begin your QNA session?**",
            color=0xff6b6b
        )
        embed.add_field(
            name="❓ Current Question",
            value=f"**{current_question['question']}**",
            inline=False
        )
        embed.add_field(
            name="🎯 Game Rules",
            value="**📝 Answer Format:** Type your answer directly\n**⚡ Speed Bonus:** First correct answer wins!\n**🏆 Rewards:** 10 points per correct answer\n**⏱️ Questions:** New question every 5 seconds",
            inline=False
        )
        embed.set_footer(text="✨ Use ?stop to end QNA session • ?skip if stuck • Answer continuously!", icon_url=ctx.author.display_avatar.url if ctx.author.display_avatar else None)
        
        await ctx.send(embed=embed)
        
        # Start continuous question loop
        asyncio.create_task(bot._qna_question_loop(guild_id))
        asyncio.create_task(bot._qna_generation_loop(guild_id))
    
    @bot.command(name='stop')
    async def stop_game(ctx):
        """Stop the current QNA game"""
        guild_id = str(ctx.guild.id)
        
        if guild_id not in bot.active_games:
            embed = discord.Embed(
                title="❌ No Active QNA",
                description="No QNA game is currently running.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return
        
        # Stop the continuous loops
        bot.active_games[guild_id]['running'] = False
        await _end_game(ctx, guild_id)
    
    @bot.command(name='skip')
    async def skip_question(ctx):
        """Skip the current QNA question"""
        guild_id = str(ctx.guild.id)
        
        if guild_id not in bot.active_games:
            embed = discord.Embed(
                title="❌ No Active QNA",
                description="No QNA game is currently running.\n\nUse `?qna` to start a new session!",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return
        
        game = bot.active_games[guild_id]
        
        # Show correct answer before skipping
        embed = discord.Embed(
            title="⏭️ Question Skipped",
            description="Moving to the next question!",
            color=0xffa500
        )
        embed.add_field(
            name="✅ Correct Answer",
            value=f"**{game['current_question']['answer'].title()}**",
            inline=False
        )
        embed.set_footer(text="Next question coming up...")
        
        await ctx.send(embed=embed)
        
        # Mark as answered to trigger next question
        game['question_answered'] = True
    
    @bot.command(name='leaderboard')
    async def show_leaderboard(ctx):
        """Show QNA game leaderboard"""
        guild_id = str(ctx.guild.id)
        
        if guild_id not in bot.leaderboard or not bot.leaderboard[guild_id]:
            embed = discord.Embed(
                title="📈 QNA Leaderboard",
                description="No scores recorded yet!\n\nPlay some QNA games with `?qna` to get on the leaderboard!",
                color=0x5865f2
            )
            await ctx.send(embed=embed)
            return
        
        # Sort players by score
        sorted_players = sorted(bot.leaderboard[guild_id].items(), key=lambda x: x[1], reverse=True)
        
        embed = discord.Embed(
            title="🏆 QNA Leaderboard",
            description="🧠 **Top QNA players in this server**",
            color=0xffd700
        )
        
        for i, (user_id, score) in enumerate(sorted_players[:10]):
            try:
                user = await bot.fetch_user(int(user_id))
                rank_emoji = ["🥇", "🥈", "🥉"][i] if i < 3 else f"{i+1}."
                embed.add_field(
                    name=f"{rank_emoji} {user.display_name}",
                    value=f"🎯 **{score} points**",
                    inline=True
                )
            except:
                continue
        
        embed.set_footer(text="Play ?qna to climb the leaderboard!")
        await ctx.send(embed=embed)
    
    async def _end_game(ctx, guild_id):
        """End the QNA game and show results"""
        game = bot.active_games[guild_id]
        players = game['players']
        
        # Stop the continuous loops
        game['running'] = False
        
        if not players:
            embed = discord.Embed(
                title="🎮 QNA Ended",
                description="QNA session finished with no players!",
                color=0xff4444
            )
            await ctx.send(embed=embed)
        else:
            # Update leaderboard
            if guild_id not in bot.leaderboard:
                bot.leaderboard[guild_id] = {}
            
            for user_id, score in players.items():
                if user_id not in bot.leaderboard[guild_id]:
                    bot.leaderboard[guild_id][user_id] = 0
                bot.leaderboard[guild_id][user_id] += score
            
            # Show final results
            sorted_players = sorted(players.items(), key=lambda x: x[1], reverse=True)
            
            embed = discord.Embed(
                title="🎮 QNA Session Finished!",
                description="🏁 **Final Results**",
                color=0x00ff88
            )
            
            for i, (user_id, score) in enumerate(sorted_players[:5]):
                try:
                    user = await bot.fetch_user(int(user_id))
                    rank_emoji = ["🥇", "🥈", "🥉"][i] if i < 3 else f"{i+1}."
                    embed.add_field(
                        name=f"{rank_emoji} {user.display_name}",
                        value=f"🎯 {score} points",
                        inline=True
                    )
                except:
                    continue
            
            embed.set_footer(text="Great session everyone! Use ?leaderboard to see all-time scores")
            await ctx.send(embed=embed)
        
        # Clean up game data
        del bot.active_games[guild_id]
    
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
