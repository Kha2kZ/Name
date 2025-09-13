import nest_asyncio
nest_asyncio.apply()
import time
import threading

# Thread để ping runtime
def keep_alive():
    while True:
        print("⏱️ Still alive")
        time.sleep(60)

t = threading.Thread(target=keep_alive)
t.start()

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
from openai import OpenAI
import psycopg2
from psycopg2.extras import RealDictCursor

from config import ConfigManager
from bot_detection import BotDetector
from spam_detection import SpamDetector
from moderation import ModerationTools
from logging_setup import setup_logging
from monitor import BotMonitor

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

def _parse_duration(duration_str):
    """Parse duration string like '30s', '5m', '2h', '1d' into seconds"""
    if not duration_str:
        return None

    # Remove spaces and convert to lowercase
    duration_str = duration_str.lower().strip()

    # Check if it's just a number (assume seconds)
    if duration_str.isdigit():
        return int(duration_str)

    # Parse format like "30s", "5m", "2h", "1d"
    import re
    match = re.match(r'^(\d+)([smhd])$', duration_str)
    if not match:
        return None

    value, unit = match.groups()
    value = int(value)

    if unit == 's':
        return value
    elif unit == 'm':
        return value * 60
    elif unit == 'h':
        return value * 3600
    elif unit == 'd':
        return value * 86400

    return None

def _format_duration(seconds):
    """Format seconds into human readable duration"""
    if seconds < 60:
        return f"{seconds} seconds"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minutes"
    elif seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hours"
    else:
        days = seconds // 86400
        return f"{days} days"

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

        # Initialize OpenAI for translation
        # the newest OpenAI model is "gpt-5" which was released August 7, 2025.
        # do not change this unless explicitly requested by the user
        self.openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

        # Database URL for creating connections as needed
        self.database_url = os.environ.get("DATABASE_URL")
        self._create_initial_tables()

        # Track member joins for raid detection
        self.recent_joins = {}

        # Track pending verifications
        self.pending_verifications = {}

        # Game system tracking
        self.active_games = {}
        self.leaderboard = {}

        # Over/Under game tracking
        self.overunder_games = {}
        
        # Auto-cycle tracking for continuous txshow
        self.overunder_autocycle = {}

        # In-memory cash storage when database isn't available
        self.user_cash_memory = {}

        # Per-user locks for preventing race conditions in daily rewards
        self._daily_locks = {}

        # File-based backup system
        self.backup_file_path = "user_cash_backup.json"
        self._load_backup_data()

        # Start background backup task
        self.backup_task = None

    def _load_backup_data(self):
        """Load user cash data from backup file on startup"""
        try:
            if os.path.exists(self.backup_file_path):
                with open(self.backup_file_path, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)
                    raw_user_data = backup_data.get('user_cash_memory', {})

                    # Convert date strings back to date objects
                    self.user_cash_memory = {}
                    for key, data in raw_user_data.items():
                        processed_data = data.copy()

                        # Convert last_daily string back to date object
                        if 'last_daily' in processed_data and processed_data['last_daily']:
                            try:
                                if isinstance(processed_data['last_daily'], str):
                                    processed_data['last_daily'] = datetime.strptime(processed_data['last_daily'], '%Y-%m-%d').date()
                            except (ValueError, TypeError):
                                processed_data['last_daily'] = None

                        self.user_cash_memory[key] = processed_data

                    logger.info(f"Loaded backup data for {len(self.user_cash_memory)} users from {self.backup_file_path}")
            else:
                logger.info("No backup file found, starting with empty memory")
                self.user_cash_memory = {}
        except Exception as e:
            logger.error(f"Error loading backup data: {e}")
            self.user_cash_memory = {}

    def _save_backup_data(self):
        """Save current user cash data to backup file"""
        try:
            # Don't save empty data - this prevents overwriting existing backups with empty data
            if not self.user_cash_memory:
                logger.debug("No user data to backup, skipping save")
                return

            # Additional safety check: don't save if all users have default/reset values
            # This catches cases where memory was corrupted but not empty
            suspicious_data = True
            for key, data in self.user_cash_memory.items():
                cash = data.get('cash', 1000)
                last_daily = data.get('last_daily')
                streak = data.get('daily_streak', 0)

                # If any user has non-default values, data looks legitimate
                if cash != 1000 or last_daily is not None or streak > 0:
                    suspicious_data = False
                    break

            if suspicious_data:
                logger.warning("All users have default values - skipping backup to prevent data loss")
                return

            # Load existing backup data first to merge with current data
            existing_data = {}
            if os.path.exists(self.backup_file_path):
                try:
                    with open(self.backup_file_path, 'r', encoding='utf-8') as f:
                        existing_backup = json.load(f)
                        existing_data = existing_backup.get('user_cash_memory', {})
                except Exception as e:
                    logger.warning(f"Could not load existing backup for merging: {e}")

            # Merge current memory with existing backup data
            # Existing data takes priority unless current data is clearly more recent/valuable
            merged_data = existing_data.copy()

            # Convert current memory data to serializable format and merge
            for key, data in self.user_cash_memory.items():
                processed_data = data.copy()

                # Convert date objects to ISO string format
                if 'last_daily' in processed_data and processed_data['last_daily']:
                    if hasattr(processed_data['last_daily'], 'isoformat'):
                        processed_data['last_daily'] = processed_data['last_daily'].isoformat()

                # Always update with current data to ensure daily claim persistence
                if key in existing_data:
                    existing_data_info = existing_data[key]
                    current_last_daily = processed_data.get('last_daily')
                    existing_last_daily = existing_data_info.get('last_daily')
                    
                    # Always prioritize more recent last_daily dates to prevent claim resets
                    should_update = False
                    
                    if current_last_daily and existing_last_daily:
                        # Compare dates - prioritize more recent claims
                        try:
                            if isinstance(current_last_daily, str):
                                current_date = datetime.strptime(current_last_daily, '%Y-%m-%d').date()
                            else:
                                current_date = current_last_daily if hasattr(current_last_daily, 'year') else datetime.utcnow().date()
                                
                            if isinstance(existing_last_daily, str):
                                existing_date = datetime.strptime(existing_last_daily, '%Y-%m-%d').date()
                            else:
                                existing_date = existing_last_daily if hasattr(existing_last_daily, 'year') else datetime.utcnow().date()
                            
                            # Update if current claim is same day or newer
                            should_update = current_date >= existing_date
                            
                        except (ValueError, TypeError, AttributeError):
                            # If date parsing fails, update anyway to be safe
                            should_update = True
                            logger.warning(f"Date parsing failed for user {key}, updating anyway")
                    elif current_last_daily and not existing_last_daily:
                        # Current has claim data, existing doesn't
                        should_update = True
                    else:
                        # Use cash as tiebreaker if no last_daily comparison possible
                        current_cash = processed_data.get('cash', 1000)
                        existing_cash = existing_data_info.get('cash', 1000)
                        should_update = current_cash >= existing_cash
                    
                    if should_update:
                        merged_data[key] = processed_data
                        logger.debug(f"Updated user {key}: ensuring daily claim persistence")
                    else:
                        # Keep existing data
                        merged_data[key] = existing_data_info
                        logger.debug(f"Kept existing data for user {key}")
                else:
                    # New user, add them
                    merged_data[key] = processed_data
                    logger.debug(f"Added new user {key} with {processed_data.get('cash', 1000)} cash")

            backup_data = {
                'user_cash_memory': merged_data,
                'last_backup': datetime.utcnow().isoformat()
            }

            # Create temporary file first, then rename for atomic write
            temp_file = f"{self.backup_file_path}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False)

            # Atomic rename to prevent corruption
            os.rename(temp_file, self.backup_file_path)
            logger.debug(f"Successfully saved backup data for {len(merged_data)} users (smart merge)")

        except Exception as e:
            logger.error(f"Error saving backup data: {e}")

    async def _backup_data_loop(self):
        """Background task that saves data every 5 seconds"""
        # Wait a bit on first run to ensure system is ready
        await asyncio.sleep(10)  # Initial delay to let system stabilize

        while True:
            try:
                await asyncio.sleep(5)  # Save every 5 seconds
                self._save_backup_data()
                logger.debug("Auto-saved user cash data to backup file")
            except Exception as e:
                logger.error(f"Error in backup loop: {e}")
                await asyncio.sleep(30)  # Wait longer if there's an error

    def _get_db_connection(self):
        """Get a fresh database connection for operations"""
        if not self.database_url:
            return None
        try:
            return psycopg2.connect(self.database_url)
        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            return None

    def _create_initial_tables(self):
        """Create necessary database tables if they don't exist"""
        if not self.database_url:
            logger.warning("DATABASE_URL not set, database features disabled")
            return

        connection = self._get_db_connection()
        if not connection:
            return

        try:
            with connection.cursor() as cursor:
                # Create user_cash table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_cash (
                        guild_id VARCHAR(50) NOT NULL,
                        user_id VARCHAR(50) NOT NULL,
                        cash BIGINT DEFAULT 0,
                        last_daily DATE,
                        daily_streak INTEGER DEFAULT 0,
                        PRIMARY KEY (guild_id, user_id)
                    )
                """)

                # Create shown_questions table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS shown_questions (
                        guild_id VARCHAR(50) NOT NULL,
                        question_text TEXT NOT NULL,
                        PRIMARY KEY (guild_id, question_text)
                    )
                """)

                # Create overunder_games table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS overunder_games (
                        game_id VARCHAR(50) PRIMARY KEY,
                        guild_id VARCHAR(50) NOT NULL,
                        channel_id VARCHAR(50) NOT NULL,
                        status VARCHAR(20) DEFAULT 'active',
                        result VARCHAR(10),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                connection.commit()
                logger.info("Database tables created/verified successfully")

        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
        finally:
            connection.close()

    def _get_shown_questions(self, guild_id):
        """Get all questions that have been shown to this guild"""
        connection = self._get_db_connection()
        if not connection:
            return set()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT question_text FROM shown_questions WHERE guild_id = %s",
                    (guild_id,)
                )
                results = cursor.fetchall()
                return {row[0] for row in results}
        except Exception as e:
            logger.error(f"Error getting shown questions: {e}")
            return set()
        finally:
            connection.close()

    def _mark_question_shown(self, guild_id, question_text):
        """Mark a question as shown for this guild"""
        connection = self._get_db_connection()
        if not connection:
            return

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO shown_questions (guild_id, question_text) VALUES (%s, %s) ON CONFLICT (guild_id, question_text) DO NOTHING",
                    (guild_id, question_text)
                )
                connection.commit()
        except Exception as e:
            logger.error(f"Error marking question as shown: {e}")
        finally:
            connection.close()

    def _batch_mark_questions_shown(self, guild_id, questions):
        """Mark multiple questions as shown for this guild (batch operation)"""
        if not questions:
            return

        connection = self._get_db_connection()
        if not connection:
            return

        try:
            with connection.cursor() as cursor:
                # Use executemany for batch insert
                values = [(guild_id, question) for question in questions]
                cursor.executemany(
                    "INSERT INTO shown_questions (guild_id, question_text) VALUES (%s, %s) ON CONFLICT (guild_id, question_text) DO NOTHING",
                    values
                )
                connection.commit()
                logger.info(f"Batch marked {len(questions)} questions as shown for guild {guild_id}")
        except Exception as e:
            logger.error(f"Error batch marking questions as shown: {e}")
            # Fallback to individual inserts
            for question in questions:
                self._mark_question_shown(guild_id, question)
        finally:
            connection.close()

    def _reset_question_history(self, guild_id):
        """Reset question history for a guild (admin command)"""
        connection = self._get_db_connection()
        if not connection:
            return

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM shown_questions WHERE guild_id = %s",
                    (guild_id,)
                )
                connection.commit()
                logger.info(f"Reset question history for guild {guild_id}")
        except Exception as e:
            logger.error(f"Error resetting question history: {e}")
        finally:
            connection.close()

    async def translate_to_vietnamese(self, text):
        """Translate English text to Vietnamese"""
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-5",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a professional translator. Translate the given English text to Vietnamese. Respond only with the Vietnamese translation, no additional text."
                    },
                    {
                        "role": "user",
                        "content": text
                    }
                ],
                max_tokens=200
            )
            if response.choices and response.choices[0].message and response.choices[0].message.content:
                return response.choices[0].message.content.strip()
            return text
        except Exception as e:
            logger.error(f"Translation error: {e}")
            return text  # Return original text if translation fails

    async def translate_to_english(self, vietnamese_text):
        """Translate Vietnamese text to English for answer checking"""
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-5",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a professional translator. Translate the given Vietnamese text to English. Respond only with the English translation, no additional text."
                    },
                    {
                        "role": "user",
                        "content": vietnamese_text
                    }
                ],
                max_tokens=200
            )
            if response.choices and response.choices[0].message and response.choices[0].message.content:
                return response.choices[0].message.content.strip().lower()
            return vietnamese_text.lower()
        except Exception as e:
            logger.error(f"Translation error: {e}")
            return vietnamese_text.lower()  # Return original text if translation fails

    # === CASH SYSTEM HELPER METHODS ===
    def _get_user_cash(self, guild_id, user_id):
        """Get user's cash amount and daily streak info"""
        connection = self._get_db_connection()
        if not connection:
            # Use in-memory storage when database isn't available
            key = f"{guild_id}_{user_id}"
            if key in self.user_cash_memory:
                data = self.user_cash_memory[key]
                return data.get('cash', 1000), data.get('last_daily'), data.get('daily_streak', 0)
            else:
                # Give new users some starting cash
                return 1000, None, 0

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT cash, last_daily, daily_streak FROM user_cash WHERE guild_id = %s AND user_id = %s",
                    (str(guild_id), str(user_id))
                )
                result = cursor.fetchone()
                if result:
                    return result[0], result[1], result[2]
                else:
                    # Create new user with starting cash instead of returning 0
                    cursor.execute(
                        "INSERT INTO user_cash (guild_id, user_id, cash) VALUES (%s, %s, %s)",
                        (str(guild_id), str(user_id), 1000)
                    )
                    connection.commit()
                    return 1000, None, 0
        except Exception as e:
            logger.error(f"Error getting user cash: {e}")
            return 0, None, 0
        finally:
            connection.close()

    def _update_user_cash(self, guild_id, user_id, cash_amount, last_daily=None, daily_streak=None):
        """Update user's cash amount and daily streak"""
        connection = self._get_db_connection()
        if not connection:
            # Use in-memory storage when database isn't available
            key = f"{guild_id}_{user_id}"
            if key not in self.user_cash_memory:
                self.user_cash_memory[key] = {'cash': 1000, 'last_daily': None, 'daily_streak': 0}

            if last_daily is not None and daily_streak is not None:
                # Set absolute values (for daily rewards)
                self.user_cash_memory[key].update({
                    'cash': cash_amount,
                    'last_daily': last_daily,
                    'daily_streak': daily_streak
                })
            else:
                # Add to existing cash (for bets/winnings)
                self.user_cash_memory[key]['cash'] += cash_amount

            # Save backup immediately when cash is updated
            self._save_backup_data()
            return True

        try:
            with connection.cursor() as cursor:
                if last_daily is not None and daily_streak is not None:
                    cursor.execute(
                        """INSERT INTO user_cash (guild_id, user_id, cash, last_daily, daily_streak)         VALUES (%s, %s, %s, %s, %s) 
                           ON CONFLICT (guild_id, user_id) 
                           DO UPDATE SET cash = %s, last_daily = %s, daily_streak = %s""",
                        (str(guild_id), str(user_id), cash_amount, last_daily, daily_streak,
                         cash_amount, last_daily, daily_streak)
                    )
                else:
                    cursor.execute(
                        """INSERT INTO user_cash (guild_id, user_id, cash) 
                           VALUES (%s, %s, %s) 
                           ON CONFLICT (guild_id, user_id) 
                           DO UPDATE SET cash = user_cash.cash + %s""",
                        (str(guild_id), str(user_id), cash_amount, cash_amount)
                    )
                connection.commit()
                return True
        except Exception as e:
            logger.error(f"Error updating user cash: {e}")
            return False
        finally:
            connection.close()

    def _calculate_daily_reward(self, streak):
        """Calculate daily reward based on streak (streak=1 is first day)"""
        base_reward = 1000
        if streak <= 1:
            return base_reward  # First day = 1000 cash
        elif streak == 2:
            return 1200  # Second consecutive day
        elif streak == 3:
            return 1500  # Third consecutive day
        else:
            # Continue increasing by 400 per day after day 3
            return 1500 + (400 * (streak - 3))

    async def _claim_daily_reward(self, guild_id, user_id, today):
        """Atomically claim daily reward - prevents double claiming"""
        # Normalize today to date object to prevent type mismatch issues
        if isinstance(today, datetime):
            today = today.date()
        elif isinstance(today, str):
            try:
                today = datetime.strptime(today, '%Y-%m-%d').date()
            except (ValueError, TypeError):
                today = datetime.utcnow().date()
        
        connection = self._get_db_connection()
        if not connection:
            # Use in-memory storage with proper locking when database isn't available
            key = f"{guild_id}_{user_id}"
            
            # Get or create per-user lock to prevent race conditions
            if key not in self._daily_locks:
                self._daily_locks[key] = asyncio.Lock()
            
            async with self._daily_locks[key]:
                # Initialize user data if not exists
                if key not in self.user_cash_memory:
                    self.user_cash_memory[key] = {'cash': 1000, 'last_daily': None, 'daily_streak': 0}
                
                current_data = self.user_cash_memory[key]
                last_daily = current_data.get('last_daily')
                
                # Normalize last_daily to date object for proper comparison
                if isinstance(last_daily, datetime):
                    last_daily = last_daily.date()
                elif isinstance(last_daily, str):
                    try:
                        last_daily = datetime.strptime(last_daily, '%Y-%m-%d').date()
                    except (ValueError, TypeError):
                        last_daily = None
                
                # Check if already claimed today
                if last_daily == today:
                    logger.info(f"User {key} already claimed daily reward today ({today})")
                    return None  # Already claimed
                
                # Re-read current cash to avoid lost updates from concurrent operations
                current_cash = self.user_cash_memory[key].get('cash', 1000)
                current_streak = current_data.get('daily_streak', 0)
                yesterday = today - timedelta(days=1)
                
                if last_daily == yesterday:
                    new_streak = current_streak + 1
                elif last_daily is None:
                    new_streak = 1  # First daily claim = 1 day streak
                else:
                    new_streak = 1  # Reset streak to 1 when claiming after missing days
                
                reward = self._calculate_daily_reward(new_streak)
                new_cash = current_cash + reward
                
                # Update in-memory data atomically
                self.user_cash_memory[key].update({
                    'cash': new_cash,
                    'last_daily': today,
                    'daily_streak': new_streak
                })
                
                logger.info(f"User {key} claimed daily reward: {reward} cash, streak: {new_streak}, total: {new_cash}")
                self._save_backup_data()
                return (reward, new_cash, new_streak, current_streak)

        try:
            with connection.cursor() as cursor:
                # First, get current user data with row locking to prevent race conditions
                cursor.execute(
                    "SELECT cash, last_daily, daily_streak FROM user_cash WHERE guild_id = %s AND user_id = %s FOR UPDATE",
                    (str(guild_id), str(user_id))
                )
                result = cursor.fetchone()
                
                if not result:
                    # Create new user
                    cursor.execute(
                        "INSERT INTO user_cash (guild_id, user_id, cash, last_daily, daily_streak) VALUES (%s, %s, %s, %s, %s)",
                        (str(guild_id), str(user_id), 1000, None, 0)
                    )
                    current_cash, last_daily, current_streak = 1000, None, 0
                else:
                    current_cash, last_daily, current_streak = result
                
                # Normalize last_daily from database
                if isinstance(last_daily, datetime):
                    last_daily = last_daily.date()
                elif isinstance(last_daily, str):
                    try:
                        last_daily = datetime.strptime(last_daily, '%Y-%m-%d').date()
                    except (ValueError, TypeError):
                        last_daily = None
                
                # Log the comparison for debugging
                logger.debug(f"Daily claim DB check: user {guild_id}_{user_id}, today={today} (type={type(today)}), last_daily={last_daily} (type={type(last_daily)})")
                
                # Check if already claimed today
                if last_daily == today:
                    connection.rollback()
                    logger.info(f"User {guild_id}_{user_id} already claimed daily reward today ({today}) - DB path")
                    return None  # Already claimed
                
                # Calculate streak and reward
                yesterday = today - timedelta(days=1)
                if last_daily == yesterday:
                    new_streak = current_streak + 1
                elif last_daily is None:
                    new_streak = 1  # First daily claim = 1 day streak
                else:
                    new_streak = 1  # Reset streak to 1 when claiming after missing days
                
                reward = self._calculate_daily_reward(new_streak)
                new_cash = current_cash + reward
                
                # Update user data atomically
                cursor.execute(
                    "UPDATE user_cash SET cash = %s, last_daily = %s, daily_streak = %s WHERE guild_id = %s AND user_id = %s",
                    (new_cash, today, new_streak, str(guild_id), str(user_id))
                )
                
                connection.commit()
                return (reward, new_cash, new_streak, current_streak)
                
        except Exception as e:
            logger.error(f"Error claiming daily reward: {e}")
            if connection:
                connection.rollback()
            return False  # Database error
        finally:
            if connection:
                connection.close()

    async def _end_overunder_game(self, guild_id, game_id, instant_stop=False):
        """End the Over/Under game and distribute winnings"""
        if not instant_stop:
            await asyncio.sleep(30)  # Wait for game duration

        if guild_id not in self.overunder_games or game_id not in self.overunder_games[guild_id]:
            return

        game_data = self.overunder_games[guild_id][game_id]
        if game_data['status'] != 'active':
            return

        # Cancel the end task if it exists (for instant stops)
        if instant_stop and 'end_task' in game_data and game_data['end_task']:
            game_data['end_task'].cancel()

        game_data['status'] = 'ended'

        # Get the channel
        channel = self.get_channel(int(game_data['channel_id']))
        if not channel or not hasattr(channel, 'send'):
            return

        # Generate random result (50/50 chance)
        result = random.choice(['tai', 'xiu'])
        game_data['result'] = result

        # Update database
        try:
            connection = self._get_db_connection()
            if connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "UPDATE overunder_games SET result = %s, status = 'ended' WHERE game_id = %s",
                        (result, game_id)
                    )
                    connection.commit()
                connection.close()
        except Exception as e:
            logger.error(f"Error updating game result: {e}")

        # Process winnings
        winners = []
        losers = []

        for bet in game_data['bets']:
            if bet['side'] == result:
                # Winner - give back double the bet
                winnings = bet['amount'] * 2
                self._update_user_cash(guild_id, bet['user_id'], winnings, None, None)
                winners.append({
                    'username': bet['username'],
                    'amount': bet['amount'],
                    'winnings': winnings
                })
            else:
                # Loser - they already lost their bet when placing it
                losers.append({
                    'username': bet['username'],
                    'amount': bet['amount']
                })

        # Create result embed
        embed = discord.Embed(
            title="🎲 Kết Quả Game Over/Under!",
            description=f"**{result.upper()} THẮNG!** 🎉",
            color=0x00ff88 if winners else 0xff4444
        )

        if winners:
            winners_text = "\n".join([f"🏆 **{w['username']}** - Cược {w['amount']:,} → Nhận **{w['winnings']:,} cash**" for w in winners])
            embed.add_field(
                name=f"✅ Người thắng ({len(winners)})",
                value=winners_text,
                inline=False
            )

        if losers:
            losers_text = "\n".join([f"💸 **{l['username']}** - Mất {l['amount']:,} cash" for l in losers])
            embed.add_field(
                name=f"❌ Người thua ({len(losers)})",
                value=losers_text,
                inline=False
            )

        if not game_data['bets']:
            embed.add_field(
                name="🤷‍♂️ Không có ai tham gia",
                value="Không có cược nào được đặt trong game này.",
                inline=False
            )

        embed.add_field(
            name="🎮 Game mới",
            value="Dùng `?tx` để bắt đầu game Over/Under mới!",
            inline=False
        )

        embed.set_footer(text=f"Game ID: {game_id} • Cảm ơn bạn đã tham gia! 🎉")

        await channel.send(embed=embed)

        # Check for auto-cycle and start new game if enabled
        channel_key = f"{guild_id}_{game_data['channel_id']}"
        if channel_key in self.overunder_autocycle:
            # Wait a moment then auto-start new game
            await asyncio.sleep(2)
            
            # Create new auto-game
            new_game_id = f"{guild_id}_{game_data['channel_id']}_{int(datetime.utcnow().timestamp())}"
            end_time = datetime.utcnow() + timedelta(seconds=30)

            if guild_id not in self.overunder_games:
                self.overunder_games[guild_id] = {}

            self.overunder_games[guild_id][new_game_id] = {
                'channel_id': game_data['channel_id'],
                'end_time': end_time,
                'bets': [],
                'status': 'active',
                'result': None,
                'end_task': None
            }

            # Store new game in database
            try:
                connection = self._get_db_connection()
                if connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "INSERT INTO overunder_games (game_id, guild_id, channel_id) VALUES (%s, %s, %s)",
                            (new_game_id, guild_id, game_data['channel_id'])
                        )
                        connection.commit()
                    connection.close()
            except Exception as e:
                logger.error(f"Error storing auto-cycle game: {e}")

            # Start the background task to end the new game (this creates the continuous cycle)
            task = asyncio.create_task(self._end_overunder_game(guild_id, new_game_id))
            self.overunder_games[guild_id][new_game_id]['end_task'] = task

            # Send auto-start announcement
            auto_embed = discord.Embed(
                title="🔄 Game Tự Động Tiếp Theo!",
                description="**Game Tài Xỉu mới đã tự động bắt đầu!**\n\nCh\u1ebf \u0111\u1ed9 t\u1ef1 \u0111\u1ed9ng \u0111ang b\u1eadt - game s\u1ebd ti\u1ebfp t\u1ee5c sau m\u1ed7i v\u00f2ng!",
                color=0x00ff88
            )
            auto_embed.add_field(
                name="⏰ Thời gian",
                value="**30 giây** để đặt cược",
                inline=True
            )
            auto_embed.add_field(
                name="💰 Cách chơi",
                value="Dùng lệnh `?cuoc <tai/xiu> <số tiền>`",
                inline=True
            )
            auto_embed.add_field(
                name="🛑 Dừng tự động",
                value="Dùng `?gamestop` để dừng hoàn toàn",
                inline=True
            )
            auto_embed.set_footer(text="Chế độ tự động: Game sẽ tiếp tục sau mỗi vòng!")
            await channel.send(embed=auto_embed)

        # Clean up game data
        del self.overunder_games[guild_id][game_id]
        if not self.overunder_games[guild_id]:  # Remove guild if no games left
            del self.overunder_games[guild_id]

    async def setup_hook(self):
        """Called when the bot is starting up"""
        logger.info("Bot is starting up...")
        # Start monitoring
        self.monitor.start_monitoring()

    async def on_ready(self):
        """Called when the bot is ready"""
        logger.info(f'{self.user} has connected to Discord!')
        logger.info(f'Bot is in {len(self.guilds)} guilds')

        # Start the backup task if not already running, but only if we have data to protect
        if self.backup_task is None or self.backup_task.done():
            # Add a delay before starting the backup loop to ensure system is fully ready
            await asyncio.sleep(2)  # Wait 2 seconds before starting backup loop
            self.backup_task = asyncio.create_task(self._backup_data_loop())
            logger.info("Started backup data loop - saving user data every 5 seconds")

        # Set bot status
        await self.change_presence(
            status=discord.Status.online,
            activity=discord.Activity(
                type=discord.ActivityType.playing,
                name="with your feelings 💔"
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

        # Get user's answer 
        user_answer = message.content.strip().lower()
        correct_answer = current_question['answer'].lower()
        vietnamese_answer = current_question.get('vietnamese_answer', '').lower()

        # Fast local matching first (no API calls needed)
        is_correct = False

        # Direct Vietnamese and English answer matching
        if (correct_answer == user_answer or 
            correct_answer in user_answer or 
            user_answer in correct_answer or
            vietnamese_answer == user_answer or
            vietnamese_answer in user_answer or
            user_answer in vietnamese_answer):
            is_correct = True

        # Common Vietnamese answer variants (instant matching)
        vietnamese_variants = {
            'fansipan': ['phan xi păng', 'phan si pan', 'fanxipan', 'fan si pan'],
            'mekong': ['cửu long', 'mê kông', 'mekong', 'sông mê kông', 'song mekong'],
            'ho chi minh': ['bác hồ', 'chú hồ', 'hồ chí minh', 'hcm', 'ho chi minh'],
            'hanoi': ['hà nội', 'ha noi', 'thủ đô', 'thu do'],
            'pho': ['phở', 'pho', 'phở bò', 'pho bo'],
            'ao dai': ['áo dài', 'ao dai', 'ao dai viet nam'],
            'lotus': ['sen', 'hoa sen', 'lotus', 'quoc hoa'],
            'dong': ['đồng', 'vnd', 'việt nam đồng', 'dong viet nam'],
            '1975': ['1975', 'một nghìn chín trăm bảy mười lăm', 'nam 75'],
            '1954': ['1954', 'một nghìn chín trăm năm mười tư', 'nam 54'],
            '1995': ['1995', 'một nghìn chín trăm chín mười lăm', 'nam 95'],
            'phu quoc': ['phú quốc', 'phu quoc', 'dao phu quoc'],
            'an giang': ['an giang', 'an giang province', 'vua lua'],
            'ha long bay': ['vịnh hạ long', 'ha long bay', 'vinh ha long'],
            'saigon': ['sài gòn', 'saigon', 'sai gon'],
            '58': ['58', 'năm mười tám', 'nam muoi tam'],
            '17 triệu': ['17 triệu', '17000000', 'mười bảy triệu', 'muoi bay trieu']
        }

        # Check Vietnamese variants instantly
        for eng_answer, viet_variants in vietnamese_variants.items():
            if eng_answer == correct_answer:
                for variant in viet_variants:
                    if variant in user_answer or user_answer in variant:
                        is_correct = True
                        break

        # Additional number and common word matching for speed
        if not is_correct:
            # Numbers matching (Vietnamese style)
            if correct_answer.isdigit():
                if correct_answer in user_answer or user_answer == correct_answer:
                    is_correct = True

            # Remove diacritics for fuzzy matching
            import unicodedata
            def remove_diacritics(text):
                return unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode('ascii')

            user_no_diacritics = remove_diacritics(user_answer)
            answer_no_diacritics = remove_diacritics(vietnamese_answer)

            if (answer_no_diacritics and 
                (answer_no_diacritics in user_no_diacritics or 
                 user_no_diacritics in answer_no_diacritics)):
                is_correct = True

        # Skip slow translation API entirely to maintain speed
        # The local matching above should handle 99% of cases instantly

        if is_correct:
            # Mark question as answered
            game['question_answered'] = True

            # Award points
            if user_id not in game['players']:
                game['players'][user_id] = 0
            game['players'][user_id] += 10

            embed = discord.Embed(
                title="🎯 Đáp án chính xác!",
                description=f"**{message.author.display_name}** đã trả lời đúng!\n\n+10 điểm được trao!",
                color=0x00ff88
            )
            embed.add_field(
                name="✅ Đáp án",
                value=f"**{current_question.get('vietnamese_answer', current_question['answer'])}**",
                inline=True
            )
            embed.add_field(
                name="🏆 Điểm của bạn",
                value=f"**{game['players'][user_id]} điểm**",
                inline=True
            )

            await message.channel.send(embed=embed)

    async def _end_game_from_message(self, message, guild_id):
        """End game from message context"""
        game = self.active_games[guild_id]
        players = game['players']

        if not players:
            embed = discord.Embed(
                title="🎮 Trò chơi kết thúc",
                description="Trò chơi kết thúc không có người chơi!",
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
                title="🎮 Trò chơi hoàn thành!",
                description="🏁 **Kết quả cuối cùng**",
                color=0x00ff88
            )

            for i, (user_id, score) in enumerate(sorted_players[:5]):
                try:
                    user = await self.fetch_user(int(user_id))
                    rank_emoji = ["🥇", "🥈", "🥉"][i] if i < 3 else f"{i+1}."
                    embed.add_field(
                        name=f"{rank_emoji} {user.display_name}",
                        value=f"🎯 {score} điểm",
                        inline=True
                    )
                except:
                    continue

            embed.set_footer(text="Trò chơi tuyệt vời! Dùng ?leaderboard để xem điểm tổng")
            await message.channel.send(embed=embed)

        # Clean up game data
        del self.active_games[guild_id]

    async def _qna_question_loop(self, guild_id):
        """Continuously show new questions every 5 seconds with 30s timeout"""
        import random

        while guild_id in self.active_games and self.active_games[guild_id]['running']:
            try:
                game = self.active_games[guild_id]

                # Wait for either answer or timeout
                start_wait = datetime.utcnow()
                while (datetime.utcnow() - start_wait).total_seconds() < 30:
                    if game['question_answered'] or not game['running']:
                        break
                    await asyncio.sleep(1)  # Check every second

                # If timeout occurred (30 seconds passed without answer)
                if not game['question_answered'] and game['running']:
                    embed = discord.Embed(
                        title="⏰ Hết giờ!",
                        description="Không ai trả lời đúng trong 30 giây!",
                        color=0xffa500
                    )
                    embed.add_field(
                        name="✅ Đáp án đúng",
                        value=f"**{game['current_question'].get('vietnamese_answer', game['current_question']['answer']).title()}**",
                        inline=False
                    )
                    embed.set_footer(text="Chúc may mắn lần sau!")
                    await game['channel'].send(embed=embed)

                # Brief pause before next question
                if game['running']:
                    await asyncio.sleep(3)

                if guild_id not in self.active_games or not self.active_games[guild_id]['running']:
                    break

                # Select next question (prioritize new questions, avoid repeats)
                current_question = None

                # First, try new generated questions
                if game['new_questions']:
                    current_question = game['new_questions'].pop(0)  # Take first new question
                    logger.info(f"Using new generated question: {current_question['question']}")
                else:
                    # Use original questions, but avoid already shown ones
                    available_questions = [q for q in game['questions'] if q['question'] not in game['shown_questions']]

                    if not available_questions:
                        # No available questions - wait for new generation without sending duplicate messages
                        logger.info("No available questions, waiting for new generation")

                        # Only show waiting message once per session
                        if not game.get('waiting_message_sent', False):
                            embed = discord.Embed(
                                title="🔄 Tạo câu hỏi mới",
                                description="**Đang tạo câu hỏi mới... Vui lòng chờ giây lát!**",
                                color=0xffa500
                            )
                            embed.add_field(
                                name="⏳ Trạng thái",
                                value="**Hệ thống đang tạo câu hỏi mới từ cơ sở dữ liệu**",
                                inline=False
                            )
                            embed.set_footer(text="Câu hỏi mới sẽ xuất hiện sớm!")

                            await game['channel'].send(embed=embed)
                            game['waiting_message_sent'] = True

                        await asyncio.sleep(2)  # Shorter wait, no continue to avoid loop restart
                        continue

                    # Select from available_questions that passed the filter
                    current_question = random.choice(available_questions)
                    logger.info(f"Using available original question: {current_question['question']}")

                # Track that this question was shown in memory and database (skip for placeholders)
                if not current_question.get('is_placeholder', False):
                    game['shown_questions'].add(current_question['question'])
                    self._mark_question_shown(guild_id, current_question['question'])
                    if current_question in game['questions']:
                        game['questions'].remove(current_question)

                game['current_question'] = current_question
                game['question_number'] += 1
                game['last_question_time'] = datetime.utcnow()
                game['question_answered'] = False
                game['question_start_time'] = datetime.utcnow()

                embed = discord.Embed(
                    title="🤔 Câu hỏi tiếp theo",
                    description=f"**Câu hỏi #{game['question_number']}**",
                    color=0x5865f2
                )
                embed.add_field(
                    name="❓ Câu hỏi",
                    value=f"**{current_question['question']}**",
                    inline=False
                )
                embed.set_footer(text="Trả lời trực tiếp trong chat • Dùng ?stop để kết thúc • ?skip nếu bí")

                await game['channel'].send(embed=embed)

            except Exception as e:
                logger.error(f"Error in QNA question loop: {e}")
                break

    async def _qna_generation_loop(self, guild_id):
        """Generate new Vietnam-focused questions every 2 seconds"""
        import random

        # Vietnam-focused question database (Vietnamese questions with English answers for matching)
        vietnam_questions = {
            "geography": [
                ("Núi cao nhất Việt Nam là gì?", "fansipan", "Fansipan"),
                ("Sông nào dài nhất ở Việt Nam?", "mekong", "Sông Mê Không"),
                ("Đảo lớn nhất của Việt Nam là đảo nào?", "phu quoc", "Phú Quốc"),
                ("Tỉnh nào được gọi là 'vựa lúa' của Việt Nam?", "an giang", "An Giang"),
                ("Vịnh nổi tiếng của Việt Nam với những cột đá vôi là gì?", "ha long bay", "Vịnh Hạ Long"),
                ("Thành phố nào là thủ đô cũ của Miền Nam Việt Nam?", "saigon", "Sài Gòn"),
                ("Tỉnh cực bắc của Việt Nam là tỉnh nào?", "ha giang", "Hà Giang"),
                ("Đồng bằng nào ở miền Nam Việt Nam?", "mekong delta", "Đồng bằng sông Cửu Long"),
                ("Hồ lớn nhất Việt Nam là hồ nào?", "ba be lake", "Hồ Ba Bể"),
                ("Dãy núi nào chạy dọc biên giới phía tây Việt Nam?", "truong son", "Trường Sơn")
            ],
            "history": [
                ("Việt Nam thống nhất vào năm nào?", "1975", "1975"),
                ("Tổng thống đầu tiên của Việt Nam là ai?", "ho chi minh", "Hồ Chí Minh"),
                ("Trận Điện Biên Phủ diễn ra vào năm nào?", "1954", "1954"),
                ("Việt Nam gia nhập ASEAN vào năm nào?", "1995", "1995"),
                ("Hà Nội được thành lập vào năm nào?", "1010", "1010"),
                ("Triều đại Lý bắt đầu vào năm nào?", "1009", "1009"),
                ("Việt Nam gia nhập WTO vào năm nào?", "2007", "2007"),
                ("Văn Miếu Hà Nội được xây dựng vào năm nào?", "1070", "1070"),
                ("Việt Nam bắt đầu Đổi Mới vào năm nào?", "1986", "1986"),
                ("Việt Nam thiết lập quan hệ ngoại giao với Mỹ vào năm nào?", "1995", "1995")
            ],
            "culture": [
                ("Trang phục truyền thống dài của Việt Nam gọi là gì?", "ao dai", "Áo dài"),
                ("Món canh nổi tiếng nhất của Việt Nam là gì?", "pho", "Phở"),
                ("Tết của người Việt gọi là gì?", "tet", "Tết"),
                ("Nhạc cụ truyền thống Việt Nam là gì?", "dan bau", "Đàn bầu"),
                ("Tác phẩm sử thi vĩ đại nhất của Việt Nam là gì?", "kieu", "Truyện Kiều"),
                ("Ai là tác giả của Truyện Kiều?", "nguyen du", "Nguyễn Du"),
                ("Nón truyền thống của Việt Nam gọi là gì?", "non la", "Nón lá"),
                ("Võ thuật truyền thống của Việt Nam là gì?", "vovinam", "Vovinam"),
                ("Gỏi cuốn Việt Nam gọi là gì?", "goi cuon", "Gỏi cuốn"),
                ("Phương pháp pha cà phê truyền thống của Việt Nam là gì?", "phin filter", "Phin")            ],
            "biology": [
                ("Con vật quốc gia của Việt Nam là gì?", "water buffalo", "Trâu nước"),
                ("Loài khỉ nào bị tuyệt chủng ở Việt Nam?", "langur", "Vườn"),
                ("Loài gấu nào sống ở Việt Nam?", "asian black bear", "Gấu ngựa Á châu"),
                ("Mèo lớn nào sống ở Việt Nam?", "leopard", "Báo hoa mai"),
                ("Loài rắn lớn nhất ở Việt Nam?", "reticulated python", "Trăn lưới"),
                ("Loài súng nào di cư đến Việt Nam?", "red crowned crane", "Súng đầu đỏ"),
                ("Loài rùa bị tuyệt chủng nào ở Hồ Hoàn Kiếm?", "yangtze giant softshell turtle", "Rùa Hồ Gươm"),
                ("Loài khỉ đặc hữu của Việt Nam là gì?", "tonkin snub nosed monkey", "Vườn mũi hếch"),
                ("Cá nước ngọt lớn nhất Việt Nam?", "mekong giant catfish", "Cá tra dau"),
                ("Chim quốc gia của Việt Nam?", "red crowned crane", "Súng đầu đỏ")
            ],
            "technology": [
                ("Công ty công nghệ lớn nhất Việt Nam?", "fpt", "FPT"),
                ("Ứng dụng xe ôm của Việt Nam là gì?", "grab", "Grab"),
                ("Tên miền internet của Việt Nam là gì?", ".vn", ".vn"),
                ("Công ty Việt Nam sản xuất điện thoại thông minh?", "vsmart", "VinSmart"),
                ("Hệ thống thanh toán quốc gia của Việt Nam?", "napas", "NAPAS"),
                ("Mạng xã hội Việt trước Facebook là gì?", "zing me", "Zing Me"),
                ("Nền tảng thương mại điện tử lớn nhất Việt Nam?", "shopee", "Shopee"),
                ("Công ty Việt cung cấp dịch vụ điện toán đám mây?", "viettel", "Viettel"),
                ("Công ty viễn thông chính của Việt Nam?", "vnpt", "VNPT"),
                ("Công ty khoi nghiệp Việt nổi tiếng về AI?", "fpt ai", "FPT AI")
            ],
            "math": [
                ("Nếu Hà Nội có 8 triệu dân và TP.HCM có 9 triệu dân, tổng là bao nhiêu?", "17 million", "17 triệu"),
                ("Việt Nam có 63 tỉnh thành. Nếu 5 là thành phố trực thuộc TW, còn lại bao nhiêu tỉnh?", "58", "58"),
                ("Nếu tô phở giá 50.000 VNĐ và mua 3 tô, tổng tiền là bao nhiêu?", "150000", "150.000"),
                ("Diện tích Việt Nam là 331.212 km². Làm tròn đến hàng nghìn.", "331000", "331.000"),
                ("Nếu Việt Nam có 98 triệu dân, một nửa là bao nhiêu?", "49 million", "49 triệu"),
                ("Vịnh Hạ Long có 1.600 hòn đảo. Nếu 400 hòn lớn, bao nhiêu hòn nhỏ?", "1200", "1.200"),
                ("Nếu bánh mì 25.000 VNĐ và cà phê 15.000 VNĐ, tổng cộng là bao nhiêu?", "40000", "40.000"),
                ("Việt Nam dài 1.650 km từ Bắc vào Nam. Một nửa là bao nhiêu km?", "825", "825"),
                ("Nếu Việt Nam có 54 dân tộc và Kiền là 1, còn lại bao nhiêu dân tộc thiểu số?", "53", "53"),
                ("Chiến tranh Việt Nam từ 1955 đến 1975. Bao nhiêu năm?", "20", "20")
            ],
            "chemistry": [
                ("Hóa chất nào làm nước mắm Việt Nam mặn?", "sodium chloride", "Natri clorua"),
                ("Nguyên tố nào phổ biến trong quặng sắt Việt Nam?", "iron", "Sắt"),
                ("Khí nào được tạo ra khi làm rượu cần Việt Nam?", "carbon dioxide", "Cacbon đioxit"),
                ("Nguyên tố nào ở mỏ boxit Việt Nam?", "aluminum", "Nhôm"),
                ("Hợp chất nào làm ớt Việt Nam cay?", "capsaicin", "Capsaicin"),
                ("Axit nào dùng để làm dưa chua Việt Nam?", "acetic acid", "Axit axetic"),
                ("Nguyên tố nào trong than đá Việt Nam?", "carbon", "Cacbon"),
                ("Hợp chất nào làm trà xanh Việt Nam đắng?", "tannin", "Tannin"),
                ("Công thức hóa học của muối ăn Việt Nam?", "nacl", "NaCl"),
                ("Nguyên tố nào được khai thác từ mỏ đất hiếm Việt Nam?", "cerium", "Cerium")
            ],
            "literature": [
                ("Nhà thơ nổi tiếng nhất Việt Nam là ai?", "nguyen du", "Nguyễn Du"),
                ("Tác phẩm văn học vĩ đại nhất Việt Nam là gì?", "kieu", "Truyện Kiều"),
                ("Ai viết 'Nỗi buồn chiến tranh'?", "bao ninh", "Bảo Ninh"),
                ("Nhà văn Việt Nam nào nổi tiếng quốc tế?", "nguyen huy thiep", "Nguyễn Huy Thiệp"),
                ("Tên bài thơ sử thi Việt Nam về người phụ nữ?", "kieu", "Truyện Kiều"),
                ("Ai viết 'Thiên đường mù'?", "duong thu huong", "Dương Thu Hương"),
                ("Nhà thơ Việt Nam viết về kháng chiến?", "to huu", "Tố Hữu"),
                ("Thời kỳ văn học cổ điển Việt Nam gọi là gì?", "medieval period", "Trung đại"),
                ("Ai được gọi là 'Shakespeare Việt Nam'?", "nguyen du", "Nguyễn Du"),
                ("Tác phẩm Việt Nam kể về cô con gái quan?", "kieu", "Truyện Kiều")
            ]
        }

        while guild_id in self.active_games and self.active_games[guild_id]['running']:
            try:
                await asyncio.sleep(2)  # Much faster generation - every 2 seconds

                if guild_id not in self.active_games or not self.active_games[guild_id]['running']:
                    break

                game = self.active_games[guild_id]

                # Generate multiple questions at once for better performance
                questions_to_generate = min(3, 10)  # Generate up to 3 at once

                # Efficiently filter available questions (avoid nested loops)
                available_new_questions = []
                for cat_name, cat_questions in vietnam_questions.items():
                    for q_data in cat_questions:
                        if q_data[0] not in game['shown_questions']:
                            available_new_questions.append((cat_name, q_data))

                # If we have new questions available and queue isn't full, generate several
                if available_new_questions and len(game['new_questions']) < 5:  # Keep queue small
                    questions_added = []

                    for _ in range(min(questions_to_generate, len(available_new_questions))):
                        if not available_new_questions:
                            break

                        category, question_data = random.choice(available_new_questions)
                        question, answer, vietnamese_answer = question_data

                        # Add to new questions pool and mark as shown
                        new_question = {"question": question, "answer": answer.lower(), "vietnamese_answer": vietnamese_answer}
                        game['new_questions'].append(new_question)
                        game['shown_questions'].add(question)
                        questions_added.append(question)

                        # Remove from available list to avoid duplicates in this batch
                        available_new_questions.remove((category, question_data))

                        logger.info(f"Generated new QNA question ({category}): {question}")

                    # Batch database operations for better performance
                    if questions_added:
                        self._batch_mark_questions_shown(guild_id, questions_added)

                    game['last_generation_time'] = datetime.utcnow()

                    # Reset waiting message flag when new questions are available
                    game['waiting_message_sent'] = False
                elif not available_new_questions:
                    # All questions used, but DON'T reset database - keep persistent history
                    logger.info("All questions used, waiting for manual reset")
                    await asyncio.sleep(5)  # Faster wait when no questions available

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
        config['logging']['channel_id'] = str(channel.id) if channel else None
        config['logging']['enabled'] = True
        bot.config_manager.save_guild_config(str(ctx.guild.id), config)

        embed = discord.Embed(
            title="📝 Logging Channel Updated",
            description=f"📍 **Channel:** {channel.mention if channel else 'None'}\n\n🔍 All moderation actions will be logged here",
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
    async def timeout_command(ctx, member: discord.Member, duration_str: str = "5m", *, reason="No reason provided"):
        """Timeout a member (duration: 30s, 5m, 2h, 1d)"""
        try:
            # Parse duration string (e.g., "30s", "5m", "2h", "1d")
            duration_seconds = _parse_duration(duration_str)
            if duration_seconds is None:
                embed = discord.Embed(
                    title="❌ Invalid Duration",
                    description="Please use format like: 30s, 5m, 2h, 1d\nExample: `?timeout @user 10m spam`",
                    color=0xff4444
                )
                await ctx.send(embed=embed)
                return

            # Discord max timeout is 28 days (2419200 seconds)
            if duration_seconds > 2419200:
                embed = discord.Embed(
                    title="❌ Duration Too Long",
                    description="Maximum timeout duration is 28 days.",
                    color=0xff4444
                )
                await ctx.send(embed=embed)
                return

            success = await bot.moderation.timeout_member(member, duration_seconds, reason)
            if success:
                embed = discord.Embed(
                    title="⏰ Member Timed Out",
                    description=f"**{member.display_name}** cannot send messages temporarily",
                    color=0xffa500
                )
                embed.add_field(name="⏱️ Duration", value=_format_duration(duration_seconds), inline=True)
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

        except Exception as e:
            logger.error(f"Error in timeout command: {e}")
            embed = discord.Embed(
                title="❌ Command Error",
                description="An error occurred while processing the timeout command.",
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
                "?antispam               → Main protection hub\n"
                "?antispam config        → View current settings\n"
                "?antispam enable/disable → Toggle protection\n"
                "?antispam logchannel    → Set logging channel\n"
                "?antispam whitelist     → Trust a user\n"
                "?antispam verification  → Toggle verification\n"
                "?antispam verify        → Send verification\n"
                "?antispam stats         → Server analytics\n"
                "?status                 → System health\n"
                "```"
            ),
            inline=False
        )

        embed.add_field(
            name="🔨 Moderation Arsenal",
            value=(
                "```diff\n"
                "+ ?kick <user> [reason]      → Remove member\n"
                "+ ?ban <user> [reason]       → Permanent ban\n"
                "+ ?timeout <user> [duration] → Temporary mute\n"
                "+ ?quarantine <user>         → Isolate threat\n"
                "```"
            ),
            inline=False
        )

        embed.add_field(
            name="🎮 Q&A Game System",
            value=(
                "```yaml\n"
                "?qna              → Start Q&A trivia game\n"
                "?skip             → Skip current question\n"
                "?stop             → End game session\n"
                "?leaderboard      → View top players\n"
                "?reset_questions  → Reset question history (Admin)\n"
                "```"
            ),
            inline=False
        )

        embed.add_field(
            name="💰 Cash & Tài Xỉu System",
            value=(
                "```yaml\n"
                "?money            → Check your cash balance\n"
                "?daily            → Claim daily reward (streak bonus)\n"
                "?cashboard        → View cash leaderboard\n"
                "?moneyhack <amt>  → Give money to user (Admin)\n"
                "\n"
                "🎲 Tài Xỉu Over/Under Game:\n"
                "?tx               → Start new game (150s to bet)\n"
                "?cuoc <tai/xiu> <amt> → Place bet on outcome\n"
                "?txstop           → End current game instantly\n"
                "```"
            ),
            inline=False
        )

        embed.add_field(
            name="💖 Social Interactions",
            value=(
                "```css\n"
                "?kiss @user       → Kiss someone 💋\n"
                "?hug @user        → Hug someone 🤗\n"
                "?hs @user         → Handshake with someone 🤝\n"
                "?f*ck @user       → Flip them off 🖕\n"
                "```"
            ),
            inline=False
        )

        embed.add_field(
            name="🔧 Utility Tools",
            value=(
                "```css\n"
                "?echo [message]   → Repeat your message\n"
                "?help             → Show this command list\n"
                "?status           → Bot status and system info\n"
                "```"
            ),
            inline=False
        )

        embed.add_field(
            name="📋 Usage Notes",
            value=(
                "**🔐 Admin Commands:** Most security and moderation commands require admin permissions\n"
                "**⚡ Quick Access:** Use `?antispam` for detailed protection settings\n"
                "**🎯 Games:** Start with `?qna` for Vietnamese trivia challenges!\n"
                "**📊 Status:** Check `?status` for real-time bot health and server stats"
            ),
            inline=False
        )
        embed.set_footer(text=f"Serving {len(bot.guilds)} servers • All commands use ? prefix • Requested by {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url if ctx.author.display_avatar else None)
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
        try:
            await ctx.message.delete()  # Delete the user's original message to keep it secret
        except discord.errors.NotFound:
            pass  # Message was already deleted
        except discord.errors.Forbidden:
            pass  # Bot doesn't have permission to delete messages
        await ctx.send(message)

    @bot.command(name='ping')
    async def ping_command(ctx):
        """Test if bot is online"""
        await ctx.send("Tôi ở đây:)")

    # Game Commands
    @bot.command(name='qna')
    async def start_game(ctx):
        """Start a QNA game"""
        guild_id = str(ctx.guild.id)

        if guild_id in bot.active_games:
            embed = discord.Embed(
                title="🎮 QNA đã đang hoạt động",
                description="Một trò chơi QNA đã đang chạy trong máy chủ này!\n\nSử dụng `?stop` để kết thúc.",
                color=0xffa500
            )
            await ctx.send(embed=embed)
            return

        # Reset shown questions for a fresh game every time
        bot._reset_question_history(guild_id)
        shown_questions = set()  # Start with empty set for fresh game

        # Start with a placeholder question - let the generation loop provide all real questions
        current_question = {
            "question": "🔄 Bắt đầu tạo câu hỏi mới...", 
            "answer": "waiting", 
            "vietnamese_answer": "Đang khởi tạo...",
            "is_placeholder": True
        }

        bot.active_games[guild_id] = {
            'questions': [],  # No hardcoded questions - all questions come from generation loop
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
            'shown_questions': shown_questions,  # Load from database
            'new_questions': [],
            'waiting_message_sent': False  # Track if waiting message was sent
        }

        # Don't mark placeholder questions as shown in database

        embed = discord.Embed(
            title="🤔 Thử thách QNA đã kích hoạt!",
            description="**🧠 Đấu trường Hỏi & Đáp**\n\n*Kiểm tra kiến thức của bạn với các câu hỏi liên tục!*\n\n✨ **Sẵn sàng bắt đầu phiên QNA?**",
            color=0xff6b6b
        )
        embed.add_field(
            name="❓ Câu hỏi hiện tại",
            value=f"**{current_question['question']}**",
            inline=False
        )
        embed.add_field(
            name="🎯 Luật chơi",
            value="**📝 Định dạng trả lời:** Gõ câu trả lời trực tiếp\n**⚡ Thưởng tốc độ:** Câu trả lời đúng đầu tiên thắng!\n**🏆 Phần thưởng:** 10 điểm mỗi câu trả lời đúng\n**⏱️ Câu hỏi:** Câu hỏi mới mỗi 5 giây",
            inline=False
        )
        embed.set_footer(text="✨ Dùng ?stop để kết thúc phiên QNA • ?skip nếu bí • Trả lời liên tục!", icon_url=ctx.author.display_avatar.url if ctx.author.display_avatar else None)

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
                title="❌ Không có trò chơi QNA",
                description="Hiện tại không có trò chơi QNA nào đang chạy.",
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
                title="📈 Bảng xếp hạng QNA",
                description="Chưa có điểm nào được ghi nhận!\n\nChơi vài trò QNA với `?qna` để lên bảng xếp hạng!",
                color=0x5865f2
            )
            await ctx.send(embed=embed)
            return

        # Sort players by score
        sorted_players = sorted(bot.leaderboard[guild_id].items(), key=lambda x: x[1], reverse=True)

        embed = discord.Embed(
            title="🏆 Bảng xếp hạng QNA",
            description="🧠 **Các người chơi QNA hàng đầu trong máy chủ**",
            color=0xffd700
        )

        for i, (user_id, score) in enumerate(sorted_players[:10]):
            try:
                user = await bot.fetch_user(int(user_id))
                rank_emoji = ["🥇", "🥈", "🥉"][i] if i < 3 else f"{i+1}."
                embed.add_field(
                    name=f"{rank_emoji} {user.display_name}",
                    value=f"🎯 **{score} điểm**",
                    inline=True
                )
            except:
                continue

        embed.set_footer(text="Chơi ?qna để leo lên bảng xếp hạng!")
        await ctx.send(embed=embed)

    async def _end_game(ctx, guild_id):
        """End the QNA game and show results"""
        game = bot.active_games[guild_id]
        players = game['players']

        # Stop the continuous loops
        game['running'] = False

        if not players:
            embed = discord.Embed(
                title="🎮 QNA kết thúc",
                description="Phiên QNA kết thúc không có người chơi!",
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
                title="🎮 Phiên QNA hoàn thành!",
                description="🏁 **Kết quả cuối cùng**",
                color=0x00ff88
            )

            for i, (user_id, score) in enumerate(sorted_players[:5]):
                try:
                    user = await bot.fetch_user(int(user_id))
                    rank_emoji = ["🥇", "🥈", "🥉"][i] if i < 3 else f"{i+1}."
                    embed.add_field(
                        name=f"{rank_emoji} {user.display_name}",
                        value=f"🎯 {score} điểm",
                        inline=True
                    )
                except:
                    continue

            embed.set_footer(text="Phiên tuyệt vời mọi người! Dùng ?leaderboard để xem điểm tổng")
            await ctx.send(embed=embed)

        # Clean up game data
        del bot.active_games[guild_id]

    # Social Interaction Commands
    @bot.command(name='kiss')
    async def kiss_command(ctx, member: Optional[discord.Member] = None):
        """Kiss someone 💋"""
        if member is None:
            embed = discord.Embed(
                title="💋 Lệnh Kiss",
                description="Hãy chọn một người để hôn!\n\nSử dụng: `?kiss @người_nào_đó`",
                color=0xff69b4
            )
            await ctx.send(embed=embed)
            return

        if member == ctx.author:
            embed = discord.Embed(
                title="💋 Tự hôn mình?",
                description="Bạn không thể tự hôn chính mình! Hãy tìm ai đó khác 😉",
                color=0xff69b4
            )
            await ctx.send(embed=embed)
            return

        # Random kiss GIFs
        kiss_gifs = [
            "https://media.tenor.com/_8oadF3hZwIAAAAM/kiss.gif",
            "https://media.tenor.com/kmxEaVuW8AoAAAAM/kiss-gentle-kiss.gif",
            "https://media.tenor.com/BZyWzw2d5tAAAAAM/hyakkano-100-girlfriends.gif",
            "https://media.tenor.com/xYUjLVz6rJoAAAAM/mhel.gif",
            "https://media.tenor.com/z0UhWlFiC1EAAAAm/flamez-ivo.webp",
            "https://media.tenor.com/7kEaMuYWPYUAAAAm/haleys-ouo.webp"
        ]

        selected_gif = random.choice(kiss_gifs)

        embed = discord.Embed(
            title="💋 Kiss!",
            description=f"**{ctx.author.mention}** đã hôn vào môi của **{member.mention}**! 💕",
            color=0xff69b4
        )
        embed.set_image(url=selected_gif)
        embed.set_footer(text="Thật ngọt ngào! 💖")

        await ctx.send(embed=embed)

    @bot.command(name='hug')
    async def hug_command(ctx, member: Optional[discord.Member] = None):
        """Hug someone 🤗"""
        if member is None:
            embed = discord.Embed(
                title="🤗 Lệnh Hug",
                description="Hãy chọn một người để ôm!\n\nSử dụng: `?hug @người_nào_đó`",
                color=0xffa500
            )
            await ctx.send(embed=embed)
            return

        if member == ctx.author:
            embed = discord.Embed(
                title="🤗 Tự ôm mình?",
                description="Bạn đang cần một cái ôm thật sự từ ai đó! 💙",
                color=0xffa500
            )
            await ctx.send(embed=embed)
            return

        # Random hug GIFs
        hug_gifs = [
            "https://media.tenor.com/9lRjN-Sr204AAAAm/anime-anime-hug.webp",
            "https://media.tenor.com/P-8xYwXoGX0AAAAM/anime-hug-hugs.gif",
            "https://media.tenor.com/G_IvONY8EFgAAAAM/aharen-san-anime-hug.gif",
            "https://media.tenor.com/sGrFJCNL1_8AAAAM/anime-sevendeadlysins.gif",
            "https://media.tenor.com/JusdVlKJLbsAAAAM/cute-anime.gif",
            "https://media.tenor.com/W9Z5NRFZq_UAAAAM/excited-hug.gif",
            "https://media.tenor.com/sl3rfZ7mQBsAAAAM/anime-hug-canary-princess.gif",
            "https://media.tenor.com/JzxgF3aebL0AAAAM/hug-hugging.gif"
        ]

        selected_gif = random.choice(hug_gifs)

        embed = discord.Embed(
            title="🤗 Hug!",
            description=f"**{ctx.author.mention}** đã ôm chặt **{member.mention}**! 💙",
            color=0xffa500
        )
        embed.set_image(url=selected_gif)
        embed.set_footer(text="Ấm áp và dễ thương! 🥰")

        await ctx.send(embed=embed)

    @bot.command(name='hs')
    async def handshake_command(ctx, member: Optional[discord.Member] = None):
        """Handshake with someone 🤝"""
        if member is None:
            embed = discord.Embed(
                title="🤝 Lệnh Handshake",
                description="Hãy chọn một người để bắt tay!\n\nSử dụng: `?hs @người_nào_đó`",
                color=0x5865f2
            )
            await ctx.send(embed=embed)
            return

        if member == ctx.author:
            embed = discord.Embed(
                title="🤝 Tự bắt tay?",
                description="Bạn không thể bắt tay với chính mình! Hãy tìm bạn bè 😄",
                color=0x5865f2
            )
            await ctx.send(embed=embed)
            return

        # Random handshake GIFs
        handshake_gifs = [
            "https://media.tenor.com/RWD2XL_CxdcAAAAM/hug.gif",
            "https://media.tenor.com/hqvisWep1eUAAAAm/ash-dawn-hug-anime-hug.webp",
            "https://media.tenor.com/0770vFtv1xAAAAAm/heart-hug.webp",
            "https://media.tenor.com/ymN_FUny2CYAAAAM/handshake-deal.gif",
            "https://media.tenor.com/DYJ2sNZQBkIAAAAM/handshake-shake-hands.gif",
            "https://media.tenor.com/c_KzMTlCXHQAAAAM/friends-handshake.gif"
        ]

        selected_gif = random.choice(handshake_gifs)

        embed = discord.Embed(
            title="🤝 Handshake!",
            description=f"**{ctx.author.mention}** đã bắt tay với **{member.mention}**! 🤝",
            color=0x5865f2
        )
        embed.set_image(url=selected_gif)
        embed.set_footer(text="Tình bạn đẹp! 👫")

        await ctx.send(embed=embed)

    @bot.command(name='f*ck')
    async def fck_command(ctx, member: Optional[discord.Member] = None):
        """Give someone the middle finger 🖕"""
        if member is None:
            embed = discord.Embed(
                title="🖕 Thể hiện thái độ",
                description="Chọn một người để thể hiện thái độ không hài lòng! 🖕\n\nCách dùng: `?f*ck @tên_người`",
                color=0xff4500
            )
            await ctx.send(embed=embed)
            return

        if member == ctx.author:
            embed = discord.Embed(
                title="🖕 Không thể tự nhắm vào mình!",
                description="Bạn không thể sử dụng lệnh này với chính mình! Hãy chọn người khác 😤",
                color=0xff4500
            )
            await ctx.send(embed=embed)
            return

        # Random middle finger GIFs
        middle_finger_gifs = [
            "https://media.tenor.com/YQpvQAW-2VcAAAAM/anime-middle-finger.gif",
            "https://media.tenor.com/H7OVBcUBE7QAAAAM/middle-finger-anime.gif",
            "https://media.tenor.com/rL3CPcYztOsAAAAM/anime-finger.gif",
            "https://media.tenor.com/e0pUE4nqbKgAAAAM/middle-finger.gif",
            "https://media.tenor.com/4wEUbVm8EEYAAAAM/anime-mad.gif",
            "https://media.tenor.com/zwKvQ9A-VFIAAAAM/fuck-you-middle-finger.gif"
        ]

        selected_gif = random.choice(middle_finger_gifs)

        embed = discord.Embed(
            title="🖕 F*ck You!",
            description=f"**{ctx.author.mention}** đã chỉ thẳng mặt **{member.mention}**! 🖕😤",
            color=0xff4500
        )
        embed.set_image(url=selected_gif)
        embed.set_footer(text="Ai bảo làm phiền! 😤🖕")

        await ctx.send(embed=embed)

    # === CASH SYSTEM HELPER METHODS ===
    def _get_user_cash(self, guild_id, user_id):
        """Get user's cash amount and daily streak info"""
        connection = self._get_db_connection()
        if not connection:
            # Use in-memory storage when database isn't available
            key = f"{guild_id}_{user_id}"
            if key in self.user_cash_memory:
                data = self.user_cash_memory[key]
                return data.get('cash', 1000), data.get('last_daily'), data.get('daily_streak', 0)
            else:
                # Give new users some starting cash
                return 1000, None, 0

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT cash, last_daily, daily_streak FROM user_cash WHERE guild_id = %s AND user_id = %s",
                    (str(guild_id), str(user_id))
                )
                result = cursor.fetchone()
                if result:
                    return result[0], result[1], result[2]
                else:
                    # Create new user with starting cash instead of returning 0
                    cursor.execute(
                        "INSERT INTO user_cash (guild_id, user_id, cash) VALUES (%s, %s, %s)",
                        (str(guild_id), str(user_id), 1000)
                    )
                    connection.commit()
                    return 1000, None, 0
        except Exception as e:
            logger.error(f"Error getting user cash: {e}")
            return 0, None, 0
        finally:
            connection.close()

    def _update_user_cash(self, guild_id, user_id, cash_amount, last_daily=None, daily_streak=None):
        """Update user's cash amount and daily streak"""
        if not self.db_connection:
            return False

        try:
            with self.db_connection.cursor() as cursor:
                if last_daily is not None and daily_streak is not None:
                    cursor.execute(
                        """INSERT INTO user_cash (guild_id, user_id, cash, last_daily, daily_streak) 
                           VALUES (%s, %s, %s, %s, %s) 
                           ON CONFLICT (guild_id, user_id) 
                           DO UPDATE SET cash = %s, last_daily = %s, daily_streak = %s""",
                        (str(guild_id), str(user_id), cash_amount, last_daily, daily_streak,
                         cash_amount, last_daily, daily_streak)
                    )
                else:
                    cursor.execute(
                        """INSERT INTO user_cash (guild_id, user_id, cash) 
                           VALUES (%s, %s, %s) 
                           ON CONFLICT (guild_id, user_id) 
                           DO UPDATE SET cash = user_cash.cash + %s""",
                        (str(guild_id), str(user_id), cash_amount, cash_amount)
                    )
                self.db_connection.commit()
                return True
        except Exception as e:
            logger.error(f"Error updating user cash: {e}")
            return False

    def _calculate_daily_reward(self, streak):
        """Calculate daily reward based on streak (streak=1 is first day)"""
        base_reward = 1000
        if streak <= 1:
            return base_reward  # First day = 1000 cash
        elif streak == 2:
            return 1200  # Second consecutive day
        elif streak == 3:
            return 1500  # Third consecutive day
        else:
            # Continue increasing by 400 per day after day 3
            return 1500 + (400 * (streak - 3))

    # === CASH SYSTEM COMMANDS ===
    @bot.command(name='money')
    async def show_money(ctx):
        """Show user's current money balance"""
        guild_id = str(ctx.guild.id)
        user_id = str(ctx.author.id)

        current_cash, last_daily, streak = bot._get_user_cash(guild_id, user_id)

        embed = discord.Embed(
            title="💰 Thông tin tài khoản",
            description=f"**{ctx.author.mention}** - Chi tiết tài khoản của bạn",
            color=0x00ff88
        )
        embed.add_field(
            name="💎 Tài sản hiện tại",
            value=f"**{current_cash:,} VND**",
            inline=True
        )
        embed.add_field(
            name="🔥 Chuỗi ngày liên tiếp",
            value=f"**{streak} ngày**",
            inline=True
        )
        if last_daily:
            embed.add_field(
                name="📅 Lần check-in cuối cùng",
                value=f"**{last_daily}**",
                inline=True
            )
        embed.set_footer(text="Sử dụng ?daily để check-in và nhận thưởng hàng ngày! 🎁")
        await ctx.send(embed=embed)

    # === DAILY REWARD COMMAND ===
    @bot.command(name='daily')
    async def daily_reward(ctx):
        """Claim daily reward with streak bonus"""
        guild_id = str(ctx.guild.id)
        user_id = str(ctx.author.id)
        today = datetime.utcnow().date()
        # Use atomic function to prevent race conditions and multiple earnings
        result = await bot._claim_daily_reward(guild_id, user_id, today)
        
        # Check if already claimed today
        if result is None:
            current_cash, last_daily, streak = bot._get_user_cash(guild_id, user_id)
            embed = discord.Embed(
                title="⏰ Hôm nay đã check-in rồi!",
                description=f"Bạn đã hoàn thành check-in hàng ngày rồi!\n\n💎 **Tài sản hiện tại:** {current_cash:,} VND\n🔥 **Chuỗi ngày:** {streak} ngày",
                color=0xffa500
            )
            embed.add_field(
                name="⏰ Lịch trình",
                value="Hãy quay lại vào ngày mai để tiếp tục chuỗi check-in của bạn!",
                inline=False
            )
            await ctx.send(embed=embed)
            return

        # Check for database error
        if result is False:
            embed = discord.Embed(
                title="❌ Lỗi hệ thống",
                description="Đã xảy ra lỗi khi xử lý check-in hàng ngày. Vui lòng thử lại sau ít phút.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Successfully claimed - result is (reward, new_cash, new_streak, old_streak)
        reward, new_cash, new_streak, old_streak = result

        # Create success embed
        embed = discord.Embed(
            title="🎁 Check-in thành công!",
            description=f"**{ctx.author.mention}** đã hoàn thành check-in hàng ngày!",
            color=0x00ff88
        )
        embed.add_field(
            name="💎 Phần thưởng",
            value=f"**+{reward:,} VND**",
            inline=True
        )
        embed.add_field(
            name="🔥 Chuỗi ngày",
            value=f"**{new_streak + 1} ngày**",
            inline=True
        )
        embed.add_field(
            name="💰 Tổng tài sản",
            value=f"**{new_cash:,} VND**",
            inline=True
        )

        if new_streak > old_streak:
            embed.add_field(
                name="🚀 Chuỗi ngày mới!",
                value=f"Chuỗi check-in tăng lên {new_streak + 1} ngày! Phần thưởng ngày mai sẽ cao hơn!",
                inline=False
            )
        elif new_streak == 0 and old_streak > 0:
            embed.add_field(
                name="💔 Chuỗi ngày bị ngắt",
                value="Bạn đã bỏ lỡ một ngày, chuỗi check-in đã được khởi động lại từ ngày 1.",
                inline=False
            )

        embed.set_footer(text="Hãy nhớ check-in hàng ngày để duy trì chuỗi ngày! 🔥")
        await ctx.send(embed=embed)

    @bot.command(name='cashboard')
    async def cash_leaderboard(ctx, page: int = 1):
        """Show cash leaderboard with pagination"""
        guild_id = str(ctx.guild.id)

        try:
            # Try database first, fall back to memory if database unavailable
            connection = bot._get_db_connection()
            users_data = []

            if connection:
                # Use database data
                with connection.cursor() as cursor:
                    cursor.execute(
                        """SELECT user_id, cash, daily_streak 
                           FROM user_cash 
                           WHERE guild_id = %s AND cash > 0 
                           ORDER BY cash DESC""",
                        (guild_id,)
                    )
                    results = cursor.fetchall()
                    users_data = [(user_id, cash, streak) for user_id, cash, streak in results]
                connection.close()
            else:
                # Use in-memory data when database is unavailable
                for key, data in bot.user_cash_memory.items():
                    if key.startswith(f"{guild_id}_") and data.get('cash', 0) > 0:
                        user_id = key.split('_', 1)[1]  # Extract user_id from "guild_id_user_id"
                        cash = data.get('cash', 0)
                        streak = data.get('daily_streak', 0)
                        users_data.append((user_id, cash, streak))

                # Sort by cash (descending)
                users_data.sort(key=lambda x: x[1], reverse=True)

            total_users = len(users_data)

            if total_users == 0:
                embed = discord.Embed(
                    title="📈 Bảng xếp hạng Cash",
                    description="Chưa có ai có tiền trong máy chủ này!\n\nDùng `?daily` để bắt đầu kiếm cash!",
                    color=0x5865f2
                )
                await ctx.send(embed=embed)
                return

            # Calculate pagination
            per_page = 10
            total_pages = (total_users + per_page - 1) // per_page

            if page < 1 or page > total_pages:
                embed = discord.Embed(
                    title="❌ Trang không hợp lệ",
                    description=f"Vui lòng chọn trang từ 1 đến {total_pages}",
                    color=0xff4444
                )
                await ctx.send(embed=embed)
                return

            # Get data for this page
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            page_data = users_data[start_idx:end_idx]

            embed = discord.Embed(
                title="🏆 Bảng xếp hạng Cash",
                description=f"💰 **Top người giàu nhất trong máy chủ**\n📄 Trang {page}/{total_pages}",
                color=0xffd700
            )

            for i, (user_id, cash, streak) in enumerate(page_data):
                try:
                    user = await bot.fetch_user(int(user_id))
                    rank = start_idx + i + 1

                    if rank == 1:
                        rank_emoji = "🥇"
                    elif rank == 2:
                        rank_emoji = "🥈" 
                    elif rank == 3:
                        rank_emoji = "🥉"
                    else:
                        rank_emoji = f"{rank}."

                    embed.add_field(
                        name=f"{rank_emoji} {user.display_name}",
                        value=f"💰 **{cash:,} cash**\n🔥 {streak} ngày streak",
                        inline=True
                    )
                except:
                    # Skip if user can't be fetched
                    continue

            if total_pages > 1:
                embed.set_footer(text=f"Dùng ?cashboard <số trang> để xem trang khác • Trang {page}/{total_pages}")
            else:
                embed.set_footer(text="Dùng ?daily để kiếm cash!")

            # Add note about data source
            if not connection:
                embed.add_field(
                    name="ℹ️ Thông tin",
                    value="Dữ liệu từ bộ nhớ tạm (database không khả dụng)",
                    inline=False
                )

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error getting cash leaderboard: {e}")
            embed = discord.Embed(
                title="❌ Lỗi hệ thống",
                description="Có lỗi xảy ra khi lấy bảng xếp hạng. Vui lòng thử lại sau.",
                color=0xff4444
            )
            await ctx.send(embed=embed)

    # === OVER/UNDER GAME COMMANDS ===
    @bot.command(name='tx')
    async def start_overunder(ctx):
        """Start an Over/Under betting game"""
        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)
        game_id = f"{guild_id}_{channel_id}_{int(datetime.utcnow().timestamp())}"

        # Check if there's already an active game in this channel
        if guild_id in bot.overunder_games:
            for existing_game_id, game_data in bot.overunder_games[guild_id].items():
                if game_data['channel_id'] == channel_id and game_data['status'] == 'active':
                    embed = discord.Embed(
                        title="⚠️ Đã có game đang diễn ra!",
                        description="Kênh này đã có một game Over/Under đang diễn ra. Vui lòng đợi game hiện tại kết thúc.",
                        color=0xffa500
                    )
                    await ctx.send(embed=embed)
                    return

        # Create new game
        end_time = datetime.utcnow() + timedelta(seconds=30)

        if guild_id not in bot.overunder_games:
            bot.overunder_games[guild_id] = {}

        bot.overunder_games[guild_id][game_id] = {
            'channel_id': channel_id,
            'end_time': end_time,
            'bets': [],
            'status': 'active',
            'result': None,
            'end_task': None
        }

        # Store in database
        try:
            connection = bot._get_db_connection()
            if connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO overunder_games (game_id, guild_id, channel_id) VALUES (%s, %s, %s)",
                        (game_id, guild_id, channel_id)
                    )
                    connection.commit()
                connection.close()
        except Exception as e:
            logger.error(f"Error storing game in database: {e}")

        embed = discord.Embed(
            title="🎲 Game Đoán Số Bắt Đầu!",
            description="**Chào mừng bạn tham gia game đoán số hấp dẫn!**\n\nHãy dự đoán kết quả sẽ là Tài (cao) hay Xỉu (thấp)!",
            color=0x00ff88
        )
        embed.add_field(
            name="⏱️ Thời gian cược",
            value="**30 giây** để đặt cược",
            inline=True
        )
        embed.add_field(
            name="🎯 Hướng dẫn",
            value="Gõ `?cuoc <tai/xiu> <số tiền>`",
            inline=True
        )
        embed.add_field(
            name="💸 Tiền thưởng",
            value="**Nhân đôi** số tiền cược khi thắng!",
            inline=True
        )
        embed.add_field(
            name="📝 Ví dụ thực tế",
            value="`?cuoc tai 1000` - Đặt cược 1000 VND vào Tài\n`?cuoc xiu 500` - Đặt cược 500 VND vào Xỉu",
            inline=False
        )
        embed.set_footer(text=f"Game ID: {game_id} • Kết thúc lúc {end_time.strftime('%H:%M:%S')}")

        await ctx.send(embed=embed)

        # Schedule game end
        game_task = asyncio.create_task(bot._end_overunder_game(guild_id, game_id))
        bot.overunder_games[guild_id][game_id]['end_task'] = game_task

    @bot.command(name='cuoc')
    async def place_bet(ctx, side=None, amount=None):
        """Place a bet in the Tai/Xiu game"""
        if not side or not amount:
            embed = discord.Embed(
                title="❌ Sai cú pháp!",
                description="Cách sử dụng: `?cuoc <tai/xiu> <số tiền>`\n\n**Ví dụ:**\n`?cuoc tai 1000` - Cược 1,000 cash\n`?cuoc xiu 5k` - Cược 5,000 cash\n`?cuoc tai 1.5m` - Cược 1,500,000 cash\n`?cuoc xiu 2b` - Cược 2,000,000,000 cash\n`?cuoc tai 5t` - Cược 5,000,000,000,000 cash\n`?cuoc xiu 1qa` - Cược 1,000,000,000,000,000 cash\n`?cuoc tai 2qi` - Cược 2,000,000,000,000,000,000 cash\n`?cuoc xiu 1sx` - Cược 1,000,000,000,000,000,000,000 cash\n`?cuoc tai all` - Cược tất cả tiền",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)
        user_id = str(ctx.author.id)

        # Validate side
        side = side.lower()
        if side not in ['tai', 'xiu']:
            embed = discord.Embed(
                title="❌ Lựa chọn không hợp lệ!",
                description="Bạn chỉ có thể chọn **tai** hoặc **xiu**",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Validate amount with support for k/m/b/t/qa/qi/sx suffixes and 'all'
        def parse_amount(amount_str):
            """Parse amount string with k/m/b/t/qa/qi/sx suffixes and 'all' for all available money"""
            amount_str = amount_str.lower().strip()

            # Handle 'all' - return special value that we'll replace with actual cash
            if amount_str == 'all':
                return -1  # Special value to indicate "all money"

            multiplier = 1

            if amount_str.endswith('sx'):
                multiplier = 1_000_000_000_000_000_000_000  # Sextillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('qi'):
                multiplier = 1_000_000_000_000_000_000  # Quintillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('qa'):
                multiplier = 1_000_000_000_000_000  # Quadrillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('t'):
                multiplier = 1_000_000_000_000  # Trillion
                amount_str = amount_str[:-1]
            elif amount_str.endswith('b'):
                multiplier = 1_000_000_000  # Billion
                amount_str = amount_str[:-1]
            elif amount_str.endswith('m'):
                multiplier = 1_000_000  # Million
                amount_str = amount_str[:-1]
            elif amount_str.endswith('k'):
                multiplier = 1_000  # Thousand
                amount_str = amount_str[:-1]

            try:
                base_amount = float(amount_str)
                if base_amount <= 0:
                    raise ValueError()
                return int(base_amount * multiplier)
            except (ValueError, OverflowError):
                raise ValueError()

        try:
            bet_amount = parse_amount(amount)
        except ValueError:
            embed = discord.Embed(
                title="❌ Số tiền không hợp lệ!",
                description="Vui lòng nhập số tiền hợp lệ.\n\n**Ví dụ:** `1000`, `5k`, `1.5m`, `2b`, `5t`, `1qa`, `2qi`, `1sx`, `all`",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Handle 'all' - get user's current cash and bet all of it
        if bet_amount == -1:
            current_cash, _, _ = bot._get_user_cash(guild_id, user_id)
            if current_cash <= 0:
                embed = discord.Embed(
                    title="💸 Tài sản không đủ!",
                    description="Bạn không có đủ tiền để đặt cược.\n\nSử dụng `?daily` để check-in và nhận thưởng!",
                    color=0xff4444
                )
                await ctx.send(embed=embed)
                return
            bet_amount = current_cash

        # Check if there's an active game in this channel
        active_game = None
        if guild_id in bot.overunder_games:
            for game_id, game_data in bot.overunder_games[guild_id].items():
                if game_data['channel_id'] == channel_id and game_data['status'] == 'active':
                    active_game = (game_id, game_data)
                    break

        if not active_game:
            embed = discord.Embed(
                title="❌ Không có game nào đang diễn ra!",
                description="Không có game Tài Xỉu nào đang diễn ra trong kênh này. Dùng `?tx` để bắt đầu game mới.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        game_id, game_data = active_game

        # Check if game has ended
        if datetime.utcnow() >= game_data['end_time']:
            embed = discord.Embed(
                title="⏰ Vòng cược đã kết thúc!",
                description="Hết thời gian đặt cược rồi. Đợi kết quả hoặc tạo game mới.",
                color=0xffa500
            )
            await ctx.send(embed=embed)
            return

        # Check user's cash
        current_cash, _, _ = bot._get_user_cash(guild_id, user_id)
        if current_cash < bet_amount:
            embed = discord.Embed(
                title="💸 Tài sản không đủ!",
                description=f"Tài sản của bạn: **{current_cash:,} VND**\nSố tiền muốn cược: **{bet_amount:,} VND**\n\nSử dụng `?daily` để check-in và nhận thưởng!",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Check if user already has a bet in this game
        for bet in game_data['bets']:
            if bet['user_id'] == user_id:
                embed = discord.Embed(
                    title="⚠️ Bạn đã tham gia rồi!",
                    description=f"Bạn đã đặt cược **{bet['amount']:,} VND** vào **{bet['side'].upper()}** cho game này rồi.",
                    color=0xffa500
                )
                await ctx.send(embed=embed)
                return

        # Deduct cash from user
        success = bot._update_user_cash(guild_id, user_id, -bet_amount, None, None)

        if not success:
            embed = discord.Embed(
                title="❌ Xảy ra lỗi!",
                description="Không thể xử lý giao dịch cược của bạn. Vui lòng thử lại sau ít giây.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Calculate remaining cash
        remaining_cash = current_cash - bet_amount

        # Add bet to game
        bet_data = {
            'user_id': user_id,
            'username': ctx.author.display_name,
            'side': side,
            'amount': bet_amount
        }
        game_data['bets'].append(bet_data)

        # Note: Bets are stored in memory during the game
        # Final results are saved to database when game ends

        # Beautiful success embed
        embed = discord.Embed(
            title="🎯 Đặt Cược Thành Công!",
            description=f"🎲 **{ctx.author.display_name}** đã tham gia game Tài Xỉu!",
            color=0x00ff88
        )
        embed.add_field(
            name="🎰 Lựa chọn của bạn",
            value=f"**{'🔺 TÀI' if side == 'tai' else '🔻 XỈU'}**",
            inline=True
        )
        embed.add_field(
            name="💰 Số tiền đã cược",
            value=f"**{bet_amount:,}** cash",
            inline=True
        )
        embed.add_field(
            name="💳 Số dư hiện tại",
            value=f"**{remaining_cash:,}** cash",
            inline=True
        )
        embed.add_field(
            name="🏆 Tiền thưởng nếu thắng",
            value=f"**{bet_amount * 2:,}** cash",
            inline=True
        )
        embed.add_field(
            name="👥 Tổng người chơi",
            value=f"**{len(game_data['bets'])}** người",
            inline=True
        )

        time_left = game_data['end_time'] - datetime.utcnow()
        minutes, seconds = divmod(int(time_left.total_seconds()), 60)
        embed.set_footer(text=f"Thời gian còn lại: {minutes}:{seconds:02d} • Chúc may mắn! 🍀")

        await ctx.send(embed=embed)


    @bot.command(name='txshow')
    async def show_overunder_result(ctx):
        """Start continuous auto-cycling: end current round, show winner, auto-start new rounds until gamestop"""
        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)
        channel_key = f"{guild_id}_{channel_id}"

        # Find active game in this channel
        active_game_id = None
        if guild_id in bot.overunder_games:
            for game_id, game_data in bot.overunder_games[guild_id].items():
                if game_data['channel_id'] == channel_id and game_data['status'] == 'active':
                    active_game_id = game_id
                    break

        if not active_game_id:
            embed = discord.Embed(
                title="❌ Không có game Tài Xỉu",
                description="Hiện tại không có game Tài Xỉu nào đang chạy trong kênh này.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Enable auto-cycle for this channel
        bot.overunder_autocycle[channel_key] = True
        
        # End current game immediately and show results
        embed = discord.Embed(
            title="🔄 Bắt đầu chế độ tự động!",
            description="Game hiện tại sẽ kết thúc và tự động bắt đầu game mới liên tục!\n\nDùng `?gamestop` để dừng.",
            color=0x00ff88
        )
        await ctx.send(embed=embed)

        # End game immediately - this will trigger auto-cycle
        await bot._end_overunder_game(guild_id, active_game_id, instant_stop=True)

    @bot.command(name='gamestop')
    async def stop_overunder(ctx):
        """Stop the current Tai/Xiu game instantly and show results"""
        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)

        # Find active game in this channel
        active_game_id = None
        if guild_id in bot.overunder_games:
            for game_id, game_data in bot.overunder_games[guild_id].items():
                if game_data['channel_id'] == channel_id and game_data['status'] == 'active':
                    active_game_id = game_id
                    break

        if not active_game_id:
            embed = discord.Embed(
                title="❌ Không có game Tài Xỉu",
                description="Hiện tại không có game Tài Xỉu nào đang chạy trong kênh này.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Stop auto-cycle if active
        channel_key = f"{guild_id}_{channel_id}"
        if channel_key in bot.overunder_autocycle:
            del bot.overunder_autocycle[channel_key]
            embed = discord.Embed(
                title="⏹️ Dừng chế độ tự động",
                description="Đã tắt chế độ tự động và dừng game Tài Xỉu! Đang công bố kết quả cuối cùng...",
                color=0xffa500
            )
        else:
            embed = discord.Embed(
                title="⏹️ Dừng game Tài Xỉu",
                description="Game Tài Xỉu đã được dừng! Đang công bố kết quả...",
                color=0xffa500
            )
        await ctx.send(embed=embed)

        # End game immediately
        await bot._end_overunder_game(guild_id, active_game_id, instant_stop=True)

    @bot.command(name='reset_questions')
    @commands.has_permissions(administrator=True)
    async def reset_questions(ctx):
        """Reset question history for the server (Admin only)"""
        guild_id = str(ctx.guild.id)
        bot._reset_question_history(guild_id)

        embed = discord.Embed(
            title="🔄 Lịch sử câu hỏi đã được reset",
            description="Tất cả câu hỏi có thể được hỏi lại từ đầu.\n\nNgười chơi sẽ gặp các câu hỏi đã hỏi trước đó trong phiên chơi mới.",
            color=0x00ff88
        )
        await ctx.send(embed=embed)

    @bot.command(name='moneyhack')
    @commands.has_permissions(administrator=True)
    async def moneyhack(ctx, amount: int, user: discord.Member = None):
        """Give money to a user (Admin only)"""
        if user is None:
            user = ctx.author

        guild_id = str(ctx.guild.id)
        user_id = str(user.id)

        if amount <= 0:
            embed = discord.Embed(
                title="❌ Số tiền không hợp lệ",
                description="Số tiền phải lớn hơn 0.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Get current cash
        current_cash, last_daily, streak = bot._get_user_cash(guild_id, user_id)
        new_cash = current_cash + amount

        # Update user's cash
        success = bot._update_user_cash(guild_id, user_id, new_cash, last_daily, streak)

        if success:
            embed = discord.Embed(
                title="💰 Money Hack Thành Công!",
                description=f"**Admin {ctx.author.mention}** đã tặng tiền cho **{user.mention}**",
                color=0x00ff88
            )
            embed.add_field(
                name="💵 Số tiền tặng",
                value=f"**+{amount:,} cash**",
                inline=True
            )
            embed.add_field(
                name="💳 Số dư mới",
                value=f"**{new_cash:,} cash**",
                inline=True
            )
            embed.set_footer(text="Chỉ Admin mới có thể sử dụng lệnh này!")
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Lỗi hệ thống",
                description="Không thể cập nhật số dư. Vui lòng thử lại sau.",
                color=0xff4444
            )
            await ctx.send(embed=embed)

    @bot.command(name='give')
    async def give_money(ctx, user: discord.Member = None, amount: str = None):
        """Give money to another user"""
        if user is None or amount is None:
            embed = discord.Embed(
                title="❌ Sai cú pháp!",
                description="Cách sử dụng: `?give <@user> <số tiền>`\n\n**Ví dụ:**\n`?give @user 1000` - Tặng 1,000 cash\n`?give @user 5k` - Tặng 5,000 cash\n`?give @user 1.5m` - Tặng 1,500,000 cash\n`?give @user 2b` - Tặng 2,000,000,000 cash\n`?give @user 5t` - Tặng 5,000,000,000,000 cash\n`?give @user all` - Tặng tất cả tiền của bạn",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        guild_id = str(ctx.guild.id)
        giver_id = str(ctx.author.id)
        receiver_id = str(user.id)

        # Don't let users give money to themselves
        if giver_id == receiver_id:
            embed = discord.Embed(
                title="❌ Không thể tự tặng tiền cho mình!",
                description="Bạn không thể tặng tiền cho chính mình.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Parse amount with support for k/m/b/t/qa/qi/sx suffixes and 'all'
        def parse_amount(amount_str):
            """Parse amount string with k/m/b/t/qa/qi/sx suffixes and 'all' for all available money"""
            amount_str = amount_str.lower().strip()

            # Handle 'all' - return special value that we'll replace with actual cash
            if amount_str == 'all':
                return -1  # Special value to indicate "all money"

            multiplier = 1

            if amount_str.endswith('sx'):
                multiplier = 1_000_000_000_000_000_000_000  # Sextillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('qi'):
                multiplier = 1_000_000_000_000_000_000  # Quintillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('qa'):
                multiplier = 1_000_000_000_000_000  # Quadrillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('t'):
                multiplier = 1_000_000_000_000  # Trillion
                amount_str = amount_str[:-1]
            elif amount_str.endswith('b'):
                multiplier = 1_000_000_000  # Billion
                amount_str = amount_str[:-1]
            elif amount_str.endswith('m'):
                multiplier = 1_000_000  # Million
                amount_str = amount_str[:-1]
            elif amount_str.endswith('k'):
                multiplier = 1_000  # Thousand
                amount_str = amount_str[:-1]

            try:
                base_amount = float(amount_str)
                if base_amount <= 0:
                    raise ValueError()
                return int(base_amount * multiplier)
            except (ValueError, OverflowError):
                raise ValueError()

        try:
            give_amount = parse_amount(amount)
        except ValueError:
            embed = discord.Embed(
                title="❌ Số tiền không hợp lệ!",
                description="Vui lòng nhập số tiền hợp lệ.\n\n**Ví dụ:** `1000`, `5k`, `1.5m`, `2b`, `5t`, `1qa`, `2qi`, `1sx`, `all`",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Get giver's current cash
        giver_cash, giver_daily, giver_streak = bot._get_user_cash(guild_id, giver_id)

        # Handle 'all' - give all of giver's money
        if give_amount == -1:
            if giver_cash <= 0:
                embed = discord.Embed(
                    title="💸 Không có tiền để tặng!",
                    description="Bạn không có tiền để tặng cho ai.\n\nDùng `?daily` để nhận thưởng hàng ngày!",
                    color=0xff4444
                )
                await ctx.send(embed=embed)
                return
            give_amount = giver_cash

        # Check if giver has enough money
        if giver_cash < give_amount:
            embed = discord.Embed(
                title="💸 Không đủ tiền!",
                description=f"Bạn chỉ có **{giver_cash:,} cash** nhưng muốn tặng **{give_amount:,} cash**.\n\nDùng `?money` để kiểm tra số dư.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Get receiver's current cash
        receiver_cash, receiver_daily, receiver_streak = bot._get_user_cash(guild_id, receiver_id)

        # Update both users' cash
        new_giver_cash = giver_cash - give_amount
        new_receiver_cash = receiver_cash + give_amount

        # Update giver's cash (subtract)
        success1 = bot._update_user_cash(guild_id, giver_id, new_giver_cash, giver_daily, giver_streak)
        # Update receiver's cash (add)
        success2 = bot._update_user_cash(guild_id, receiver_id, new_receiver_cash, receiver_daily, receiver_streak)

        if success1 and success2:
            embed = discord.Embed(
                title="💝 Chuyển tiền thành công!",
                description=f"**{ctx.author.mention}** đã tặng tiền cho **{user.mention}**",
                color=0x00ff88
            )
            embed.add_field(
                name="💰 Số tiền tặng",
                value=f"**{give_amount:,} cash**",
                inline=True
            )
            embed.add_field(
                name="👤 Người tặng",
                value=f"{ctx.author.mention}\n💳 Còn lại: **{new_giver_cash:,} cash**",
                inline=True
            )
            embed.add_field(
                name="🎁 Người nhận",
                value=f"{user.mention}\n💳 Tổng cộng: **{new_receiver_cash:,} cash**",
                inline=True
            )
            embed.set_footer(text="Cảm ơn bạn đã chia sẻ!")
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Lỗi hệ thống",
                description="Không thể thực hiện giao dịch. Vui lòng thử lại sau.",
                color=0xff4444
            )
            await ctx.send(embed=embed)

    @bot.command(name='clear')
    @commands.has_permissions(administrator=True)
    async def clear_money(ctx, user: discord.Member = None):
        """Reset a user's money to 0 (Admin only)"""
        if user is None:
            embed = discord.Embed(
                title="❌ Sai cú pháp!",
                description="Cách sử dụng: `?clear <@user>`\n\n**Ví dụ:**\n`?clear @user` - Reset tiền của user về 0",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        guild_id = str(ctx.guild.id)
        user_id = str(user.id)

        # Get user's current cash
        current_cash, last_daily, streak = bot._get_user_cash(guild_id, user_id)

        # Reset user's cash to 0
        success = bot._update_user_cash(guild_id, user_id, 0, last_daily, streak)

        if success:
            embed = discord.Embed(
                title="🗑️ Reset tiền thành công!",
                description=f"**Admin {ctx.author.mention}** đã reset tiền của **{user.mention}**",
                color=0x00ff88
            )
            embed.add_field(
                name="💰 Tiền trước đó",
                value=f"**{current_cash:,} cash**",
                inline=True
            )
            embed.add_field(
                name="💳 Tiền hiện tại",
                value="**0 cash**",
                inline=True
            )
            embed.set_footer(text="Chỉ Admin mới có thể sử dụng lệnh này!")
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Lỗi hệ thống",
                description="Không thể reset tiền của người dùng. Vui lòng thử lại sau.",
                color=0xff4444
            )
            await ctx.send(embed=embed)

    @bot.command(name='win')
    @commands.has_permissions(administrator=True)
    async def set_winner(ctx, result: str = None):
        """Manually set the winner of the current game (Admin only)"""
        if not result:
            embed = discord.Embed(
                title="❌ Sai cú pháp!",
                description="Cách sử dụng: `?win <tai/xiu>`\n\n**Ví dụ:**\n`?win tai` - Đặt kết quả là Tài\n`?win xiu` - Đặt kết quả là Xỉu",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)

        # Validate result
        result = result.lower()
        if result not in ['tai', 'xiu']:
            embed = discord.Embed(
                title="❌ Kết quả không hợp lệ!",
                description="Bạn chỉ có thể chọn **tai** hoặc **xiu**",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Check if there's an active game in this channel
        active_game = None
        if guild_id in bot.overunder_games:
            for game_id, game_data in bot.overunder_games[guild_id].items():
                if game_data['channel_id'] == channel_id and game_data['status'] == 'active':
                    active_game = (game_id, game_data)
                    break

        if not active_game:
            embed = discord.Embed(
                title="❌ Không có game nào đang diễn ra!",
                description="Không có game Tài Xỉu nào đang diễn ra trong kênh này. Dùng `?tx` để bắt đầu game mới.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        game_id, game_data = active_game

        # Set the result manually
        game_data['result'] = result
        game_data['status'] = 'ended'

        # Update database
        try:
            connection = bot._get_db_connection()
            if connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "UPDATE overunder_games SET result = %s, status = 'ended' WHERE game_id = %s",
                        (result, game_id)
                    )
                    connection.commit()
                connection.close()
        except Exception as e:
            logger.error(f"Error updating game result: {e}")

        # Show admin action first
        embed = discord.Embed(
            title="⚙️ Admin đã đặt kết quả!",
            description=f"**Admin {ctx.author.mention}** đã đặt kết quả game là **{result.upper()}**",
            color=0xffa500
        )
        embed.set_footer(text="Game sẽ kết thúc ngay lập tức...")
        await ctx.send(embed=embed)

        # Process the game ending with the set result
        winners = []
        losers = []
        total_winners = 0
        total_losers = 0
        total_winnings = 0

        for bet in game_data['bets']:
            if bet['side'] == result:
                winners.append(bet)
                total_winners += 1
                total_winnings += bet['amount']
            else:
                losers.append(bet)
                total_losers += 1

        # Distribute winnings (2x payout)
        for bet in winners:
            user_id = bet['user_id']
            winnings = bet['amount'] * 2  # 2x payout for winning bets
            bot._update_user_cash(guild_id, user_id, winnings)

        # Create result embed
        result_embed = discord.Embed(
            title="🎲 Kết quả game Tài Xỉu!",
            description=f"**Kết quả:** {result.upper()} {'🔺' if result == 'tai' else '🔻'}\n\n*Kết quả được đặt bởi Admin*",
            color=0x00ff88 if result == 'tai' else 0xff6b6b
        )

        result_embed.add_field(
            name="🏆 Người thắng",
            value=f"**{total_winners}** người thắng\n💰 Tổng thưởng: **{total_winnings * 2:,} cash**",
            inline=True
        )

        result_embed.add_field(
            name="💸 Người thua",
            value=f"**{total_losers}** người thua\n💔 Mất: **{sum(bet['amount'] for bet in losers):,} cash**",
            inline=True
        )

        result_embed.add_field(
            name="💡 Lưu ý",
            value="Người thắng nhận lại 2x số tiền đã cược!\nDùng `?tx` để bắt đầu game mới.",
            inline=False
        )

        await ctx.send(embed=result_embed)

        # Clean up the game
        if guild_id in bot.overunder_games and game_id in bot.overunder_games[guild_id]:
            del bot.overunder_games[guild_id][game_id]
            if not bot.overunder_games[guild_id]:
                del bot.overunder_games[guild_id]

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

    # Start bot with automatic restart capability
    try:
        await bot.start(token)
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise  # Re-raise to be caught by the restart wrapper

async def start_bot_with_auto_restart():
    """Main bot execution with auto-restart capability"""
    restart_count = 0
    max_restarts = 10

    while restart_count < max_restarts:
        try:
            logger.info(f"Starting bot system (attempt {restart_count + 1}/{max_restarts})")
            await main()
            break  # If main() completes normally, exit
        except KeyboardInterrupt:
            logger.info("Bot shutdown requested by user")
            break
        except Exception as e:
            restart_count += 1
            logger.error(f"Bot system crashed (attempt {restart_count}): {e}")

            if restart_count < max_restarts:
                logger.info(f"Restarting bot system in 5 seconds... ({restart_count}/{max_restarts})")
                await asyncio.sleep(5)
            else:
                logger.error("Maximum restart attempts reached. Bot will not restart automatically.")
                break

if __name__ == "__main__":
    asyncio.run(start_bot_with_auto_restart())