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
        
        # Initialize OpenAI for translation
        # the newest OpenAI model is "gpt-5" which was released August 7, 2025.
        # do not change this unless explicitly requested by the user
        self.openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        
        # Track member joins for raid detection
        self.recent_joins = {}
        
        # Track pending verifications
        self.pending_verifications = {}
        
        # Game system tracking
        self.active_games = {}
        self.leaderboard = {}
        
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
        
        # Get user's answer and translate to English for comparison
        user_answer = message.content.strip()
        user_answer_english = await self.translate_to_english(user_answer)
        correct_answer = current_question['answer'].lower()
        
        # Check if answer matches (flexible matching)
        is_correct = False
        if (correct_answer == user_answer_english or 
            correct_answer in user_answer_english or 
            user_answer_english in correct_answer or
            correct_answer == user_answer.lower() or
            correct_answer in user_answer.lower() or
            user_answer.lower() in correct_answer):
            is_correct = True
        
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
                value=f"**{current_question['vietnamese_answer']}**",
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
                        # If no questions available, generate one immediately
                        logger.info("No questions available, generating one immediately")
                        import random
                        # Quick generation of Vietnam-focused math question in Vietnamese
                        vietnam_math_questions = [
                            ("Nếu Hà Nội có 8 triệu dân và TP.HCM có 9 triệu dân, tổng là bao nhiêu?", "17 triệu", "17 triệu"),
                            ("Việt Nam có 63 tỉnh thành. Nếu 5 là thành phố trực thuộc TW, còn lại bao nhiêu tỉnh?", "58", "58"),
                            ("Nếu tô phở giá 50.000 VNĐ và mua 3 tô, tổng tiền là bao nhiêu?", "150000", "150.000 VNĐ"),
                            ("Nếu bánh mì 25.000 VNĐ và cà phê 15.000 VNĐ, tổng cộng là bao nhiêu?", "40000", "40.000 VNĐ")
                        ]
                        question_data = random.choice(vietnam_math_questions)
                        current_question = {
                            "question": question_data[0],
                            "answer": question_data[1].lower(),
                            "vietnamese_answer": question_data[2]
                        }
                    else:
                        # If no questions available, prioritize Vietnamese questions from the database
                        import random
                        vietnam_questions = {
                            "geography": [
                                ("Núi cao nhất Việt Nam là gì?", "fansipan", "Fansipan"),
                                ("Sông nào dài nhất ở Việt Nam?", "mekong", "Sông Mê Kông"),
                                ("Đảo lớn nhất của Việt Nam là đảo nào?", "phu quoc", "Phú Quốc")
                            ],
                            "history": [
                                ("Việt Nam thống nhất vào năm nào?", "1975", "1975"),
                                ("Tổng thống đầu tiên của Việt Nam là ai?", "ho chi minh", "Hồ Chí Minh"),
                                ("Trận Điện Biên Phủ diễn ra vào năm nào?", "1954", "1954")
                            ]
                        }
                        category = random.choice(list(vietnam_questions.keys()))
                        question_data = random.choice(vietnam_questions[category])
                        question, answer, vietnamese_answer = question_data
                        current_question = {
                            "question": question,
                            "answer": answer.lower(),
                            "vietnamese_answer": vietnamese_answer
                        }
                        logger.info(f"Using backup Vietnamese question: {current_question['question']}")
                
                # Track that this question was shown and remove from original pool
                game['shown_questions'].add(current_question['question'])
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
        """Generate new Vietnam-focused questions every 30 seconds"""
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
                await asyncio.sleep(30)  # Generate new question every 30 seconds
                
                if guild_id not in self.active_games or not self.active_games[guild_id]['running']:
                    break
                
                game = self.active_games[guild_id]
                
                # Choose random category and question
                category = random.choice(list(vietnam_questions.keys()))
                question_data = random.choice(vietnam_questions[category])
                question, answer, vietnamese_answer = question_data
                
                # Add to new questions pool
                new_question = {"question": question, "answer": answer.lower(), "vietnamese_answer": vietnamese_answer}
                game['new_questions'].append(new_question)
                game['last_generation_time'] = datetime.utcnow()
                
                logger.info(f"Generated new QNA question ({category}): {question}")
                
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
                title="🎮 QNA đã đang hoạt động",
                description="Một trò chơi QNA đã đang chạy trong máy chủ này!\n\nSử dụng `?stop` để kết thúc.",
                color=0xffa500
            )
            await ctx.send(embed=embed)
            return
        
        # Start new QNA game with Vietnam-focused questions in Vietnamese
        questions = [
            {"question": "Thủ đô của Việt Nam là gì?", "answer": "hanoi", "vietnamese_answer": "Hà Nội"},
            {"question": "Thành phố lớn nhất Việt Nam là gì?", "answer": "ho chi minh city", "vietnamese_answer": "TP. Hồ Chí Minh"},
            {"question": "Quốc hoa của Việt Nam là gì?", "answer": "lotus", "vietnamese_answer": "Hoa sen"},
            {"question": "Việt Nam giành độc lập vào năm nào?", "answer": "1945", "vietnamese_answer": "1945"},
            {"question": "Đồng tiền của Việt Nam là gì?", "answer": "dong", "vietnamese_answer": "Đồng Việt Nam"}
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
            'shown_questions': {current_question['question']},
            'new_questions': []
        }
        
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
