#!/usr/bin/env python3
"""
Launcher script to run Discord bot with web dashboard
This script starts both the bot and the Flask web interface in parallel
"""

import asyncio
import threading
import time
import logging
from web_app import run_web_app, set_bot_instance
from main import AntiSpamBot

logger = logging.getLogger(__name__)

def start_web_dashboard():
    """Start the web dashboard in a separate thread"""
    try:
        time.sleep(2)  # Give bot time to initialize
        logger.info("Starting web dashboard...")
        run_web_app(host='0.0.0.0', port=5000, debug=False)
    except Exception as e:
        logger.error(f"Web dashboard error: {e}")

async def start_bot_with_dashboard():
    """Start both bot and web dashboard"""
    try:
        # Create bot instance
        bot = AntiSpamBot()
        
        # Start web dashboard in background thread
        dashboard_thread = threading.Thread(target=start_web_dashboard, daemon=True)
        dashboard_thread.start()
        
        # Set bot instance for web dashboard
        set_bot_instance(bot)
        
        # Get bot token from environment
        import os
        token = os.environ.get('DISCORD_BOT_TOKEN')
        if not token:
            logger.error("DISCORD_BOT_TOKEN environment variable not set!")
            return
        
        # Start bot
        logger.info("Starting Discord bot...")
        await bot.start(token)
        
    except Exception as e:
        logger.error(f"Failed to start bot with dashboard: {e}")

if __name__ == "__main__":
    # Setup logging
    from logging_setup import setup_logging
    setup_logging()
    
    logger.info("=== Discord Anti-Bot with Web Dashboard ===")
    logger.info("Bot will be available on Discord")
    logger.info("Dashboard will be available at http://localhost:5000")
    
    try:
        asyncio.run(start_bot_with_dashboard())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Startup error: {e}")