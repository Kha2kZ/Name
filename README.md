# Discord Anti-Bot Moderation Tool

A comprehensive Discord bot designed to detect and prevent malicious bots, spam, and raids in your Discord server using advanced heuristics and configurable detection systems.

## 🌟 Features

### 🤖 Advanced Bot Detection
- **Account Age Analysis**: Automatically flags accounts that are suspiciously new
- **Profile Pattern Recognition**: Analyzes avatars, usernames, and display names for bot-like characteristics
- **Behavioral Analysis**: Monitors join patterns, message timing, and activity behaviors
- **Configurable Thresholds**: Customize detection sensitivity for different server needs

### 🛡️ Comprehensive Spam Protection
- **Intelligent Rate Limiting**: Prevents message flooding with configurable limits
- **Duplicate Content Detection**: Identifies and stops repeated message spam
- **Mention Spam Protection**: Limits excessive @mentions and @everyone abuse
- **Link & URL Analysis**: Detects suspicious links, URL shorteners, and known scam domains
- **Content Pattern Matching**: Recognizes common spam phrases and promotional content

### ⚡ Raid Protection System
- **Mass Join Detection**: Identifies coordinated bot attacks and mass joins
- **Automatic Server Lockdown**: Temporarily enables verification during detected raids
- **Configurable Response Actions**: Choose between alerts, lockdowns, or custom responses
- **Real-time Monitoring**: Continuous analysis of join patterns and member behavior

### 🔧 Advanced Moderation Tools
- **Multiple Action Types**: Kick, ban, timeout, quarantine, and custom actions
- **Automated Response System**: Configurable automatic actions based on threat levels
- **Smart Quarantine System**: Isolate suspicious users while maintaining server security
- **Escalation Management**: Progressive responses for repeat offenders

### ⚙️ Flexible Configuration System
- **Per-Server Customization**: Independent settings for each Discord server
- **Admin-Friendly Commands**: Easy configuration via Discord slash commands
- **Whitelist Management**: Exempt trusted users, roles, and verified members
- **Action Customization**: Fine-tune responses for different types of threats
- **Real-time Configuration**: Update settings without restarting the bot

### 📊 Comprehensive Logging & Analytics
- **Detailed Action Logs**: Track all moderation actions with timestamps and reasons
- **Event Monitoring**: Log member joins, leaves, and suspicious activities
- **Discord Channel Integration**: Send logs to designated channels with rich embeds
- **File-based Backup**: Persistent logging with file storage and export options
- **Analytics Dashboard**: View detection statistics and server health metrics

## 🚀 Quick Start

### Prerequisites
- Python 3.9 or higher
- Discord bot token with appropriate permissions
- discord.py library (automatically installed)

### Installation

1. **Clone or download the bot files**
   ```bash
   # If using git
   git clone <repository-url>
   cd discord-antibot
   
   # Or download and extract the files
   ```

2. **Set up your Discord bot token**
   ```bash
   # On Linux/Mac
   export DISCORD_BOT_TOKEN="your_bot_token_here"
   
   # On Windows (Command Prompt)
   set DISCORD_BOT_TOKEN=your_bot_token_here
   
   # On Windows (PowerShell)
   $env:DISCORD_BOT_TOKEN="your_bot_token_here"
   ```

3. **Run the bot**
   ```bash
   python main.py
   ```

### Required Bot Permissions

Your Discord bot needs the following permissions:
- Read Messages/View Channels
- Send Messages
- Manage Messages (for spam deletion)
- Kick Members
- Ban Members
- Moderate Members (for timeouts)
- Manage Roles (for quarantine system)
- View Audit Log
- Read Message History

**Permission Integer**: `1374389469270` (for easy setup)

## 📋 Commands

### Configuration Commands (Admin Only)

