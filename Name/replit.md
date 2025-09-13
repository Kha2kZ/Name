# Discord Anti-Bot Moderation Tool

## Overview

This is a comprehensive Discord bot designed to detect and prevent malicious bots, spam, and raids in Discord servers. The bot uses advanced heuristics and configurable detection systems to analyze member behavior, message patterns, and join activities. It provides automated moderation actions including kicks, bans, timeouts, and quarantine systems, with extensive logging and analytics capabilities.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Core Components
- **Bot Detection System**: Analyzes account age, profile patterns, and username characteristics to identify suspicious bot accounts
- **Spam Detection Engine**: Monitors message rates, duplicate content, mention abuse, and suspicious links using configurable thresholds
- **Raid Protection**: Detects mass join events and implements automatic server lockdown mechanisms
- **Moderation Tools**: Provides automated response systems with escalation management and quarantine capabilities
- **Currency System**: Advanced economy system with massive denominations and user-to-user transfers
- **Game Control**: Admin tools for manual game result control and player money management

### Configuration Management
- **Per-Guild Configuration**: Independent settings stored as JSON files for each Discord server
- **Default Configuration System**: Fallback configuration with hardcoded defaults when files are missing
- **Real-time Updates**: Configuration changes applied without bot restart
- **Whitelist System**: Exempts trusted users, roles, and verified members from detection

### Detection Algorithms
- **Heuristic Scoring**: Multiple detection criteria contribute to suspicious activity scores
- **Pattern Matching**: Regular expressions for username patterns, suspicious domains, and spam phrases
- **Rate Limiting**: Time-window based analysis for message frequency and join patterns
- **Behavioral Analysis**: Tracks user activity patterns and join timing

### Moderation System
- **Progressive Actions**: Escalating responses from warnings to quarantine to bans
- **Automatic Response**: Configurable thresholds trigger automated moderation actions
- **Manual Override**: Admin commands for custom actions and configuration management
- **DM Notifications**: Automated messages to users explaining moderation actions

### Logging Architecture
- **Dual Logging**: Both file-based persistence and Discord channel integration
- **Event Tracking**: Comprehensive logging of joins, leaves, detections, and moderation actions
- **Rich Embeds**: Formatted Discord messages with timestamps, reasons, and user information
- **Analytics**: Detection statistics and server health metrics

## External Dependencies

### Discord API Integration
- **discord.py**: Primary library for Discord bot functionality
- **Bot Permissions**: Requires kick, ban, manage roles, and message management permissions
- **Intents**: Uses member, message content, and guild intents for comprehensive monitoring

### Enhanced Currency and Gaming System
- **Extended Denominations**: Supports K (Thousand), M (Million), B (Billion), T (Trillion), Qa (Quadrillion), Qi (Quintillion), Sx (Sextillion)
- **User-to-User Transfers**: `?give` command allows players to transfer money with full denomination support
- **Admin Money Management**: `?clear` command for resetting player funds, `?moneyhack` for adding money
- **Manual Game Control**: `?win` command allows admins to manually set Tai/Xiu game results
- **All-In Betting**: Support for "all" keyword in betting and money transfers
- **Automatic Data Persistence**: File-based backup system saves player data every 5 seconds

### File System Storage
- **JSON Configuration**: Guild-specific settings stored in local JSON files
- **Log Files**: Daily log rotation with structured logging format
- **Config Directory**: Organized file structure for configurations and logs
- **Backup System**: `user_cash_backup.json` stores player money data with automatic saves every 5 seconds
- **Data Recovery**: Bot loads player data from backup file on startup with smart merge functionality, preserving existing user data while adding new users, ensuring no data loss on restarts

### Python Libraries
- **asyncio**: Handles concurrent Discord API operations and background tasks
- **collections**: Uses deque and defaultdict for efficient message tracking
- **datetime**: Time-based analysis for account age and rate limiting
- **re**: Regular expression pattern matching for content analysis
- **logging**: Structured logging with multiple output destinations

### Discord Bot Token
- **Authentication**: Requires valid Discord bot token for API access
- **Permissions**: Needs specific guild permissions for moderation actions
- **Rate Limiting**: Implements Discord API rate limit compliance