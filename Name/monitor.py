import asyncio
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import Dict, List, Optional, Union
import discord

logger = logging.getLogger(__name__)

class BotMonitor:
    """Real-time monitoring system for Discord bot activity and statistics"""
    
    def __init__(self, bot):
        self.bot = bot
        self.stats = {
            'detections': defaultdict(int),
            'actions': defaultdict(int),
            'guilds': defaultdict(lambda: {
                'members_joined': 0,
                'members_left': 0,
                'spam_detected': 0,
                'bots_detected': 0,
                'raids_detected': 0,
                'verifications_completed': 0,
                'verifications_failed': 0
            }),
            'hourly_stats': defaultdict(lambda: defaultdict(int)),
            'daily_stats': defaultdict(lambda: defaultdict(int))
        }
        
        # Recent activity tracking (last 24 hours)
        self.recent_activity = deque(maxlen=1000)
        
        # Performance metrics
        self.response_times = deque(maxlen=100)
        self.api_calls = defaultdict(int)
        
        # Start monitoring tasks
        self.monitoring_task = None
        
    def start_monitoring(self):
        """Start the monitoring background tasks"""
        if self.monitoring_task is None or self.monitoring_task.done():
            self.monitoring_task = asyncio.create_task(self._monitoring_loop())
            logger.info("Bot monitoring started")
    
    def stop_monitoring(self):
        """Stop the monitoring background tasks"""
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
            logger.info("Bot monitoring stopped")
    
    async def _monitoring_loop(self):
        """Main monitoring loop that runs every minute"""
        while True:
            try:
                await self._collect_system_stats()
                await self._cleanup_old_data()
                await self._check_bot_health()
                await asyncio.sleep(60)  # Check every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(60)
    
    async def _collect_system_stats(self):
        """Collect system-wide statistics"""
        now = datetime.utcnow()
        hour_key = now.strftime('%Y-%m-%d-%H')
        day_key = now.strftime('%Y-%m-%d')
        
        # Update hourly and daily counters
        total_members = sum(guild.member_count for guild in self.bot.guilds if guild.member_count)
        self.stats['hourly_stats'][hour_key]['total_members'] = total_members
        self.stats['daily_stats'][day_key]['total_members'] = total_members
        
        # Bot performance metrics
        latency_ms = round(self.bot.latency * 1000, 2)
        self.stats['hourly_stats'][hour_key]['bot_latency'] = latency_ms
        
        # Guild count
        guild_count = len(self.bot.guilds)
        self.stats['hourly_stats'][hour_key]['guild_count'] = guild_count
        self.stats['daily_stats'][day_key]['guild_count'] = guild_count
    
    async def _check_bot_health(self):
        """Check bot health and restart if necessary"""
        try:
            # Check if bot is still connected to Discord
            if not self.bot.is_ready():
                logger.warning("Bot is not ready - potential connection issue")
                await self._record_error("Bot not ready")
                return
            
            # Check latency - if too high, might indicate connection issues
            latency_ms = self.bot.latency * 1000
            if latency_ms > 5000:  # 5 seconds is very high
                logger.warning(f"Very high latency detected: {latency_ms:.0f}ms")
                await self._record_error(f"High latency: {latency_ms:.0f}ms")
            
            # Check if bot lost connection to guilds
            if len(self.bot.guilds) == 0:
                logger.warning("Bot is connected but in 0 guilds - potential issue")
                await self._record_error("Bot in 0 guilds")
            
            # Record health check completion
            now = datetime.utcnow()
            hour_key = now.strftime('%Y-%m-%d-%H')
            self.stats['hourly_stats'][hour_key]['health_checks'] = self.stats['hourly_stats'][hour_key].get('health_checks', 0) + 1
            
        except Exception as e:
            logger.error(f"Error during health check: {e}")
            await self._record_error(f"Health check error: {str(e)}")
    
    async def _record_error(self, error_message: str):
        """Record error and add to recent activity for tracking"""
        now = datetime.utcnow()
        
        # Add to recent activity
        activity = {
            'timestamp': now.isoformat(),
            'type': 'error',
            'subtype': 'health_check',
            'guild_id': 'system',
            'details': {'error': error_message}
        }
        self.recent_activity.append(activity)
        
        # Update error counters
        hour_key = now.strftime('%Y-%m-%d-%H')
        self.stats['hourly_stats'][hour_key]['errors'] = self.stats['hourly_stats'][hour_key].get('errors', 0) + 1
    
    async def _cleanup_old_data(self):
        """Clean up old statistical data to prevent memory bloat"""
        cutoff = datetime.utcnow() - timedelta(days=7)
        cutoff_hour = cutoff.strftime('%Y-%m-%d-%H')
        cutoff_day = cutoff.strftime('%Y-%m-%d')
        
        # Remove old hourly stats (keep 7 days)
        old_hours = [k for k in self.stats['hourly_stats'].keys() if k < cutoff_hour]
        for hour in old_hours[:50]:  # Remove in batches to avoid blocking
            del self.stats['hourly_stats'][hour]
        
        # Remove old daily stats (keep 30 days)
        cutoff_30_days = datetime.utcnow() - timedelta(days=30)
        cutoff_30_day = cutoff_30_days.strftime('%Y-%m-%d')
        old_days = [k for k in self.stats['daily_stats'].keys() if k < cutoff_30_day]
        for day in old_days[:10]:
            del self.stats['daily_stats'][day]
    
    def record_detection(self, detection_type: str, guild_id: str, details: Optional[Dict] = None):
        """Record a detection event"""
        now = datetime.utcnow()
        
        # Update counters
        self.stats['detections'][detection_type] += 1
        
        # Update guild-specific stats
        if detection_type == 'bot':
            self.stats['guilds'][guild_id]['bots_detected'] += 1
        elif detection_type == 'spam':
            self.stats['guilds'][guild_id]['spam_detected'] += 1
        elif detection_type == 'raid':
            self.stats['guilds'][guild_id]['raids_detected'] += 1
        
        # Add to recent activity
        activity = {
            'timestamp': now.isoformat(),
            'type': 'detection',
            'subtype': detection_type,
            'guild_id': guild_id,
            'details': details or {}
        }
        self.recent_activity.append(activity)
        
        # Update hourly/daily stats
        hour_key = now.strftime('%Y-%m-%d-%H')
        day_key = now.strftime('%Y-%m-%d')
        self.stats['hourly_stats'][hour_key][f'{detection_type}_detected'] += 1
        self.stats['daily_stats'][day_key][f'{detection_type}_detected'] += 1
        
        logger.info(f"Detection recorded: {detection_type} in guild {guild_id}")
    
    def record_action(self, action_type: str, guild_id: str, target_user: str, reason: Optional[str] = None):
        """Record a moderation action"""
        now = datetime.utcnow()
        
        # Update counters
        self.stats['actions'][action_type] += 1
        
        # Add to recent activity
        activity = {
            'timestamp': now.isoformat(),
            'type': 'action',
            'subtype': action_type,
            'guild_id': guild_id,
            'target': target_user,
            'reason': reason
        }
        self.recent_activity.append(activity)
        
        # Update hourly/daily stats
        hour_key = now.strftime('%Y-%m-%d-%H')
        day_key = now.strftime('%Y-%m-%d')
        self.stats['hourly_stats'][hour_key][f'{action_type}_actions'] += 1
        self.stats['daily_stats'][day_key][f'{action_type}_actions'] += 1
        
        logger.info(f"Action recorded: {action_type} for {target_user} in guild {guild_id}")
    
    def record_member_event(self, event_type: str, guild_id: str, member_id: str):
        """Record member join/leave events"""
        now = datetime.utcnow()
        
        # Update guild stats
        if event_type == 'join':
            self.stats['guilds'][guild_id]['members_joined'] += 1
        elif event_type == 'leave':
            self.stats['guilds'][guild_id]['members_left'] += 1
        
        # Add to recent activity
        activity = {
            'timestamp': now.isoformat(),
            'type': 'member_event',
            'subtype': event_type,
            'guild_id': guild_id,
            'member_id': member_id
        }
        self.recent_activity.append(activity)
        
        # Update hourly/daily stats
        hour_key = now.strftime('%Y-%m-%d-%H')
        day_key = now.strftime('%Y-%m-%d')
        self.stats['hourly_stats'][hour_key][f'members_{event_type}'] += 1
        self.stats['daily_stats'][day_key][f'members_{event_type}'] += 1
    
    def record_verification(self, guild_id: str, success: bool, member_id: str):
        """Record verification attempt results"""
        now = datetime.utcnow()
        
        # Update guild stats
        if success:
            self.stats['guilds'][guild_id]['verifications_completed'] += 1
        else:
            self.stats['guilds'][guild_id]['verifications_failed'] += 1
        
        # Add to recent activity
        activity = {
            'timestamp': now.isoformat(),
            'type': 'verification',
            'subtype': 'success' if success else 'failure',
            'guild_id': guild_id,
            'member_id': member_id
        }
        self.recent_activity.append(activity)
        
        # Update hourly/daily stats
        hour_key = now.strftime('%Y-%m-%d-%H')
        day_key = now.strftime('%Y-%m-%d')
        status = 'completed' if success else 'failed'
        self.stats['hourly_stats'][hour_key][f'verifications_{status}'] += 1
        self.stats['daily_stats'][day_key][f'verifications_{status}'] += 1
    
    def record_response_time(self, operation: str, duration_ms: float):
        """Record API response times for performance monitoring"""
        self.response_times.append({
            'operation': operation,
            'duration_ms': duration_ms,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        # Track API call counts
        self.api_calls[operation] += 1
    
    def get_guild_stats(self, guild_id: str) -> Dict:
        """Get statistics for a specific guild"""
        return dict(self.stats['guilds'][guild_id])
    
    def get_global_stats(self) -> Dict:
        """Get global statistics across all guilds"""
        return {
            'total_detections': dict(self.stats['detections']),
            'total_actions': dict(self.stats['actions']),
            'guild_count': len(self.bot.guilds),
            'total_members': sum(guild.member_count for guild in self.bot.guilds if guild.member_count),
            'bot_latency_ms': round(self.bot.latency * 1000, 2),
            'uptime_hours': self._get_uptime_hours(),
            'api_calls': dict(self.api_calls)
        }
    
    def get_recent_activity(self, limit: int = 50, activity_type: Optional[str] = None) -> List[Dict]:
        """Get recent activity events"""
        activities = list(self.recent_activity)
        
        if activity_type:
            activities = [a for a in activities if a.get('type') == activity_type]
        
        # Sort by timestamp (most recent first)
        activities.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return activities[:limit]
    
    def get_hourly_trends(self, hours: int = 24) -> Dict:
        """Get hourly trend data for the last N hours"""
        now = datetime.utcnow()
        trends = {}
        
        for i in range(hours):
            hour_time = now - timedelta(hours=i)
            hour_key = hour_time.strftime('%Y-%m-%d-%H')
            hour_data = self.stats['hourly_stats'].get(hour_key, {})
            trends[hour_key] = {
                'bots_detected': hour_data.get('bot_detected', 0),
                'spam_detected': hour_data.get('spam_detected', 0),
                'raids_detected': hour_data.get('raid_detected', 0),
                'verifications_completed': hour_data.get('verifications_completed', 0),
                'members_joined': hour_data.get('members_join', 0),
                'bot_latency': hour_data.get('bot_latency', 0)
            }
        
        return trends
    
    def get_performance_metrics(self) -> Dict:
        """Get bot performance metrics"""
        if not self.response_times:
            return {'average_response_time': 0, 'max_response_time': 0, 'min_response_time': 0}
        
        times = [rt['duration_ms'] for rt in self.response_times]
        return {
            'average_response_time': round(sum(times) / len(times), 2),
            'max_response_time': max(times),
            'min_response_time': min(times),
            'total_api_calls': sum(self.api_calls.values())
        }
    
    def _get_uptime_hours(self) -> float:
        """Calculate bot uptime in hours"""
        # This is a simplified calculation - in a real implementation,
        # you'd store the start time when the bot starts
        return 24.0  # Placeholder
    
    def export_stats(self, filepath: Optional[str] = None) -> Optional[str]:
        """Export all statistics to JSON file"""
        if filepath is None:
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filepath = f"logs/bot_stats_{timestamp}.json"
        
        export_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'global_stats': self.get_global_stats(),
            'guild_stats': dict(self.stats['guilds']),
            'recent_activity': list(self.recent_activity),
            'performance_metrics': self.get_performance_metrics(),
            'hourly_trends': self.get_hourly_trends(24)
        }
        
        try:
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            logger.info(f"Statistics exported to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to export statistics: {e}")
            return None
    
    async def generate_stats_embed(self, guild_id: Optional[str] = None) -> discord.Embed:
        """Generate a Discord embed with statistics"""
        if guild_id:
            # Guild-specific stats
            guild_stats = self.get_guild_stats(guild_id)
            guild = self.bot.get_guild(int(guild_id))
            guild_name = guild.name if guild else f"Guild {guild_id}"
            
            embed = discord.Embed(
                title=f"📊 {guild_name} Statistics",
                description="🛡️ **Server Protection Summary**",
                color=0x5865f2,
                timestamp=datetime.utcnow()
            )
            
            embed.add_field(
                name="👥 Member Activity",
                value=f"➕ Joined: {guild_stats['members_joined']}\n➖ Left: {guild_stats['members_left']}",
                inline=True
            )
            embed.add_field(
                name="🤖 Detections",
                value=f"🤖 Bots: {guild_stats['bots_detected']}\n🚫 Spam: {guild_stats['spam_detected']}\n⚡ Raids: {guild_stats['raids_detected']}",
                inline=True
            )
            embed.add_field(
                name="🔐 Verifications",
                value=f"✅ Passed: {guild_stats['verifications_completed']}\n❌ Failed: {guild_stats['verifications_failed']}",
                inline=True
            )
            
        else:
            # Global stats
            global_stats = self.get_global_stats()
            
            embed = discord.Embed(
                title="📊 Global Bot Statistics",
                description="🌐 **Anti-Bot System Overview**",
                color=0x5865f2,
                timestamp=datetime.utcnow()
            )
            
            embed.add_field(
                name="🏛️ Server Coverage",
                value=f"📍 Guilds: {global_stats['guild_count']}\n👥 Total Members: {global_stats['total_members']:,}",
                inline=True
            )
            embed.add_field(
                name="🛡️ Total Detections",
                value=f"🤖 Bots: {global_stats['total_detections'].get('bot', 0)}\n🚫 Spam: {global_stats['total_detections'].get('spam', 0)}\n⚡ Raids: {global_stats['total_detections'].get('raid', 0)}",
                inline=True
            )
            embed.add_field(
                name="⚡ Performance",
                value=f"🏓 Latency: {global_stats['bot_latency_ms']}ms\n⏰ Uptime: {global_stats['uptime_hours']:.1f}h",
                inline=True
            )
        
        embed.set_footer(text="AntiBot Monitoring System • Updated every minute")
        return embed
    
    async def get_system_health(self) -> Dict:
        """Get system health status"""
        health = {
            'status': 'healthy',
            'issues': [],
            'warnings': []
        }
        
        # Check bot latency
        latency_ms = self.bot.latency * 1000
        if latency_ms > 1000:
            health['issues'].append(f"High latency: {latency_ms:.0f}ms")
            health['status'] = 'degraded'
        elif latency_ms > 500:
            health['warnings'].append(f"Elevated latency: {latency_ms:.0f}ms")
        
        # Check if bot is in too many guilds
        guild_count = len(self.bot.guilds)
        if guild_count > 100:
            health['warnings'].append(f"High guild count: {guild_count}")
        
        # Check recent error rate
        recent_errors = len([a for a in self.recent_activity 
                           if a.get('type') == 'error' and 
                           datetime.fromisoformat(a['timestamp']) > datetime.utcnow() - timedelta(hours=1)])
        
        if recent_errors > 10:
            health['issues'].append(f"High error rate: {recent_errors} errors in last hour")
            health['status'] = 'degraded'
        elif recent_errors > 5:
            health['warnings'].append(f"Elevated error rate: {recent_errors} errors in last hour")
        
        return health
    
    def get_top_active_guilds(self, limit: int = 10) -> List[Dict]:
        """Get the most active guilds by detection count"""
        guild_activity = []
        
        for guild_id, stats in self.stats['guilds'].items():
            guild = self.bot.get_guild(int(guild_id))
            total_activity = (
                stats['bots_detected'] + 
                stats['spam_detected'] + 
                stats['raids_detected'] +
                stats['members_joined']
            )
            
            guild_activity.append({
                'guild_id': guild_id,
                'guild_name': guild.name if guild else f"Unknown Guild {guild_id}",
                'total_activity': total_activity,
                'bots_detected': stats['bots_detected'],
                'spam_detected': stats['spam_detected'],
                'raids_detected': stats['raids_detected'],
                'members_joined': stats['members_joined']
            })
        
        # Sort by total activity
        guild_activity.sort(key=lambda x: x['total_activity'], reverse=True)
        return guild_activity[:limit]