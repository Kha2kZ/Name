from flask import Flask, render_template, jsonify, request, redirect, url_for, flash
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import asyncio
import logging

# Web dashboard for Discord Anti-Bot monitoring and management
app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')

logger = logging.getLogger(__name__)

class WebDashboard:
    """Web interface for Discord bot management and monitoring"""
    
    def __init__(self, bot_instance=None):
        self.bot = bot_instance
        self.monitor = None
        if bot_instance and hasattr(bot_instance, 'monitor'):
            self.monitor = bot_instance.monitor
    
    def get_config_files(self) -> List[str]:
        """Get list of available guild configuration files"""
        config_dir = 'configs'
        if not os.path.exists(config_dir):
            return []
        
        config_files = []
        for filename in os.listdir(config_dir):
            if filename.endswith('.json'):
                config_files.append(filename.replace('.json', ''))
        return config_files
    
    def load_guild_config(self, guild_id: str) -> Dict:
        """Load configuration for a specific guild"""
        config_path = f'configs/{guild_id}.json'
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load config for guild {guild_id}: {e}")
        
        # Return default config if file doesn't exist
        with open('config/default_config.json', 'r') as f:
            return json.load(f)
    
    def save_guild_config(self, guild_id: str, config: Dict) -> bool:
        """Save configuration for a specific guild"""
        try:
            os.makedirs('configs', exist_ok=True)
            config_path = f'configs/{guild_id}.json'
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=4)
            return True
        except Exception as e:
            logger.error(f"Failed to save config for guild {guild_id}: {e}")
            return False
    
    def get_log_files(self) -> List[Dict]:
        """Get list of available log files with metadata"""
        logs_dir = 'logs'
        if not os.path.exists(logs_dir):
            return []
        
        log_files = []
        for filename in os.listdir(logs_dir):
            if filename.endswith('.log'):
                filepath = os.path.join(logs_dir, filename)
                stat = os.stat(filepath)
                log_files.append({
                    'filename': filename,
                    'size': stat.st_size,
                    'modified': datetime.fromtimestamp(stat.st_mtime),
                    'path': filepath
                })
        
        # Sort by modification time (newest first)
        log_files.sort(key=lambda x: x['modified'], reverse=True)
        return log_files

# Initialize dashboard
dashboard = WebDashboard()

@app.route('/')
def index():
    """Main dashboard page"""
    try:
        # Get basic stats
        guild_configs = dashboard.get_config_files()
        log_files = dashboard.get_log_files()
        
        # Get global stats if monitor is available
        global_stats = {}
        recent_activity = []
        if dashboard.monitor:
            global_stats = dashboard.monitor.get_global_stats()
            recent_activity = dashboard.monitor.get_recent_activity(20)
        
        return render_template('dashboard.html',
                             guild_count=len(guild_configs),
                             log_count=len(log_files),
                             global_stats=global_stats,
                             recent_activity=recent_activity)
    except Exception as e:
        logger.error(f"Error in dashboard route: {e}")
        return f"Dashboard Error: {e}", 500

@app.route('/guilds')
def guilds():
    """Guild management page"""
    try:
        guild_configs = dashboard.get_config_files()
        guild_data = []
        
        for guild_id in guild_configs:
            config = dashboard.load_guild_config(guild_id)
            guild_stats = {}
            if dashboard.monitor:
                guild_stats = dashboard.monitor.get_guild_stats(guild_id)
            
            guild_info = {
                'id': guild_id,
                'name': f"Guild {guild_id}",  # Would get real name from bot
                'enabled': config.get('enabled', False),
                'stats': guild_stats,
                'config': config
            }
            guild_data.append(guild_info)
        
        return render_template('guilds.html', guilds=guild_data)
    except Exception as e:
        logger.error(f"Error in guilds route: {e}")
        return f"Guilds Error: {e}", 500

@app.route('/guild/<guild_id>')
def guild_detail(guild_id: str):
    """Detailed view for a specific guild"""
    try:
        config = dashboard.load_guild_config(guild_id)
        guild_stats = {}
        recent_activity = []
        
        if dashboard.monitor:
            guild_stats = dashboard.monitor.get_guild_stats(guild_id)
            # Filter activity for this guild
            all_activity = dashboard.monitor.get_recent_activity(100)
            recent_activity = [a for a in all_activity if a.get('guild_id') == guild_id][:20]
        
        return render_template('guild_detail.html',
                             guild_id=guild_id,
                             config=config,
                             stats=guild_stats,
                             recent_activity=recent_activity)
    except Exception as e:
        logger.error(f"Error in guild detail route: {e}")
        return f"Guild Detail Error: {e}", 500

@app.route('/guild/<guild_id>/config', methods=['GET', 'POST'])
def guild_config(guild_id: str):
    """Guild configuration management"""
    if request.method == 'POST':
        try:
            # Update configuration from form data
            config = dashboard.load_guild_config(guild_id)
            
            # Update basic settings
            config['enabled'] = request.form.get('enabled') == 'on'
            
            # Bot detection settings
            config['bot_detection']['enabled'] = request.form.get('bot_detection_enabled') == 'on'
            config['bot_detection']['action'] = request.form.get('bot_detection_action', 'quarantine')
            config['bot_detection']['min_account_age_days'] = int(request.form.get('min_account_age', 7))
            
            # Spam detection settings
            config['spam_detection']['enabled'] = request.form.get('spam_detection_enabled') == 'on'
            config['spam_detection']['action'] = request.form.get('spam_detection_action', 'timeout')
            config['spam_detection']['max_messages_per_window'] = int(request.form.get('max_messages', 5))
            
            # Raid protection settings
            config['raid_protection']['enabled'] = request.form.get('raid_protection_enabled') == 'on'
            config['raid_protection']['max_joins'] = int(request.form.get('max_joins', 10))
            config['raid_protection']['time_window'] = int(request.form.get('time_window', 60))
            
            # Verification settings
            config['verification']['enabled'] = request.form.get('verification_enabled') == 'on'
            
            # Logging settings
            config['logging']['enabled'] = request.form.get('logging_enabled') == 'on'
            config['logging']['channel_id'] = request.form.get('log_channel_id', '')
            
            # Save configuration
            if dashboard.save_guild_config(guild_id, config):
                flash('Configuration updated successfully!', 'success')
            else:
                flash('Failed to save configuration.', 'error')
            
            return redirect(url_for('guild_config', guild_id=guild_id))
            
        except Exception as e:
            logger.error(f"Error saving guild config: {e}")
            flash(f'Error saving configuration: {e}', 'error')
    
    # GET request - show config form
    try:
        config = dashboard.load_guild_config(guild_id)
        return render_template('guild_config.html', 
                             guild_id=guild_id, 
                             config=config)
    except Exception as e:
        logger.error(f"Error loading guild config: {e}")
        return f"Config Error: {e}", 500

@app.route('/api/stats')
def api_stats():
    """API endpoint for real-time statistics"""
    try:
        if dashboard.monitor:
            global_stats = dashboard.monitor.get_global_stats()
            performance = dashboard.monitor.get_performance_metrics()
            return jsonify({
                'global_stats': global_stats,
                'performance': performance,
                'timestamp': datetime.utcnow().isoformat()
            })
        else:
            return jsonify({'error': 'Monitor not available'}), 503
    except Exception as e:
        logger.error(f"Error in stats API: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/guild/<guild_id>/stats')
def api_guild_stats(guild_id: str):
    """API endpoint for guild-specific statistics"""
    try:
        if dashboard.monitor:
            guild_stats = dashboard.monitor.get_guild_stats(guild_id)
            recent_activity = dashboard.monitor.get_recent_activity(50)
            guild_activity = [a for a in recent_activity if a.get('guild_id') == guild_id]
            
            return jsonify({
                'guild_id': guild_id,
                'stats': guild_stats,
                'recent_activity': guild_activity[:20],
                'timestamp': datetime.utcnow().isoformat()
            })
        else:
            return jsonify({'error': 'Monitor not available'}), 503
    except Exception as e:
        logger.error(f"Error in guild stats API: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/health')
def api_health():
    """API endpoint for system health check"""
    try:
        if dashboard.monitor:
            health = asyncio.run(dashboard.monitor.get_system_health())
            return jsonify(health)
        else:
            return jsonify({'status': 'unknown', 'issues': ['Monitor not available']})
    except Exception as e:
        logger.error(f"Error in health API: {e}")
        return jsonify({'status': 'error', 'issues': [str(e)]})

@app.route('/logs')
def logs():
    """Log file viewer"""
    try:
        log_files = dashboard.get_log_files()
        return render_template('logs.html', log_files=log_files)
    except Exception as e:
        logger.error(f"Error in logs route: {e}")
        return f"Logs Error: {e}", 500

@app.route('/logs/<filename>')
def view_log(filename: str):
    """View specific log file content"""
    try:
        # Security check - only allow .log files
        if not filename.endswith('.log'):
            return "Invalid file type", 400
        
        log_path = os.path.join('logs', filename)
        if not os.path.exists(log_path):
            return "Log file not found", 404
        
        # Read last 1000 lines of the log file
        lines = []
        with open(log_path, 'r') as f:
            lines = f.readlines()
        
        # Get last 1000 lines
        recent_lines = lines[-1000:] if len(lines) > 1000 else lines
        
        return render_template('log_view.html', 
                             filename=filename,
                             log_content=recent_lines,
                             total_lines=len(lines))
    except Exception as e:
        logger.error(f"Error viewing log {filename}: {e}")
        return f"Log View Error: {e}", 500

@app.route('/api/export/stats')
def api_export_stats():
    """Export statistics to JSON file"""
    try:
        if dashboard.monitor:
            filepath = dashboard.monitor.export_stats()
            if filepath:
                return jsonify({
                    'success': True,
                    'filepath': filepath,
                    'message': 'Statistics exported successfully'
                })
            else:
                return jsonify({
                    'success': False,
                    'message': 'Failed to export statistics'
                }), 500
        else:
            return jsonify({'error': 'Monitor not available'}), 503
    except Exception as e:
        logger.error(f"Error exporting stats: {e}")
        return jsonify({'error': str(e)}), 500

# Template functions for basic HTML if templates don't exist
def create_basic_templates():
    """Create basic HTML templates if they don't exist"""
    templates_dir = 'templates'
    os.makedirs(templates_dir, exist_ok=True)
    
    # Basic dashboard template
    dashboard_html = '''<!DOCTYPE html>
<html>
<head>
    <title>Discord Anti-Bot Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #2c2f33; color: #fff; }
        .card { background: #36393f; padding: 20px; margin: 10px 0; border-radius: 8px; }
        .stats { display: flex; flex-wrap: wrap; gap: 20px; }
        .stat-item { background: #7289da; padding: 15px; border-radius: 5px; text-align: center; min-width: 150px; }
        .stat-value { font-size: 24px; font-weight: bold; }
        .stat-label { font-size: 12px; opacity: 0.8; }
        .nav { background: #7289da; padding: 10px; margin: -20px -20px 20px -20px; border-radius: 8px 8px 0 0; }
        .nav a { color: white; text-decoration: none; margin-right: 20px; }
        .activity { max-height: 400px; overflow-y: auto; }
        .activity-item { padding: 8px; border-bottom: 1px solid #444; }
    </style>
</head>
<body>
    <div class="card">
        <div class="nav">
            <a href="/">Dashboard</a>
            <a href="/guilds">Guilds</a>
            <a href="/logs">Logs</a>
        </div>
        <h1>🛡️ Discord Anti-Bot Dashboard</h1>
        
        <div class="stats">
            <div class="stat-item">
                <div class="stat-value">{{ guild_count }}</div>
                <div class="stat-label">Active Guilds</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{{ log_count }}</div>
                <div class="stat-label">Log Files</div>
            </div>
            {% if global_stats %}
            <div class="stat-item">
                <div class="stat-value">{{ global_stats.get('total_members', 0) }}</div>
                <div class="stat-label">Total Members</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{{ global_stats.get('bot_latency_ms', 0) }}ms</div>
                <div class="stat-label">Bot Latency</div>
            </div>
            {% endif %}
        </div>
        
        {% if recent_activity %}
        <div class="card">
            <h3>📊 Recent Activity</h3>
            <div class="activity">
                {% for activity in recent_activity %}
                <div class="activity-item">
                    <strong>{{ activity.subtype.title() }}</strong> - 
                    {{ activity.timestamp[:19] }} - 
                    Guild: {{ activity.guild_id }}
                </div>
                {% endfor %}
            </div>
        </div>
        {% endif %}
    </div>
</body>
</html>'''
    
    with open(os.path.join(templates_dir, 'dashboard.html'), 'w') as f:
        f.write(dashboard_html)
    
    # Basic guilds template
    guilds_html = '''<!DOCTYPE html>
<html>
<head>
    <title>Guild Management - Discord Anti-Bot</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #2c2f33; color: #fff; }
        .card { background: #36393f; padding: 20px; margin: 10px 0; border-radius: 8px; }
        .guild-item { background: #40444b; padding: 15px; margin: 10px 0; border-radius: 5px; }
        .guild-stats { display: flex; gap: 15px; margin-top: 10px; }
        .stat { background: #7289da; padding: 8px; border-radius: 3px; font-size: 12px; }
        .nav { background: #7289da; padding: 10px; margin: -20px -20px 20px -20px; border-radius: 8px 8px 0 0; }
        .nav a { color: white; text-decoration: none; margin-right: 20px; }
        .enabled { color: #43b581; }
        .disabled { color: #f04747; }
        .btn { background: #7289da; color: white; padding: 8px 15px; text-decoration: none; border-radius: 3px; }
    </style>
</head>
<body>
    <div class="card">
        <div class="nav">
            <a href="/">Dashboard</a>
            <a href="/guilds">Guilds</a>
            <a href="/logs">Logs</a>
        </div>
        <h1>🏛️ Guild Management</h1>
        
        {% for guild in guilds %}
        <div class="guild-item">
            <h3>{{ guild.name }} (ID: {{ guild.id }})</h3>
            <p>Status: 
                {% if guild.enabled %}
                    <span class="enabled">🟢 Active</span>
                {% else %}
                    <span class="disabled">🔴 Disabled</span>
                {% endif %}
            </p>
            
            <div class="guild-stats">
                <div class="stat">🤖 Bots: {{ guild.stats.get('bots_detected', 0) }}</div>
                <div class="stat">🚫 Spam: {{ guild.stats.get('spam_detected', 0) }}</div>
                <div class="stat">⚡ Raids: {{ guild.stats.get('raids_detected', 0) }}</div>
                <div class="stat">✅ Verified: {{ guild.stats.get('verifications_completed', 0) }}</div>
            </div>
            
            <p style="margin-top: 15px;">
                <a href="/guild/{{ guild.id }}" class="btn">View Details</a>
                <a href="/guild/{{ guild.id }}/config" class="btn">Configure</a>
            </p>
        </div>
        {% endfor %}
    </div>
</body>
</html>'''
    
    with open(os.path.join(templates_dir, 'guilds.html'), 'w') as f:
        f.write(guilds_html)
    
    # Additional basic templates for other routes...
    logger.info("Basic HTML templates created")

def run_web_app(host='0.0.0.0', port=5000, debug=False):
    """Run the Flask web application"""
    try:
        # Create basic templates if they don't exist
        create_basic_templates()
        
        logger.info(f"Starting web dashboard on {host}:{port}")
        app.run(host=host, port=port, debug=debug, threaded=True)
    except Exception as e:
        logger.error(f"Failed to start web application: {e}")

def set_bot_instance(bot_instance):
    """Set the bot instance for the dashboard"""
    dashboard.bot = bot_instance
    if hasattr(bot_instance, 'monitor'):
        dashboard.monitor = bot_instance.monitor
    logger.info("Bot instance connected to web dashboard")

if __name__ == '__main__':
    # Run standalone web app (without bot integration)
    create_basic_templates()
    run_web_app(debug=True)