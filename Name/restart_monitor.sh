#!/bin/bash

# Discord Bot Auto-Restart Monitor
# This script monitors the bot process and restarts it when it crashes

LOG_FILE="logs/restart_monitor.log"
BOT_SCRIPT="main.py"
MAX_RESTARTS=50
RESTART_DELAY=5

# Create logs directory if it doesn't exist
mkdir -p logs

# Function to log with timestamp
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - MONITOR - $1" | tee -a "$LOG_FILE"
}

# Function to check if bot is running
is_bot_running() {
    pgrep -f "python.*main.py" > /dev/null
    return $?
}

# Function to start the bot
start_bot() {
    log_message "INFO - Starting Discord bot..."
    python main.py &
    BOT_PID=$!
    log_message "INFO - Bot started with PID: $BOT_PID"
}

# Function to kill existing bot processes
kill_existing_bots() {
    local pids=$(pgrep -f "python.*main.py")
    if [ -n "$pids" ]; then
        log_message "INFO - Killing existing bot processes: $pids"
        pkill -f "python.*main.py"
        sleep 2
    fi
}

# Main monitoring loop
main() {
    log_message "INFO - Discord Bot Monitor Started"
    log_message "INFO - Maximum restarts allowed: $MAX_RESTARTS"
    
    restart_count=0
    
    # Clean up any existing bot processes
    kill_existing_bots
    
    # Start the bot initially
    start_bot
    
    # Monitor loop
    while [ $restart_count -lt $MAX_RESTARTS ]; do
        sleep 10  # Check every 10 seconds
        
        if ! is_bot_running; then
            restart_count=$((restart_count + 1))
            log_message "WARNING - Bot process died! Restart attempt $restart_count/$MAX_RESTARTS"
            
            if [ $restart_count -lt $MAX_RESTARTS ]; then
                log_message "INFO - Waiting $RESTART_DELAY seconds before restart..."
                sleep $RESTART_DELAY
                
                # Clean up any zombie processes
                kill_existing_bots
                
                # Start bot again
                start_bot
                
                log_message "INFO - Bot restarted successfully"
            else
                log_message "ERROR - Maximum restart attempts reached. Monitor stopping."
                break
            fi
        fi
    done
    
    log_message "INFO - Discord Bot Monitor Stopped"
}

# Handle script termination
cleanup() {
    log_message "INFO - Monitor received termination signal"
    kill_existing_bots
    exit 0
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Start monitoring
main