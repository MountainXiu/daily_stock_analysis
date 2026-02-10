#!/bin/bash
set -e

# Activate virtual environment
source venv/bin/activate

# Create directories
mkdir -p data logs reports

# Start Analyzer in background
echo "Starting Stock Analyzer..."
nohup python main.py --schedule > logs/analyzer.log 2>&1 &
ANALYZER_PID=$!
echo $ANALYZER_PID > analyzer.pid
echo "Analyzer started with PID $ANALYZER_PID"

# Start Server in background
echo "Starting API Server..."
# Default port 8000 if not set
PORT=${API_PORT:-8000}
nohup python main.py --serve-only --host 0.0.0.0 --port $PORT > logs/server.log 2>&1 &
SERVER_PID=$!
echo $SERVER_PID > server.pid
echo "Server started with PID $SERVER_PID on port $PORT"

echo "Services started."
echo "Analyzer Log: logs/analyzer.log"
echo "Server Log:   logs/server.log"
