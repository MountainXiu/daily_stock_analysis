#!/bin/bash

if [ -f analyzer.pid ]; then
    PID=$(cat analyzer.pid)
    if ps -p $PID > /dev/null; then
        echo "Stopping Analyzer (PID $PID)..."
        kill $PID
    else
        echo "Analyzer (PID $PID) not running."
    fi
    rm analyzer.pid
else
    echo "Analyzer PID file not found."
fi

if [ -f server.pid ]; then
    PID=$(cat server.pid)
    if ps -p $PID > /dev/null; then
        echo "Stopping Server (PID $PID)..."
        kill $PID
    else
        echo "Server (PID $PID) not running."
    fi
    rm server.pid
else
    echo "Server PID file not found."
fi
