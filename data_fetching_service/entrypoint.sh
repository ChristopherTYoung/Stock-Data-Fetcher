#!/bin/bash
set -e

# Start the Uvicorn server in the background
echo "Starting Uvicorn server..."
uvicorn main:app --host 0.0.0.0 --port 8000 &
UVICORN_PID=$!

# Give the server a moment to start
sleep 2

# Start the worker scheduler
echo "Starting worker scheduler..."
python worker_scheduler.py &
WORKER_PID=$!

# Wait for both processes
wait $UVICORN_PID $WORKER_PID
