#!/bin/bash

# Redshift Watchdog - Start Script
echo "🔍 Starting Redshift Watchdog..."
echo "=================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "⚠️  Warning: .env file not found!"
    echo "Please create a .env file with your configuration:"
    echo ""
    echo "GOOGLE_API_KEY=your_gemini_api_key_here"
    echo "GEMINI_MODEL=gemini-2.5-flash"
    echo "REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com"
    echo "REDSHIFT_DATABASE=your_database"
    echo "REDSHIFT_USER=your_username"
    echo "REDSHIFT_PASSWORD=your_password"
    echo "REDSHIFT_PORT=5439"
    echo "DB_MCP_MODE=readonly"
    echo ""
    echo "You can also configure these in the web interface."
    echo ""
fi

# Start the application
echo "🚀 Starting Streamlit application..."
echo "Open your browser to: http://localhost:8501"
echo ""

streamlit run app.py 