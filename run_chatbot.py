#!/usr/bin/env python3
"""
Zoo Digital Platform - Streamlit Application Runner

This script runs the comprehensive zoo analytics dashboard and AI chatbot.
"""

import os
import sys
import subprocess
from pathlib import Path

def main():
    """Run the Streamlit application."""
    
    # Add the app directory to Python path
    app_dir = Path(__file__).parent / "app"
    sys.path.insert(0, str(app_dir))
    
    # Set environment variables
    os.environ.setdefault("STREAMLIT_SERVER_PORT", "8501")
    os.environ.setdefault("STREAMLIT_SERVER_ADDRESS", "localhost")
    
    # Change to the genai directory
    genai_dir = app_dir / "genai"
    os.chdir(genai_dir)
    
    print("🦁 Starting Zoo Digital Platform...")
    print("📊 Dashboard will be available at: http://localhost:8501")
    print("🤖 AI Assistant will be available in the navigation")
    print("\nPress Ctrl+C to stop the application")
    
    try:
        # Run the Streamlit app
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", "main.py",
            "--server.port", "8501",
            "--server.address", "localhost",
            "--browser.gatherUsageStats", "false"
        ])
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user")
    except Exception as e:
        print(f"❌ Error running application: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 