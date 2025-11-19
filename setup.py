"""Setup script for NYC 311 Analytics Agent."""
import os
import sys
import subprocess
from pathlib import Path


def check_env_file():
    """Check if .env file exists and prompt to create it."""
    env_file = Path(".env")
    if not env_file.exists():
        print("⚠️  .env file not found!")
        print("\nPlease create a .env file with the following content:")
        print("\nDEEPSEEK_API_KEY=your_deepseek_api_key_here")
        print("DEEPSEEK_BASE_URL=https://api.deepseek.com/v1")
        print("DEEPSEEK_MODEL=deepseek-chat")
        print("DB_PATH=data/nyc_311.duckdb")
        print("TABLE_NAME=nyc_311")
        return False
    return True


def install_dependencies():
    """Install Python dependencies."""
    print("📦 Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "backend/requirements.txt"])
        print("✅ Dependencies installed successfully!")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to install dependencies")
        return False


def check_csv_file():
    """Check if CSV file exists in data directory."""
    data_dir = Path("data")
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        print("⚠️  No CSV file found in data/ directory")
        print("Please download the NYC 311 dataset and place it in the data/ directory")
        return None
    return csv_files[0]


def run_etl(csv_path):
    """Run ETL script to load data."""
    print(f"🔄 Running ETL for {csv_path}...")
    try:
        subprocess.check_call([sys.executable, "-m", "backend.etl", str(csv_path)])
        print("✅ ETL completed successfully!")
        return True
    except subprocess.CalledProcessError:
        print("❌ ETL failed")
        return False


def main():
    """Main setup function."""
    print("🚀 NYC 311 Analytics Agent Setup\n")
    
    # Check environment file
    if not check_env_file():
        print("\n⚠️  Please create .env file before continuing")
        return
    
    # Install dependencies
    if not install_dependencies():
        return
    
    # Check for CSV file
    csv_path = check_csv_file()
    if csv_path:
        response = input(f"\nFound CSV file: {csv_path}\nRun ETL now? (y/n): ")
        if response.lower() == 'y':
            run_etl(csv_path)
    else:
        print("\n⚠️  Please download the NYC 311 dataset first")
        print("Then run: python -m backend.etl data/your_file.csv")
    
    print("\n✅ Setup complete!")
    print("\nTo start the server, run:")
    print("  uvicorn backend.main:app --reload")
    print("\nThen open frontend/index.html in your browser")


if __name__ == "__main__":
    main()

