"""Quick check: can the backend start and listen? Run from backend folder: python check_backend.py"""
import sys
import subprocess

def main():
    print("Checking Python...")
    if sys.version_info < (3, 9):
        print("ERROR: Python 3.9+ required. You have:", sys.version)
        return 1
    print("  OK:", sys.version.split()[0])

    print("Checking dependencies...")
    try:
        import fastapi
        import uvicorn
        import pandas
        import numpy
    except ImportError as e:
        print("  ERROR: Missing dependency:", e)
        print("  Run: pip install -r requirements.txt")
        return 1
    print("  OK")

    print("Checking backend imports...")
    try:
        from main import app
    except Exception as e:
        print("  ERROR:", e)
        return 1
    print("  OK")

    print("\nBackend is ready to start. Run: uvicorn main:app --host 0.0.0.0 --port $PORT")
    print("Check deployment settings if hosting on Render.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
