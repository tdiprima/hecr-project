# Interfolio Data Collector

  ### Created files:
  - `models.py` - SQLAlchemy models for User, Publication, and Grant tables
  - `create_db.py` - Script to create database tables
  - `gather_data.py` - Multi-threaded data collection script using 16 cores
  - requirements.txt - Python dependencies
  - README.md - Setup and usage instructions

  ### Key features implemented:
  - Efficient 16-core multi-threading for API calls
  - Proper Interfolio API authentication based on far_example.py
  - SQLAlchemy ORM with comprehensive data models
  - Error handling and progress statistics
  - Support for Journal Articles, Books, and Grants
  - Duplicate record handling with merge operations

  ### Usage:
  1. pip install -r requirements.txt
  2. python `create_db.py`
  3. python `gather_data.py`
