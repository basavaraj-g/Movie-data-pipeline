# Movie Data Engineering Pipeline

## Overview
This project implements a simple ETL pipeline to ingest movie data, enrich it
using the OMDb API, and store it in a relational database.

## Tech Stack
- Python
- Pandas
- SQLite
- OMDb API

## How to Run
1. Install dependencies:
   pip install pandas requests
2. Create database:
   sqlite3 movies.db < schema.sql
3. Run ETL:
   python etl.py

## Design Decisions
- Normalized database schema
- Idempotent ETL
- Graceful API error handling

## Challenges
- API title mismatches
- Missing metadata
- Rate limiting

## Improvements
- PostgreSQL
- Airflow
- Parallel API calls
