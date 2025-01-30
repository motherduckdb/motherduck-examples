import requests
import pandas as pd
import duckdb
import logging
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_github_data():
    url = 'https://api.github.com/repos/duckdb/duckdb/stats/contributors'
    logger.info(f"Fetching data from {url}")
    response = requests.get(url)
    response.raise_for_status()
    logger.info("Data fetched successfully")
    return response.json()

def process_data(data):
    logger.info("Processing data")
    records = []
    for author in data:
        total_commits = sum(week['c'] for week in author['weeks'])
        records.append({
            'login': author['author']['login'],
            'total_commits': total_commits
        })
    df = pd.DataFrame(records)
    logger.info("Data processed into DataFrame")
    return df

def main():
    logger.info("Starting main process")
    data = fetch_github_data()
    df = process_data(data)
    
    # Connect to MotherDuck and create a table
    con = duckdb.connect()
    logger.info("Connecting to MotherDuck")
    con.sql("ATTACH 'md:'")
    con.sql("CREATE DATABASE IF NOT EXISTS github")
    # Loading data into MotherDuck based on the DataFrame
    con.sql("CREATE TABLE IF NOT EXISTS github.github_commits AS SELECT * FROM df")
    logger.info("Data loaded into MotherDuck successfully")

if __name__ == "__main__":
    main()


