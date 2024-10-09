#!/usr/bin/env python3

import mysql.connector
import argparse
from dotenv import load_dotenv
import os

load_dotenv()

def get_db_connection(db_name):
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=db_name
    )

def display_updated_targets_view(db_name):
    conn = get_db_connection(db_name)
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("SELECT * FROM updated_targets_view ORDER BY last_update DESC")
        results = cursor.fetchall()

        if not results:
            print("No records found in updated_targets_view.")
            return

        print("Contents of updated_targets_view:")
        print("---------------------------------")
        for row in results:
            print(f"ID: {row['id']}")
            print(f"Last Update: {row['last_update']}")
            print(f"Owner: {row['owner']}")
            print(f"Owner URL: {row['ownerurl']}")
            print(f"Title: {row['title']}")
            print(f"URL: {row['url']}")
            print(f"Archive URL: {row['archive_url']}")
            print(f"QMD Name: {row['qmd_name']}")
            print("---------------------------------")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Display contents of updated_targets_view")
    parser.add_argument("--db", required=True, help="Database name")
    args = parser.parse_args()

    display_updated_targets_view(args.db)
