#!/usr/bin/env python3
# glc_log.py
# Version 1.0.0
# - Initial version: View action_logs in syslog or JSON format

import argparse
import json
import mysql.connector
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

def get_db_connection(db_name):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=db_name,
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return connection
    except mysql.connector.Error as e:
        print(f"データベース接続エラー: {e}")
        return None

def fetch_logs(db_name):
    conn = get_db_connection(db_name)
    if conn is None:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT al.*, st.url 
            FROM action_logs al
            JOIN scraping_targets st ON al.target_id = st.id
            ORDER BY al.action_time DESC
        """)
        logs = cursor.fetchall()
        return logs
    except mysql.connector.Error as e:
        print(f"ログの取得中にエラーが発生しました: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def format_syslog(log):
    timestamp = log['action_time'].strftime("%b %d %H:%M:%S")
    status = "SUCCESS" if log['status'] else "FAILURE"
    return f"{timestamp} GLC[{log['id']}]: {log['action_type'].upper()} {status} - URL: {log['url']} - {log['message']}"

def display_logs(logs, json_format=False):
    if json_format:
        print(json.dumps(logs, default=str, indent=2))
    else:
        for log in logs:
            print(format_syslog(log))

def main():
    parser = argparse.ArgumentParser(description="View action logs in syslog or JSON format")
    parser.add_argument("--db", required=True, help="Database name to use")
    parser.add_argument("--json", action="store_true", help="Display logs in JSON format")
    args = parser.parse_args()

    logs = fetch_logs(args.db)
    display_logs(logs, args.json)

if __name__ == "__main__":
    main()
