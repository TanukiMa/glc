#!/usr/bin/env python3
# check_glc_diff.py

import argparse
import json
import logging
from dotenv import load_dotenv
import mysql.connector
import os

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection(db_name):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=db_name
        )
        return connection
    except mysql.connector.Error as e:
        logger.error(f"データベース接続エラー: {e}")
        return None

def check_updates(conn, debug=False):
    cursor = conn.cursor(dictionary=True)
    comparison_results = []

    try:
        cursor.execute("""
            SELECT t.id, t.url, 
                   r1.last_content AS latest_content, r1.last_update AS latest_update,
                   r2.last_content AS previous_content, r2.last_update AS previous_update
            FROM scraping_targets t
            JOIN scraping_results r1 ON t.id = r1.target_id
            LEFT JOIN scraping_results r2 ON t.id = r2.target_id AND r2.id < r1.id
            WHERE r1.id = (SELECT MAX(id) FROM scraping_results WHERE target_id = t.id)
            AND (r2.id IS NULL OR r2.id = (SELECT MAX(id) FROM scraping_results WHERE target_id = t.id AND id < r1.id))
        """)
        
        results = cursor.fetchall()
        
        for result in results:
            comparison_result = {
                "target_id": result['id'],
                "url": result['url'],
                "last_update": result['latest_update'].isoformat() if result['latest_update'] else None,
                "last_content": result['latest_content'],
                "previous_update": result['previous_update'].isoformat() if result['previous_update'] else None,
                "previous_content": result['previous_content'],
                "is_updated": result['latest_content'] != result['previous_content'] if result['previous_content'] is not None else None
            }
            comparison_results.append(comparison_result)

    except Exception as e:
        logger.error(f"更新チェック中にエラーが発生しました: {e}")
    finally:
        cursor.close()

    return comparison_results

def main(db_name, debug=False):
    conn = get_db_connection(db_name)
    if conn:
        try:
            results = check_updates(conn, debug)
            print(json.dumps(results, indent=2, ensure_ascii=False))
        finally:
            conn.close()
    else:
        logger.error("データベース接続の取得に失敗しました。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check the behavior of glc_diff.py")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    main(args.db, args.debug)
