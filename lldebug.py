#!/usr/bin/env python3
# lldebug.py

import argparse
import json
from dotenv import load_dotenv
from glc_utils import get_db_connection

load_dotenv()

def get_all_updates(db_name):
    conn = get_db_connection(db_name)
    if conn is None:
        return None

    cursor = conn.cursor(dictionary=True)
    try:
        query = """
        SELECT t.url, r.last_update, r.last_content
        FROM scraping_targets t
        JOIN scraping_results r ON t.id = r.target_id
        ORDER BY t.url, r.last_update
        """
        cursor.execute(query)
        results = cursor.fetchall()

        updates = {}
        for row in results:
            url = row['url']
            if url not in updates:
                updates[url] = []
            updates[url].append({
                'last_update': row['last_update'].isoformat() if row['last_update'] else None,
                'last_content': row['last_content']
            })

        return updates
    except Exception as e:
        print(f"エラー: {str(e)}")
        return None
    finally:
        cursor.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Retrieve all last_update and last_content for each URL")
    parser.add_argument("--db", required=True, help="Database name")
    args = parser.parse_args()

    updates = get_all_updates(args.db)
    if updates:
        print(json.dumps(updates, ensure_ascii=False, indent=2))
    else:
        print("データの取得に失敗しました。")

if __name__ == "__main__":
    main()
