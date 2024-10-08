#!/usr/bin/env python3
# glc_diff.py
# Version 2.3.0
# - check_lastmodifiedフラグを除去し、全てのターゲットで同じ更新判定ロジックを使用

import json
import logging
from glc_utils import get_db_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_updates(conn, debug=False):
    cursor = conn.cursor(dictionary=True)
    updated_targets = []

    try:
        cursor.execute("SELECT id, url FROM scraping_targets")
        targets = cursor.fetchall()

        for target in targets:
            target_id = target['id']
            target_url = target['url']
            
            cursor.execute("""
                SELECT last_content, last_update
                FROM scraping_results
                WHERE target_id = %s
                ORDER BY id DESC
                LIMIT 2
            """, (target_id,))
            
            results = cursor.fetchall()
            
            if len(results) == 2:
                latest = results[0]
                previous = results[1]
                
                if previous['last_content'] is not None and latest['last_content'] != previous['last_content']:
                    updated_targets.append({"id": target_id, "url": target_url})

        if debug:
            logger.debug(f"[glc_diff.py] Updated targets: {json.dumps(updated_targets, indent=2)}")

    except Exception as e:
        logger.error(f"[glc_diff.py] 更新チェック中にエラーが発生しました: {e}")
    finally:
        cursor.close()

    return updated_targets
    

def main(db_name, debug=False):
    conn = get_db_connection(db_name)
    if conn:
        updated = check_updates(conn, debug)
        logger.info(f"更新されたターゲット: {updated}")
        conn.close()
    else:
        logger.error("データベース接続の取得に失敗しました。")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Check for updates in scraped content")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    main(args.db, args.debug)
