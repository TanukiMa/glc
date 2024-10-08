#!/usr/bin/env python3
# glc_csr.py
# Version 1.0.0
# - scraping_resultsテーブルの圧縮機能

import argparse
import logging
from glc_utils import get_db_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def compress_scraping_results(db_name, debug=False):
    conn = get_db_connection(db_name)
    if conn is None:
        logger.error("データベース接続の取得に失敗しました。")
        return

    cursor = conn.cursor()
    try:
        # 削除対象のレコードを取得
        cursor.execute("""
            SELECT t1.id, t1.target_id, t1.last_content, t1.last_update
            FROM scraping_results t1
            INNER JOIN (
                SELECT target_id, last_content, MIN(id) as min_id
                FROM scraping_results
                GROUP BY target_id, last_content
            ) t2 ON t1.target_id = t2.target_id AND t1.last_content = t2.last_content
            WHERE t1.id > t2.min_id
        """)
        to_delete = cursor.fetchall()

        if debug:
            for row in to_delete:
                logger.debug(f"削除予定: ID={row[0]}, target_id={row[1]}, last_update={row[3]}, last_content={row[2][:50]}...")

        # 重複レコードの削除
        cursor.execute("""
            DELETE t1 FROM scraping_results t1
            INNER JOIN (
                SELECT target_id, last_content, MIN(id) as min_id
                FROM scraping_results
                GROUP BY target_id, last_content
            ) t2 ON t1.target_id = t2.target_id AND t1.last_content = t2.last_content
            WHERE t1.id > t2.min_id
        """)
        deleted_count = cursor.rowcount
        conn.commit()
        logger.info(f"圧縮完了: {deleted_count}件の重複レコードを削除しました。")
    except Exception as e:
        logger.error(f"圧縮中にエラーが発生しました: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Compress scraping_results table")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    compress_scraping_results(args.db, args.debug)

if __name__ == "__main__":
    main()
