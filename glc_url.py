#!/usr/bin/env python3
# glc_url.py

import logging
from datetime import datetime, timezone
from glc_utils import get_initial_content, calculate_sha3_512

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_target(conn, session, target):
    url = target['url']
    check_lastmodified = target['check_lastmodified']
    tag = target['tag']
    tag_id = target['tag_id']
    tag_class = target['tag_class']
    target_id = target['id']

    user_agent = session.headers['User-Agent']

    last_content, last_update, content_hash = get_initial_content(url, check_lastmodified, tag, tag_id, tag_class, user_agent)
    
    if last_content is None:
        logger.error(f"コンテンツの取得に失敗しました: {url}")
        return

    cursor = conn.cursor()
    try:
        if not check_lastmodified:
            # check_lastmodified=0の場合、SHA3-512ハッシュ値を計算
            last_content = calculate_sha3_512(last_content)
            last_update = datetime.now(timezone.utc)

        cursor.execute("""
            INSERT INTO scraping_results (target_id, last_content, last_update, content_hash)
            VALUES (%s, %s, %s, %s)
        """, (target_id, last_content, last_update, content_hash))
        conn.commit()
        logger.info(f"URLの処理が完了しました: {url}")
    except Exception as e:
        logger.error(f"データベース挿入エラー: {e}")
        conn.rollback()
    finally:
        cursor.close()

def process_urls(conn):
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM scraping_targets")
        targets = cursor.fetchall()
        cursor.execute("SELECT agent FROM user_agents ORDER BY RAND() LIMIT 1")
        user_agent = cursor.fetchone()['agent']

        logger.debug(f"Found {len(targets)} targets")
        logger.debug(f"Selected user agent: {user_agent}")

        import requests
        session = requests.Session()
        session.headers.update({'User-Agent': user_agent})

        for target in targets:
            try:
                process_target(conn, session, target)
            except Exception as e:
                logger.error(f"ターゲット処理中にエラーが発生しました ({target['url']}): {e}")
    except Exception as e:
        logger.error(f"URL処理中にエラーが発生しました: {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    # テスト用のコード
    from glc_utils import get_db_connection
    db_name = "your_database_name"
    conn = get_db_connection(db_name)
    if conn:
        process_urls(conn)
        conn.close()
    else:
        logger.error("データベース接続の取得に失敗しました。")
