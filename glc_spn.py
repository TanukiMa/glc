#!/usr/bin/env python3
# glc_spn.py
# Version 2.2.0
# - archive_updated_urlsの修正：整数型のtarget_idを正しく処理するように変更

import logging
from glc_utils import get_db_connection, archive_with_custom_user_agent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def archive_and_save(db_name, target_id, url):
    conn = get_db_connection(db_name)
    if conn is None:
        logger.error("データベース接続の取得に失敗しました。")
        return None

    try:
        cursor = conn.cursor(dictionary=True)
        user_agent = get_random_user_agent(conn)
        

        result = archive_with_custom_user_agent(url, user_agent)
        if result['success']:
            cursor.execute('''
                INSERT INTO archive_urls (target_id, archive_url, created_at)
                VALUES (%s, %s, NOW())
                ON DUPLICATE KEY UPDATE archive_url = VALUES(archive_url), created_at = NOW()
            ''', (target_id, result['archived_url']))
            conn.commit()
            logger.info(f"アーカイブ成功: {url}")
            return result['archived_url']
        else:
            logger.error(f"アーカイブ失敗: {url}, エラー: {result['error']}")
            return None
    except Exception as e:
        logger.error(f"アーカイブ処理中にエラーが発生: {str(e)}")
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()

def archive_updated_urls(db_name, updated_targets):
    if isinstance(updated_targets, list):
        for target in updated_targets:
            if isinstance(target, dict):
                archive_and_save(db_name, target['id'], target['url'])
            elif isinstance(target, int):
                # target_idが直接渡された場合の処理
                conn = get_db_connection(db_name)
                if conn:
                    try:
                        cursor = conn.cursor(dictionary=True)
                        cursor.execute("SELECT url FROM scraping_targets WHERE id = %s", (target,))
                        result = cursor.fetchone()
                        if result:
                            archive_and_save(db_name, target, result['url'])
                        else:
                            logger.error(f"Target ID {target} not found in database")
                    finally:
                        cursor.close()
                        conn.close()
    elif isinstance(updated_targets, int):
        # 単一のtarget_idが渡された場合の処理
        conn = get_db_connection(db_name)
        if conn:
            try:
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT url FROM scraping_targets WHERE id = %s", (updated_targets,))
                result = cursor.fetchone()
                if result:
                    archive_and_save(db_name, updated_targets, result['url'])
                else:
                    logger.error(f"Target ID {updated_targets} not found in database")
            finally:
                cursor.close()
                conn.close()
    else:
        logger.error("Invalid type for updated_targets")

if __name__ == "__main__":
    # テスト用のコード
    db_name = "your_database_name"
    updated_targets = [{"id": 1, "url": "https://example.com"}]
    archive_updated_urls(db_name, updated_targets)
