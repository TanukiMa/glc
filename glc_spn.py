#!/usr/bin/env python3
# glc_spn.py

import logging
import savepagenow
import time
from queue import Queue
from threading import Timer
from glc_utils import get_db_connection, get_random_user_agent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class URLArchiver:
    def __init__(self, db_name):
        self.db_name = db_name
        self.queue = Queue()
        self.last_archive_time = 0
        self.processing = False
        self.retry_count = {}

    def add_url(self, target_id, url):
        self.queue.put((target_id, url))
        self.retry_count[(target_id, url)] = 0
        if not self.processing:
            self.process_next()

    def process_next(self):
        if self.queue.empty():
            self.processing = False
            return

        self.processing = True
        current_time = time.time()
        wait_time = max(0, 20 - (current_time - self.last_archive_time))

        Timer(wait_time, self._archive_url).start()

    def _archive_url(self):
        target_id, url = self.queue.get()
        try:
            archived_url = self.archive_and_save(target_id, url)
            if archived_url:
                logger.info(f"[glc_spn.py] アーカイブ成功: {url}")
            else:
                self.retry_count[(target_id, url)] += 1
                if self.retry_count[(target_id, url)] < 3:
                    logger.warning(f"[glc_spn.py] アーカイブ失敗: {url}, リトライ回数: {self.retry_count[(target_id, url)]}")
                    self.queue.put((target_id, url))
                else:
                    logger.error(f"[glc_spn.py] アーカイブ失敗: {url}, 3回のリトライ後も失敗")
        except Exception as e:
            logger.error(f"[glc_spn.py] アーカイブ処理中にエラーが発生: {url}, エラー: {str(e)}")

        self.last_archive_time = time.time()
        self.process_next()

    def archive_and_save(self, target_id, url):
        conn = get_db_connection(self.db_name)
        if conn is None:
            logger.error("[glc_spn.py] データベース接続の取得に失敗しました。")
            return None

        try:
            user_agent = get_random_user_agent(conn)
            if user_agent is None:
                logger.error("[glc_spn.py] ユーザーエージェントの取得に失敗しました。")
                return None

            archived_url = savepagenow.capture(url, user_agent=user_agent)
            
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO archive_urls (target_id, archive_url, created_at)
                VALUES (%s, %s, NOW())
                ON DUPLICATE KEY UPDATE archive_url = VALUES(archive_url), created_at = NOW()
            """, (target_id, archived_url))
            conn.commit()
            return archived_url
        except Exception as e:
            logger.error(f"[glc_spn.py] アーカイブ失敗: {url}, エラー: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()

def archive_updated_urls(db_name, updated_targets):
    archiver = URLArchiver(db_name)
    for target in updated_targets:
        archiver.add_url(target['id'], target['url'])

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Archive updated URLs")
    parser.add_argument("--db", required=True, help="Database name")
    args = parser.parse_args()

    # テスト用のコード
    test_targets = [
        {"id": 1, "url": "https://example.com"},
        {"id": 2, "url": "https://example.org"},
    ]
    archive_updated_urls(args.db, test_targets)
