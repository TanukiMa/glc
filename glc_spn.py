#!/usr/bin/env python3
# glc_spn.py
# Version 1.2.0
# - アーカイブリクエストを待機リストに追加し、21秒間隔で処理する機能を実装
# - savepagenowを使用したアーカイブ機能を統合
# - エラーハンドリングとログ機能を改善

import time
import threading
from queue import Queue
import savepagenow
import requests
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

waiting_list = Queue()
processing_lock = threading.Lock()

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

def check_url_status(url, timeout=10):
    try:
        response = requests.head(url, allow_redirects=True, timeout=timeout)
        return response.status_code
    except requests.RequestException:
        return None

def archive_with_custom_user_agent(url, user_agent):
    result = {
        "original_url": url,
        "archived_url": None,
        "newly_captured": None,
        "success": False,
        "error": None,
        "status_code": None
    }
    
    status_code = check_url_status(url)
    result["status_code"] = status_code

    if status_code != 200:
        result["error"] = f"URL returned status code {status_code}"
        return result

    try:
        archived_url = savepagenow.capture(url, user_agent=user_agent)
        result["archived_url"] = archived_url
        result["newly_captured"] = True
        result["success"] = True
    except savepagenow.exceptions.CachedPage as e:
        result["archived_url"] = str(e)
        result["newly_captured"] = False
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
    
    return result

def process_waiting_list():
    while True:
        with processing_lock:
            if not waiting_list.empty():
                db_name, target_id, url, user_agent = waiting_list.get()
                try:
                    result = archive_with_custom_user_agent(url, user_agent)
                    if result['success']:
                        update_archive_url(db_name, target_id, result['archived_url'])
                        print(f"アーカイブ成功: {url}")
                    else:
                        print(f"アーカイブ失敗: {url}, エラー: {result['error']}")
                        # 失敗した場合、再度キューに追加
                        waiting_list.put((db_name, target_id, url, user_agent))
                except Exception as e:
                    print(f"アーカイブ処理中にエラーが発生: {url}, エラー: {str(e)}")
                    # エラーが発生した場合も再度キューに追加
                    waiting_list.put((db_name, target_id, url, user_agent))
        time.sleep(21)  # 21秒間隔で処理

def update_archive_url(db_name, target_id, archived_url):
    conn = get_db_connection(db_name)
    if conn is None:
        return
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO archive_urls (target_id, archive_url, created_at)
            VALUES (%s, %s, NOW())
            ON DUPLICATE KEY UPDATE archive_url = VALUES(archive_url), created_at = NOW()
        ''', (target_id, archived_url))
        conn.commit()
    except Exception as e:
        print(f"アーカイブURLの更新中にエラーが発生: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def archive_and_save(db_name, target_id, url):
    conn = get_db_connection(db_name)
    if conn is None:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT agent FROM user_agents ORDER BY RAND() LIMIT 1")
        user_agent = cursor.fetchone()[0]
        waiting_list.put((db_name, target_id, url, user_agent))
        print(f"アーカイブリクエストを待機リストに追加: {url}")
    except Exception as e:
        print(f"アーカイブリクエストの追加中にエラーが発生: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# 待機リスト処理スレッドの開始
processing_thread = threading.Thread(target=process_waiting_list, daemon=True)
processing_thread.start()

if __name__ == "__main__":
    # テスト用のコード
    db_name = "your_database_name"
    target_id = 1  # テスト用のtarget_id
    url = "https://example.com"  # テスト用のURL
    
    archive_and_save(db_name, target_id, url)
    print("アーカイブリクエストを送信しました。処理は非同期で行われます。")
