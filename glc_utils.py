# glc_utils.py

import os
import mysql.connector
import requests
import savepagenow
import json
from datetime import datetime, timezone, time
import pytz
from bs4 import BeautifulSoup

def get_db_connection(db_name=None):
    """データベースへの接続を取得します。"""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'goudge-tc01-sid'),
            database=db_name,
            user=os.getenv('DB_USER', 'mguuji'),
            password=os.getenv('DB_PASSWORD')
        )
        return connection
    except mysql.connector.Error as e:
        print(f"データベース接続エラー: {e}")
        return None

def is_within_time_range():
    """現在の時刻が指定された時間範囲内にあるかどうかをチェックします。"""
    jst = pytz.timezone('Asia/Tokyo')
    current_time = datetime.now(jst).time()
    return time(7, 0) <= current_time <= time(19, 0)

def calculate_sha3_512(content):
    """コンテンツのSHA3-512ハッシュ値を計算します。"""
    import hashlib
    return hashlib.sha3_512(content.encode('utf-8')).hexdigest()

def compress_scraping_results(db_name):
    """スクレイピング結果テーブルから重複したレコードを削除します。"""
    conn = get_db_connection(db_name)
    if conn is None:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DELETE t1 FROM scraping_results t1
            INNER JOIN scraping_results t2
            WHERE t1.id < t2.id AND t1.target_id = t2.target_id AND t1.last_content = t2.last_content
        """)
        deleted_count = cursor.rowcount
        conn.commit()
        print(f"圧縮完了: {deleted_count}件の重複レコードを削除しました。")
    except Exception as e:
        print(f"圧縮中にエラーが発生しました: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def check_url_status(url, timeout=10):
    """URLのステータスコードをチェックします。"""
    try:
        response = requests.head(url, allow_redirects=True, timeout=timeout)
        return response.status_code
    except requests.RequestException:
        return None

def archive_with_custom_user_agent(url, user_agent):
    """指定されたユーザーエージェントを使用してURLをアーカイブします。"""
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

def load_toot_config():
    """Mastodonのトゥート設定を読み込みます。"""
    config_path = os.path.expanduser('~/.config/toot/config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)
    active_user = config['active_user']
    user_config = config['users'][active_user]
    app_config = config['apps'][user_config['instance']]
    return {
        'access_token': user_config['access_token'],
        'base_url': app_config['base_url'],
        'client_id': app_config['client_id'],
        'client_secret': app_config['client_secret']
    }

def fetch_url_content(url, user_agent=None):
    headers = {'User-Agent': user_agent} if user_agent else {}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.content.decode('utf-8', errors='replace')
    except requests.exceptions.RequestException as e:
        print(f"URL取得エラー ({url}): {e}")
        return None

def scrape_content(content, tag, tag_id, tag_class):
    soup = BeautifulSoup(content, 'html.parser')
    if tag_id:
        elements = soup.find_all(tag, id=tag_id)
    elif tag_class:
        classes = tag_class.split()
        elements = soup.find_all(tag, class_=lambda x: x and all(c in x.split() for c in classes))
    else:
        elements = soup.find_all(tag)

    if elements:
        return '\n'.join([element.text.strip() for element in elements])
    else:
        print(f"要素が見つかりません: tag={tag}, tag_id={tag_id}, tag_class={tag_class}")
        print(f"ページの内容（最初の500文字）: {soup.prettify()[:500]}...")
        return None

def get_initial_content(url, check_lastmodified, tag, tag_id, tag_class, user_agent):
    if check_lastmodified:
        try:
            response = requests.head(url, headers={'User-Agent': user_agent}, timeout=30)
            response.raise_for_status()
            last_modified = response.headers.get('Last-Modified')
            if last_modified:
                last_update = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
                return last_modified, last_update, None
            else:
                print(f"Last-Modifiedヘッダーがありません: {url}")
                return None, None, None
        except requests.exceptions.RequestException as e:
            print(f"URL取得エラー ({url}): {e}")
            return None, None, None
    else:
        content = fetch_url_content(url, user_agent)
        if content is None:
            return None, None, None

        scraped_content = scrape_content(content, tag, tag_id, tag_class)
        if scraped_content is None:
            print(f"コンテンツのスクレイピングに失敗しました: {url}")
            return None, None, None

        last_update = datetime.now(timezone.utc)
        content_hash = calculate_sha3_512(scraped_content)
        return scraped_content, last_update, content_hash
