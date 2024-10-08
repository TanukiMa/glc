#!/usr/bin/env python3
# glc_msg.py
# Version 5.0.1
# - SNSへのメッセージ投稿機能のみを担当

import os
import yaml
import logging
from datetime import datetime
from dotenv import load_dotenv
from mastodon import Mastodon
from atproto import Client as AtprotoClient
from twikit import Client as TwitterClient
from glc_utils import get_db_connection, sort_key

# 環境変数の読み込みとロギングの設定
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_msg_config():
    try:
        with open('msg.config', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"設定ファイルの読み込みに失敗しました: {str(e)}")
        return None

MSG_CONFIG = load_msg_config()

def format_message(target_info, formatted_time):
    return MSG_CONFIG['Message']['format'].format(
        time=formatted_time,
        owner=target_info['owner'],
        title=target_info['title'],
        url=target_info['url']
    )

def send_toot(message, no_toot=False):
    if no_toot:
        logger.info(f"トゥートメッセージ（送信されません）: {message}")
        return

    try:
        mastodon = Mastodon(
            client_id=os.getenv('MASTODON_CLIENT_ID'),
            client_secret=os.getenv('MASTODON_CLIENT_SECRET'),
            access_token=os.getenv('MASTODON_ACCESS_TOKEN'),
            api_base_url=os.getenv('MASTODON_BASE_URL')
        )
        status = mastodon.status_post(message)
        logger.info(f"トゥートの送信に成功しました。投稿URL: {status['url']}")
    except Exception as e:
        logger.error(f"トゥートの送信に失敗しました: {str(e)}, 送信内容: {message}")

def send_bluesky(message, no_toot=False):
    if no_toot:
        logger.info(f"Blueskyメッセージ（送信されません）: {message}")
        return

    try:
        client = AtprotoClient()
        client.login(os.getenv('BLUESKY_HANDLE'), os.getenv('BLUESKY_PASSWORD'))
        post = client.send_post(text=message)
        logger.info(f"Blueskyへの投稿に成功しました。投稿URI: {post.uri}")
    except Exception as e:
        logger.error(f"Blueskyへの投稿に失敗しました: {str(e)}, 送信内容: {message}")

def send_tweet(message, no_toot=False):
    if no_toot:
        logger.info(f"Twitterメッセージ（送信されません）: {message}")
        return

    try:
        client = TwitterClient('en-US')
        client.login(
            auth_info_1=os.getenv('TWITTER_USERNAME'),
            auth_info_2=os.getenv('TWITTER_EMAIL'),
            password=os.getenv('TWITTER_PASSWORD')
        )
        tweet = client.create_tweet(text=message)
        logger.info(f"Twitterへの投稿に成功しました。投稿ID: {tweet.id}")
    except Exception as e:
        logger.error(f"Twitterへの投稿に失敗しました: {str(e)}, 送信内容: {message}")

def send_messages(updated_targets, no_toot=False):
    formatted_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for target in updated_targets:
        message = format_message(target, formatted_time)
        send_toot(message, no_toot)
        send_bluesky(message, no_toot)
        send_tweet(message, no_toot)

def generate_qmd_content(updated_targets, qmd_name, title, owner):
    # QMDコンテンツ生成ロジックをここに実装
    pass

def generate_top_page_content(updated_targets, qmd_targets):
    # トップページコンテンツ生成ロジックをここに実装
    pass


def process_updates(db_name, updated_targets, no_toot=False):
    if not updated_targets or not isinstance(updated_targets, list):
        logger.info("[glc_msg.py] 更新されたターゲットはありません。")
        return

    conn = get_db_connection(db_name)
    if conn is None:
        logger.error("[glc_msg.py] データベース接続の取得に失敗しました。")
        return

    try:
        cursor = conn.cursor(dictionary=True)
        
        # QMDターゲットを取得
        cursor.execute("SELECT * FROM qmd_view")
        qmd_targets = cursor.fetchall()

        # メッセージを送信
        send_messages(updated_targets, no_toot)

        # 個別のQMDファイルを生成
        for target in qmd_targets:
            qmd_content = generate_qmd_content(updated_targets, target['qmd_name'], target['title'], target['owner'])
            if qmd_content:
                with open(f"{target['qmd_name']}.qmd", 'w', encoding='utf-8') as f:
                    f.write(qmd_content)

        # トップページを生成
        top_page_content = generate_top_page_content(updated_targets, qmd_targets)
        with open(MSG_CONFIG['Files']['top_page_filename'], 'w', encoding='utf-8') as f:
            f.write(top_page_content)

    except Exception as e:
        logger.error(f"[glc_msg.py] 更新処理中にエラーが発生: {str(e)}")
    finally:
        cursor.close()
        conn.close()
    

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SNSへのメッセージ投稿と更新処理")
    parser.add_argument("--db", help="データベース名", required=True)
    parser.add_argument("--no-toot", action="store_true", help="メッセージを実際に送信しない")
    args = parser.parse_args()

    conn = get_db_connection(args.db)
    if conn:
        # updated_targetsの取得ロジックをここに実装
        updated_targets = []  # ダミーデータ、実際の実装に置き換える
        process_updates(args.db, updated_targets, args.no_toot)
    else:
        logger.error("データベース接続の取得に失敗しました。")
