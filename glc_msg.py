#!/usr/bin/env python3
# glc_msg.py
# Version 7.0.0
# - 更新検出されたURLのqmd_nameに基づいてqmd_name_viewを参照し、メッセージを作成

import os
import yaml
import logging
from datetime import datetime
from dotenv import load_dotenv
from mastodon import Mastodon
#from atproto import Client as AtprotoClient
from twikit import Client as TwitterClient
from glc_utils import get_db_connection

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

def format_message(target_info):
    return MSG_CONFIG['Message']['format'].format(
        time=target_info['last_update'].strftime("%Y年%m月%d日 %H時%M分(日本時間)"),
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

#def send_bluesky(message, no_toot=False):
#    if no_toot:
#        logger.info(f"Blueskyメッセージ（送信されません）: {message}")
#        return
#
#    try:
#        client = AtprotoClient()
#        client.login(os.getenv('BLUESKY_HANDLE'), os.getenv('BLUESKY_PASSWORD'))
#        post = client.send_post(text=message)
#        logger.info(f"Blueskyへの投稿に成功しました。投稿URI: {post.uri}")
#    except Exception as e:
#        logger.error(f"Blueskyへの投稿に失敗しました: {str(e)}, 送信内容: {message}")

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

def process_updates(db_name, updated_qmd_names, no_toot=False):
    conn = get_db_connection(db_name)
    if conn is None:
        logger.error("データベース接続の取得に失敗しました。")
        return

    try:
        cursor = conn.cursor(dictionary=True)
        
        for qmd_name in updated_qmd_names:
            # qmd_name_viewから最新の2行を取得
            cursor.execute(f"SELECT * FROM {qmd_name}_view ORDER BY last_update DESC LIMIT 2")
            rows = cursor.fetchall()
            
            if len(rows) < 2:
                logger.info(f"{qmd_name}_view に十分なデータがありません。スキップします。")
                continue
            
            latest = rows[0]
            previous = rows[1]
            
            # 最新行と最最新行が異なる場合にメッセージを送信
            if latest != previous:
                message = format_message(latest)
                send_toot(message, no_toot)
                send_bluesky(message, no_toot)
                send_tweet(message, no_toot)
            else:
                logger.info(f"{qmd_name}_view の最新2行に変更がありません。メッセージは送信しません。")

    except Exception as e:
        logger.error(f"[glc_msg.py] 更新処理中にエラーが発生: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Send messages for updated targets")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--qmd_names", required=True, nargs='+', help="List of updated qmd_names")
    parser.add_argument("--no-toot", action="store_true", help="Don't actually send messages, just print them")
    args = parser.parse_args()

    process_updates(args.db, args.qmd_names, args.no_toot)
