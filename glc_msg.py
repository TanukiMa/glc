#!/usr/bin/env python3
# glc_msg.py
# Version 4.3.0
# - 処理の順序を変更
# - 各URLの更新確認後にメッセージ投稿を行い、その後QMDファイルを生成するように修正

import yaml
import os
import sys
import pytz
import argparse
from datetime import datetime, timezone
from collections import defaultdict
from twikit import Client
from mastodon import Mastodon
from atproto import Client as AtprotoClient
from glc_utils import get_db_connection
from functools import lru_cache

def debug_print(message):
    if os.getenv('DEBUG'):
        print(f"[DEBUG] {message}", file=sys.stderr)

def load_msg_config():
    """msg.configから設定を読み込む"""
    with open('msg.config', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

# msg.configから設定を読み込む
MSG_CONFIG = load_msg_config()
TOP_PAGE_FILENAME = MSG_CONFIG['Files']['top_page_filename']

def execute_query(db_name, query, params=None):
    conn = get_db_connection(db_name)
    if not conn:
        debug_print(f"データベース接続に失敗しました: {db_name}")
        return None
    cursor = conn.cursor(dictionary=True)
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        debug_print(f"クエリ実行成功: {query}")
        return result
    except Exception as e:
        debug_print(f"クエリ実行エラー: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

@lru_cache(maxsize=128)
def get_updated_targets(db_name, offset=0, limit=1000000):
    query = """
    SELECT *
    FROM updated_targets_view
    ORDER BY last_update DESC
    LIMIT %s OFFSET %s
    """
    return execute_query(db_name, query, (limit, offset))

def get_qmd_targets(db_name):
    return execute_query(db_name, "SELECT * FROM qmd_view")

def sort_key(target):
    jst = pytz.timezone('Asia/Tokyo')
    last_update = target['last_update']
    if isinstance(last_update, str):
        last_update = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
    elif isinstance(last_update, datetime):
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=timezone.utc)
    return last_update.astimezone(jst)

def generate_quarto_content(updated_targets, qmd_targets):
    quarto_content = MSG_CONFIG['QuartoContent']['top_page_yaml'] + "\n\n"
    quarto_content += MSG_CONFIG['QuartoContent']['top_page_header'] + "\n"

    grouped_results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for target in updated_targets:
        last_update = sort_key(target)
        grouped_results[last_update.year][last_update.month][last_update.day].append(target)

    for year in sorted(grouped_results.keys(), reverse=True):
        quarto_content += f'\n## {year}年\n\n'
        for month in sorted(grouped_results[year].keys(), reverse=True):
            quarto_content += f'\n### {year}年{month}月\n\n'
            for day in sorted(grouped_results[year][month].keys(), reverse=True):
                quarto_content += f'\n#### {year}年{month}月{day}日\n\n'
                for target in sorted(grouped_results[year][month][day], key=sort_key, reverse=True):
                    formatted_time = sort_key(target).strftime("%Y年%m月%d日 %H時%M分(日本時間)")
                    archive_link = f"[🪦]({target['archive_url']}){{.external target=\"_blank\"}}" if target.get('archive_url') else ""
                    quarto_content += f"* {formatted_time}、[{target['owner']}]({target['ownerurl']}){{.external target=\"_blank\"}} の [{target['title']}]({target['url']}){{.external target=\"_blank\"}} が更新されました。{archive_link} ([💾]({target['qmd_name']}.qmd){{.external target=\"_blank\"}})\n"

    quarto_content += '\n# 全ての更新履歴\n\n'
    for target in qmd_targets:
        quarto_content += f"* [{target['owner']} の {target['title']}]({target['qmd_name']}.qmd){{.external target=\"_blank\"}}\n"

    return quarto_content

def generate_qmd_content(updated_targets, qmd_name, title, owner):
    debug_print(f"QMDコンテンツ生成開始: {qmd_name}")
    qmd_content = MSG_CONFIG['IndividualQuartoContent']['individual_page_yaml'].format(owner=owner, title=title)
    qmd_content += "\n"

    relevant_updates = [target for target in updated_targets if target['qmd_name'] == qmd_name]
    if not relevant_updates:
        debug_print(f"更新履歴なし: {qmd_name}")
        return None

    for target in sorted(relevant_updates, key=sort_key, reverse=True):
        formatted_time = sort_key(target).strftime('%Y年%m月%d日 %H時%M分(日本時間)')
        archive_link = f"[🪦]({target['archive_url']}){{.external target=\"_blank\"}}" if target.get('archive_url') else ""
        qmd_content += f"* {formatted_time}、[{target['owner']}]({target['ownerurl']}){{.external target=\"_blank\"}} の [{target['title']}]({target['url']}){{.external target=\"_blank\"}} が更新されました。{archive_link}\n"

    debug_print(f"QMDコンテンツ生成完了: {qmd_name}")
    return qmd_content

def format_message(target_info, formatted_time):
    return MSG_CONFIG['Message']['format'].format(time=formatted_time, owner=target_info['owner'], title=target_info['title'], url=target_info['url'])

def send_toot(message, no_toot=False):
    if no_toot:
        print("トゥートメッセージ（送信されません）:", message)
        return

    try:
        mastodon = Mastodon(
            client_id=os.getenv('MASTODON_CLIENT_ID'),
            client_secret=os.getenv('MASTODON_CLIENT_SECRET'),
            access_token=os.getenv('MASTODON_ACCESS_TOKEN'),
            api_base_url=os.getenv('MASTODON_BASE_URL')
        )
        status = mastodon.status_post(message)
        print("トゥートの送信に成功しました。", f"投稿URL: {status['url']}")
    except Exception as e:
        print(f"トゥートの送信に失敗しました: {str(e)}", f"送信内容: {message}", file=sys.stderr)

def send_bluesky(message, no_toot=False):
    if no_toot:
        print("Blueskyメッセージ（送信されません）:", message)
        return

    try:
        client = AtprotoClient()
        client.login(os.getenv('BLUESKY_HANDLE'), os.getenv('BLUESKY_PASSWORD'))
        post = client.send_post(text=message)
        print("Blueskyへの投稿に成功しました。", f"投稿URI: {post.uri}")
    except Exception as e:
        print(f"Blueskyへの投稿に失敗しました: {str(e)}", f"送信内容: {message}", file=sys.stderr)

async def send_tweet(message, no_toot=False):
    if no_toot:
        print("Twitterメッセージ（送信されません）:", message)
        return

    try:
        client = Client('en-US')
        await client.login(auth_info_1=os.getenv('TWITTER_USERNAME'), auth_info_2=os.getenv('TWITTER_EMAIL'), password=os.getenv('TWITTER_PASSWORD'))
        tweet = await client.create_tweet(text=message)
        print("Twitterへの投稿に成功しました。", f"投稿ID: {tweet.id}")
    except Exception as e:
        if "flow name LoginFlow is currently not accessible" in str(e):
            print("Twitterへのログインに失敗しました。しばらく時間をおいて再試行してください。", file=sys.stderr)
        else:
            print(f"Twitterへの投稿に失敗しました: {str(e)}", f"送信内容: {message}", file=sys.stderr)

async def process_updates(db_name, no_toot=False):
    try:
        debug_print("更新処理開始")
        
        # 1. 各URLの更新を確認（この部分はglc.pyで行われるため、ここでは省略）

        # 2. Mastodon, Twitter, Blueskyへのメッセージ投稿
        updated_targets = get_updated_targets(db_name)
        if updated_targets:
            for update in updated_targets:
                formatted_time = sort_key(update).strftime("%Y年%m月%d日 %H時%M分(日本時間)")
                message = format_message(update, formatted_time)
                send_toot(message, no_toot)
                send_bluesky(message, no_toot)
                await send_tweet(message, no_toot)

        # 3. 個別のqmd_name.qmdファイルの作成
        qmd_targets = get_qmd_targets(db_name)
        if qmd_targets:
            for target in qmd_targets:
                qmd_content = generate_qmd_content(updated_targets, target['qmd_name'], target['title'], target['owner'])
                if qmd_content:
                    with open(f"{target['qmd_name']}.qmd", 'w', encoding='utf-8') as f:
                        f.write(qmd_content)
                    debug_print(f"{target['qmd_name']}.qmd ファイルが生成されました。")
                else:
                    debug_print(f"{target['qmd_name']}.qmd ファイルは更新履歴がないため生成されませんでした。")

        # 4. qmd_viewの更新（この部分はデータベース側で自動的に行われるため、ここでの特別な処理は不要）

        # 5. top_page_filename.qmdの作成
        quarto_content = generate_quarto_content(updated_targets, qmd_targets)
        if quarto_content:
            with open(TOP_PAGE_FILENAME, 'w', encoding='utf-8') as f:
                f.write(quarto_content)
            debug_print(f"{TOP_PAGE_FILENAME} ファイルが生成されました。")

        debug_print("更新処理完了")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}", file=sys.stderr)

async def main():
    parser = argparse.ArgumentParser(description="Process updates, send messages, and generate Quarto output for GLC")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--no-toot", action="store_true", help="Don't actually send toots, Bluesky posts, or tweets, just print the message")
    args = parser.parse_args()

    await process_updates(args.db, args.no_toot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
