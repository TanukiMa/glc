#!/usr/bin/env python3
# glc.py
# Version 2.2.0
# - 更新確認後にglc_msg.pyを呼び出すように修正

import os
import sys
import time
import argparse
import random
import requests
import json
import unicodedata
from datetime import datetime, timezone
import mysql.connector
from contextlib import contextmanager
from glc_utils import get_db_connection, is_within_time_range, calculate_sha3_512, compress_scraping_results, get_initial_content
from glc_spn import archive_and_save
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import asyncio
import logging
import subprocess

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def set_log_level(debug):
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

def ascii_encode(text):
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')

@contextmanager
def db_cursor(db_name):
    conn = get_db_connection(db_name)
    if conn is None:
        raise Exception("データベース接続の取得に失敗しました。")
    try:
        cursor = conn.cursor(dictionary=True)
        yield cursor
        conn.commit()
    except mysql.connector.Error as e:
        logger.error(f"データベースエラー: {e}")
        conn.rollback()
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def get_previous_result(cursor, target_id):
    cursor.execute("SELECT last_update, content_hash FROM scraping_results WHERE target_id = %s ORDER BY id DESC LIMIT 1", (target_id,))
    return cursor.fetchone()

def insert_new_result(cursor, target_id, last_content, last_update, content_hash):
    cursor.execute("INSERT INTO scraping_results (target_id, last_content, last_update, content_hash) VALUES (%s, %s, %s, %s)",
                   (target_id, last_content, last_update, content_hash))

def is_content_updated(check_lastmodified, previous_result, last_update, last_content):
    if not previous_result:
        return True
    previous_last_update = previous_result['last_update']
    if previous_last_update and previous_last_update.tzinfo is None:
        previous_last_update = previous_last_update.replace(tzinfo=timezone.utc)
    if check_lastmodified:
        return previous_last_update is None or last_update > previous_last_update
    return previous_result['content_hash'] is None or last_content != previous_result['content_hash']

def process_target(db_name, target, user_agent):
    url, check_lastmodified, tag, tag_id, tag_class, target_id = target['url'], target['check_lastmodified'], target['tag'], target['tag_id'], target['tag_class'], target['id']
    logger.debug(f"Processing target: {url}")
    try:
        last_content, last_update, content_hash = get_initial_content(url, check_lastmodified, tag, tag_id, tag_class, user_agent)
        if last_content is None:
            logger.error(f"コンテンツの取得に失敗しました: {url}")
            return None
        logger.debug(f"Fetched content: last_update={last_update}, content_hash={content_hash}")
        if not check_lastmodified:
            last_content = calculate_sha3_512(last_content)
        with db_cursor(db_name) as cursor:
            previous_result = get_previous_result(cursor, target_id)
            if is_content_updated(check_lastmodified, previous_result, last_update, last_content):
                insert_new_result(cursor, target_id, last_content, last_update, content_hash)
                logger.info(f"更新を検出: {url}")
                archive_and_save(db_name, target_id, url)
                return target_id
            if not previous_result:
                insert_new_result(cursor, target_id, last_content, last_update, content_hash)
                logger.info(f"初回アクセス: {url}")
                archive_and_save(db_name, target_id, url)
        return None
    except Exception as e:
        logger.error(f"ターゲット処理中にエラーが発生しました ({url}): {e}")
        return None

def get_targets_and_user_agent(db_name):
    with db_cursor(db_name) as cursor:
        cursor.execute("SELECT * FROM scraping_targets")
        targets = cursor.fetchall()
        cursor.execute("SELECT agent FROM user_agents ORDER BY RAND() LIMIT 1")
        user_agent = cursor.fetchone()['agent']
    return targets, user_agent

async def process_targets(db_name, force=False, no_toot=False):
    if not force and not is_within_time_range():
        logger.warning("Execution outside local time 7:00-19:00 requires --force option.")
        return
    try:
        targets, user_agent = get_targets_and_user_agent(db_name)
        logger.debug(f"Found {len(targets)} targets")
        logger.debug(f"Selected user agent: {user_agent}")
        updated_target_ids = [process_target(db_name, target, user_agent) for target in targets if process_target(db_name, target, user_agent)]
        if updated_target_ids:
            logger.debug(f"Updated targets: {updated_target_ids}")
            await run_glc_msg(db_name, no_toot)
        compress_scraping_results(db_name)
    except Exception as e:
        logger.error(f"処理中に予期せぬエラーが発生しました: {e}")

async def run_glc_msg(db_name, no_toot):
    cmd = [sys.executable, 'glc_msg.py', '--db', db_name]
    if no_toot:
        cmd.append('--no-toot')
    
    process = await asyncio.create_subprocess_exec(*cmd)
    await process.communicate()

async def main():
    parser = argparse.ArgumentParser(description="Webページの更新をチェックし、更新があればアーカイブと通知を行います。")
    parser.add_argument("--force", action="store_true", help="現地時間7:00-19:00以外でも実行します。")
    parser.add_argument("--no-toot", action="store_true", help="Tootの送信を抑制します。")
    parser.add_argument("--db", required=True, help="使用するデータベース名")
    parser.add_argument("--debug", action="store_true", help="デバッグモードを有効にします")
    args = parser.parse_args()
    set_log_level(args.debug)
    logger.debug("Starting main function")
    await process_targets(args.db, args.force, args.no_toot)
    logger.debug("Finished main function")

if __name__ == "__main__":
    asyncio.run(main())
