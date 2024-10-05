#!/usr/bin/env python3
# urljson.py
# Version 1.1.0
# JSON import and export functions for URL management
# 修正内容: 新しいqmd_nameが生成された際に、対応するビューを作成する機能を追加

import json
import argparse
from glc_utils import get_db_connection, get_initial_content, calculate_sha3_512
from glc_spn import archive_and_save
import time
import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

last_archive_time = 0

def generate_qmd_name():
    import string
    import secrets
    chars = [c for c in string.ascii_letters + string.digits if c not in 'OLI']
    return ''.join(secrets.choice(chars) for _ in range(8))

def confirm_add(data):
    print("以下の内容で登録しますか？")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    response = input("[Y/n]: ").strip().lower()
    return response == 'y' or response == ''

def archive_url(db_name, target_id, url):
    global last_archive_time
    current_time = time.time()
    if current_time - last_archive_time < 20:
        time.sleep(20 - (current_time - last_archive_time))
    archive_url = archive_and_save(db_name, target_id, url)
    last_archive_time = time.time()
    return archive_url

def create_qmd_view(db_name, qmd_name):
    conn = get_db_connection(db_name)
    if conn is None:
        return
    cursor = conn.cursor()
    try:
        view_name = f"{qmd_name}_view"
        query = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT qmd_name, owner, ownerurl, title, url, last_update
        FROM updated_targets_view
        WHERE qmd_name = '{qmd_name}';
        """
        cursor.execute(query)
        conn.commit()
        print(f"ビュー '{view_name}' を作成しました。")
    except mysql.connector.Error as err:
        print(f"ビューの作成中にエラーが発生しました: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def add_urls_from_json(db_name, json_file):
    with open(json_file, 'r') as file:
        urls_data = json.load(file)

    confirmed_urls = []
    for url_data in urls_data:
        if confirm_add(url_data):
            confirmed_urls.append(url_data)
        else:
            print(f"{url_data['url']}の登録をスキップしました。")

    for url_data in confirmed_urls:
        conn = get_db_connection(db_name)
        if conn is None:
            return
        cursor = conn.cursor()
        try:
            qmd_name = generate_qmd_name() if 'qmd_name' not in url_data or not url_data['qmd_name'] else url_data['qmd_name']
            check_lastmodified = url_data.get('check_lastmodified')
            
            if isinstance(check_lastmodified, str):
                check_lastmodified = check_lastmodified.lower() == 'true'
            elif check_lastmodified is None:
                check_lastmodified = False

            cursor.execute("""
                INSERT INTO scraping_targets
                (url, title, owner, ownerurl, check_lastmodified, tag, tag_id, tag_class, email_recipient, qmd_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                url_data['url'],
                url_data['title'],
                url_data['owner'],
                url_data['ownerurl'],
                check_lastmodified,
                url_data.get('tag'),
                url_data.get('tag_id'),
                url_data.get('tag_class'),
                url_data.get('email_recipient'),
                qmd_name
            ))
            conn.commit()

            cursor.execute("SELECT LAST_INSERT_ID()")
            target_id = cursor.fetchone()[0]

            cursor.execute("SELECT agent FROM user_agents ORDER BY RAND() LIMIT 1")
            user_agent = cursor.fetchone()[0]
            last_content, last_update, content_hash = get_initial_content(
                url_data['url'],
                check_lastmodified,
                url_data.get('tag'),
                url_data.get('tag_id'),
                url_data.get('tag_class'),
                user_agent
            )

            if not check_lastmodified and last_content:
                last_content = calculate_sha3_512(last_content)

            cursor.execute("""
                INSERT INTO scraping_results (target_id, last_content, last_update, content_hash)
                VALUES (%s, %s, %s, %s)
            """, (target_id, last_content, last_update, content_hash))
            conn.commit()

            print(f"URLを追加しました: {url_data['url']}")

            archive_url(db_name, target_id, url_data['url'])

            # 新しいqmd_nameに対応するビューを作成
            create_qmd_view(db_name, qmd_name)

        except Exception as e:
            print(f"URLの追加中にエラーが発生しました: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    print("JSONファイルからのURL追加が完了しました。")

def export_to_json(db_name, output_file):
    conn = get_db_connection(db_name)
    if conn is None:
        return
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM scraping_targets")
        urls = cursor.fetchall()
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(urls, f, ensure_ascii=False, indent=2)
        print(f"URLデータをJSONファイルにエクスポートしました: {output_file}")
    except Exception as e:
        print(f"URLデータのエクスポート中にエラーが発生しました: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Import and export URLs using JSON")
    parser.add_argument("--db", required=True, help="Database name to use")
    parser.add_argument("--action", choices=['import', 'export'], required=True, help="Action to perform")
    parser.add_argument("--json_file", required=True, help="JSON file to import from or export to")
    args = parser.parse_args()

    if args.action == 'import':
        add_urls_from_json(args.db, args.json_file)
    elif args.action == 'export':
        export_to_json(args.db, args.json_file)

if __name__ == "__main__":
    main()
