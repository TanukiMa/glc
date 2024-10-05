#!/usr/bin/env python3
# urledit.py
# Version 1.11.0
# Changes:
# - Updated delete_url function to use URL instead of ID
# - Added cascade delete for related records in other tables
# - Updated edit_url function to handle multiple classes in tag_class
# - Removed add_from_json and export_json actions

import argparse
import mysql.connector
from dotenv import load_dotenv
from glc_spn import archive_and_save
import time
import os
import json
import string
import secrets
from glc_utils import get_db_connection, get_initial_content, calculate_sha3_512

load_dotenv()

last_archive_time = 0

def generate_qmd_name():
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

def add_url(db_name, url, title, owner, ownerurl, check_lastmodified, tag, tag_id, tag_class, email_recipient, qmd_name=None):
    if check_lastmodified is None:
        check_lastmodified = False  # デフォルト値

    if not qmd_name:
        qmd_name = generate_qmd_name()

    data = {
        "url": url,
        "title": title,
        "owner": owner,
        "ownerurl": ownerurl,
        "check_lastmodified": check_lastmodified,
        "tag": tag,
        "tag_id": tag_id,
        "tag_class": tag_class,
        "email_recipient": email_recipient,
        "qmd_name": qmd_name
    }

    if not confirm_add(data):
        print("登録をキャンセルしました。")
        return

    conn = get_db_connection(db_name)
    if conn is None:
        return

    cursor = conn.cursor()
    try:
        cursor.execute("""
        INSERT INTO scraping_targets
        (url, title, owner, ownerurl, check_lastmodified, tag, tag_id, tag_class, email_recipient, qmd_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (url, title, owner, ownerurl, check_lastmodified, tag, tag_id, tag_class, email_recipient, qmd_name))
        conn.commit()
        print(f"URLを追加しました: {url}")

        # Get the inserted id
        cursor.execute("SELECT LAST_INSERT_ID()")
        target_id = cursor.fetchone()[0]

        # Get initial content
        cursor.execute("SELECT agent FROM user_agents ORDER BY RAND() LIMIT 1")
        user_agent = cursor.fetchone()[0]
        last_content, last_update, content_hash = get_initial_content(url, check_lastmodified, tag, tag_id, tag_class, user_agent)

        # If check_lastmodified is False, store the hash of the scraped content
        if not check_lastmodified and last_content:
            last_content = calculate_sha3_512(last_content)

        # Insert into scraping_results
        cursor.execute("""
            INSERT INTO scraping_results (target_id, last_content, last_update, content_hash)
            VALUES (%s, %s, %s, %s)
        """, (target_id, last_content, last_update, content_hash))
        conn.commit()

        archive_url(db_name, target_id, url)
    except mysql.connector.Error as e:
        print(f"URLの追加またはアーカイブ中にエラーが発生しました: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()




def edit_url(db_name, url, **kwargs):
    conn = get_db_connection(db_name)
    if conn is None:
        return

    cursor = conn.cursor(dictionary=True)
    try:
        # 現在のURLを取得
        cursor.execute("SELECT id FROM scraping_targets WHERE url = %s", (url,))
        result = cursor.fetchone()
        if result is None:
            print(f"指定されたURL {url} は見つかりません。")
            return

        target_id = result['id']

        # 更新する項目と値をセット
        update_fields = []
        update_values = []
        for key, value in kwargs.items():
            if value is not None:
                update_fields.append(f"{key} = %s")
                update_values.append(value)

        # UPDATE文を実行
        if update_fields:
            update_query = f"UPDATE scraping_targets SET {', '.join(update_fields)} WHERE id = %s"
            cursor.execute(update_query, (*update_values, target_id))
            conn.commit()
            print(f"URL {url} を更新しました。")
        else:
            print("更新する項目がありません。")

    except mysql.connector.Error as e:
        print(f"URLの更新中にエラーが発生しました: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def delete_url(db_name, url):
    conn = get_db_connection(db_name)
    if conn is None:
        return

    cursor = conn.cursor()
    try:
        # Get the target_id for the given URL
        cursor.execute("SELECT id FROM scraping_targets WHERE url = %s", (url,))
        result = cursor.fetchone()
        if not result:
            print(f"指定されたURL {url} は見つかりません。")
            return

        target_id = result[0]

        # Delete related records from other tables
        cursor.execute("DELETE FROM archive_urls WHERE target_id = %s", (target_id,))
        cursor.execute("DELETE FROM action_logs WHERE target_id = %s", (target_id,))
        cursor.execute("DELETE FROM scraping_results WHERE target_id = %s", (target_id,))

        # Delete the main record from scraping_targets
        cursor.execute("DELETE FROM scraping_targets WHERE id = %s", (target_id,))

        conn.commit()
        print(f"URLを削除しました: {url}")
    except mysql.connector.Error as e:
        print(f"URLの削除中にエラーが発生しました: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def list_urls(db_name):
    conn = get_db_connection(db_name)
    if conn is None:
        return

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM scraping_targets")
        urls = cursor.fetchall()
        print(json.dumps(urls, indent=2, ensure_ascii=False))
    except mysql.connector.Error as e:
        print(f"URLの一覧取得中にエラーが発生しました: {e}")
    finally:
        cursor.close()
        conn.close()


    except mysql.connector.Error as e:
        print(f"URLの追加中にエラーが発生しました: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

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
            # 文字列またはブール値としてcheck_lastmodifiedを処理
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

            # Get the inserted id
            cursor.execute("SELECT LAST_INSERT_ID()")
            target_id = cursor.fetchone()[0]

            # Get initial content
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

            # If check_lastmodified is False, store the hash of the scraped content
            if not check_lastmodified and last_content:
                last_content = calculate_sha3_512(last_content)

            # Insert into scraping_results
            cursor.execute("""
                INSERT INTO scraping_results (target_id, last_content, last_update, content_hash)
                VALUES (%s, %s, %s, %s)
            """, (target_id, last_content, last_update, content_hash))
            conn.commit()

            print(f"URLを追加しました: {url_data['url']}")

            # savepagenow への登録
            archive_url(db_name, target_id, url_data['url'])

        except mysql.connector.Error as e:
            print(f"URLの追加中にエラーが発生しました: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    print("JSONファイルからのURL追加が完了しました。")





def main():
    parser = argparse.ArgumentParser(description="Manage URLs in the database")
    parser.add_argument("--db", required=True, help="Database name to use")
    parser.add_argument("--action", choices=['add', 'edit', 'delete', 'list'], required=True, help="Action to perform")
    parser.add_argument("--url", help="URL to add, edit, or delete")
    parser.add_argument("--title", help="Title of the page")
    parser.add_argument("--owner", help="Owner of the page")
    parser.add_argument("--ownerurl", help="Owner's URL")
    parser.add_argument("--check_lastmodified", type=str, choices=['true', 'false'], help="Whether to check Last-Modified header (true/false)")
    parser.add_argument("--tag", help="HTML tag to scrape")
    parser.add_argument("--tag_id", help="ID of the HTML tag to scrape")
    parser.add_argument("--tag_class", help="Class of the HTML tag to scrape")
    parser.add_argument("--email_recipient", help="Email recipient")
    args = parser.parse_args()

    if args.action == 'add':
        add_url(args.db, args.url, args.title, args.owner, args.ownerurl,
                args.check_lastmodified == 'true' if args.check_lastmodified else None,
                args.tag, args.tag_id, args.tag_class, args.email_recipient)
    elif args.action == 'edit':
        if not args.url:
            print("編集するURLを指定してください。")
            return
        edit_data = {k: v for k, v in vars(args).items() if k not in ['db', 'action', 'url'] and v is not None}
        if 'check_lastmodified' in edit_data:
            edit_data['check_lastmodified'] = edit_data['check_lastmodified'] == 'true'
        edit_url(args.db, args.url, **edit_data)
    elif args.action == 'delete':
        if not args.url:
            print("削除するURLを指定してください。")
            return
        delete_url(args.db, args.url)
    elif args.action == 'list':
        list_urls(args.db)


    elif args.action == 'add_from_json':
        if not args.json_file:
            print("JSONファイルを指定してください。")
            return
        add_urls_from_json(args.db, args.json_file)
    elif args.action == 'export_json':
        if not args.json_file:
            print("エクスポート先のJSONファイルを指定してください。")
            return
        export_to_json(args.db, args.json_file)

if __name__ == "__main__":
    main()

    main()
