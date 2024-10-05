#!/usr/bin/env python3
# glc_qmd.py
# Version 2.0.0
# Quartoファイル生成のデバッグ用スクリプト

import os
import sys
import argparse
import json
from datetime import datetime
import pytz
from dotenv import load_dotenv
import os
from collections import defaultdict
from glc_utils import get_db_connection
import mysql.connector

def get_db_connection(db_name):
    load_dotenv()  # .env ファイルから環境変数を読み込む
    try:
        conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=db_name
        )
        return conn
    except mysql.connector.Error as e:
        print(f"データベース接続エラー: {e}", file=sys.stderr)
        return None

def get_updated_targets(db_name):
    """updated_targets_view から更新されたターゲット情報を取得する"""
    conn = get_db_connection(db_name)
    if conn is None:
        return None

    cursor = conn.cursor(dictionary=True)
    try:
        query = """
        SELECT utv.*, st.qmd_name
        FROM updated_targets_view utv
        JOIN scraping_targets st ON utv.id = st.id
        ORDER BY utv.last_update DESC
        """
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"更新されたターゲット情報取得エラー: {e}", file=sys.stderr)
        return None
    finally:
        cursor.close()
        conn.close()

def get_qmd_targets(db_name):
    """qmd_view からターゲット情報を取得する"""
    conn = get_db_connection(db_name)
    if conn is None:
        return None

    cursor = conn.cursor(dictionary=True)
    try:
        query = "SELECT * FROM qmd_view"
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"QMDターゲット情報取得エラー: {e}", file=sys.stderr)
        return None
    finally:
        cursor.close()
        conn.close()

def generate_quarto_content(updated_targets, qmd_targets):
    """Quartoコンテンツを生成する"""
    quarto_content = '''---
title: 魔狸アンテナ
format:
  html:
    toc: true
    toc-expand: true
    embed-resources: true
lang: ja
---

# 最新の更新情報

'''

    jst = pytz.timezone('Asia/Tokyo')
    grouped_results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for target in updated_targets:
        last_update_jst = target['last_update'].astimezone(jst)
        year = last_update_jst.year
        month = last_update_jst.month
        day = last_update_jst.day
        grouped_results[year][month][day].append(target)

    for year in sorted(grouped_results.keys(), reverse=True):
        quarto_content += f'\n## {year}年\n\n'
        for month in sorted(grouped_results[year].keys(), reverse=True):
            quarto_content += f'\n### {year}年{month}月\n\n'
            for day in sorted(grouped_results[year][month].keys(), reverse=True):
                quarto_content += f'\n#### {year}年{month}月{day}日\n\n'
                for target in sorted(grouped_results[year][month][day], key=lambda x: x['last_update'], reverse=True):
                    formatted_time = target['last_update'].astimezone(jst).strftime("%Y年%m月%d日、%H:%M(JST)")
                    archive_link = f"[🪦]({target['archive_url']}){{.external target=\"_blank\"}}" if target['archive_url'] else ""
                    quarto_content += f"* {formatted_time}、[{target['owner']}]({target['ownerurl']}){{.external target=\"_blank\"}} の [{target['title']}]({target['url']}){{.external target=\"_blank\"}} が更新されました。{archive_link} ([💾]({target['qmd_name']}.qmd){{.external target=\"_blank\"}})\n"

    quarto_content += '\n# 全ての更新履歴\n\n'
    for target in qmd_targets:
        quarto_content += f"* [{target['owner']} の {target['title']}]({target['qmd_name']}.qmd){{.external target=\"_blank\"}}\n"

    return quarto_content

def generate_qmd_from_db(db_name):
    """データベースからデータを取得し、Quartoファイル(glc.qmd)を生成する"""
    updated_targets = get_updated_targets(db_name)
    if not updated_targets:
        print("更新されたターゲットの情報が取得できませんでした。", file=sys.stderr)
        return

    qmd_targets = get_qmd_targets(db_name)
    if not qmd_targets:
        print("QMDターゲットの情報が取得できませんでした。", file=sys.stderr)
        return

    quarto_content = generate_quarto_content(updated_targets, qmd_targets)

    try:
        with open('glc.qmd', 'w', encoding='utf-8') as f:
            f.write(quarto_content)
        print("glc.qmd ファイルが生成されました。", file=sys.stderr)
    except IOError as e:
        print(f"glc.qmd ファイルの書き込みエラー: {e}", file=sys.stderr)

def main():
    load_dotenv()  # .env ファイルから環境変数を読み込む
    print(f"DB_HOST: {os.getenv('DB_HOST')}")
    print(f"DB_USER: {os.getenv('DB_USER')}")
    print(f"DB_PASSWORD: {'*' * len(os.getenv('DB_PASSWORD')) if os.getenv('DB_PASSWORD') else 'Not set'}")
    parser = argparse.ArgumentParser(description="Generate Quarto output for GLC")
    parser.add_argument("--db", required=True, help="Database name")
    args = parser.parse_args()

    generate_qmd_from_db(args.db)

if __name__ == "__main__":
    main()
