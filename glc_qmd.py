#!/usr/bin/env python3
# glc_qmd.py
# Version 1.0.2
# - QMDファイル生成後にquarto renderを実行する機能を追加

import yaml
import sys
import os
from datetime import datetime
from collections import defaultdict
import logging
import argparse
import subprocess

# glc_csr.pyのディレクトリをPYTHONPATHに追加
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from glc_csr import compress_scraping_results
from glc_utils import get_db_connection, sort_key

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_msg_config():
    with open('msg.config', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

MSG_CONFIG = load_msg_config()
TOP_PAGE_FILENAME = MSG_CONFIG['Files']['top_page_filename']

def generate_qmd_content(updated_targets, qmd_name, title, owner):
    qmd_content = MSG_CONFIG['IndividualQuartoContent']['individual_page_yaml'].format(owner=owner, title=title)
    qmd_content += "\n"
    qmd_content += MSG_CONFIG['IndividualQuartoContent']['individual_page_header'].format(owner=owner, title=title)
    qmd_content += "\n\n"

    relevant_updates = [target for target in updated_targets if target['qmd_name'] == qmd_name]
    for target in sorted(relevant_updates, key=sort_key, reverse=True):
        formatted_time = sort_key(target).strftime('%Y年%m月%d日 %H時%M分(日本時間)')
        archive_link = f"[🪦]({target['archive_url']}){{.external target=\"_blank\"}}" if target.get('archive_url') else ""
        qmd_content += f"* {formatted_time}、[{target['owner']}]({target['ownerurl']}){{.external target=\"_blank\"}} の [{target['title']}]({target['url']}){{.external target=\"_blank\"}} が更新されました。{archive_link}\n"

    return qmd_content

def generate_top_page_content(updated_targets, qmd_targets):
    content = MSG_CONFIG['QuartoContent']['top_page_yaml'] + "\n\n"
    content += MSG_CONFIG['QuartoContent']['top_page_header'] + "\n\n"

    grouped_results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for target in updated_targets:
        last_update = sort_key(target)
        grouped_results[last_update.year][last_update.month][last_update.day].append(target)

    for year in sorted(grouped_results.keys(), reverse=True):
        content += f"## {year}年\n\n"
        for month in sorted(grouped_results[year].keys(), reverse=True):
            content += f"### {year}年{month}月\n\n"
            for day in sorted(grouped_results[year][month].keys(), reverse=True):
                content += f"#### {year}年{month}月{day}日\n\n"
                for target in sorted(grouped_results[year][month][day], key=sort_key, reverse=True):
                    formatted_time = sort_key(target).strftime("%Y年%m月%d日 %H時%M分(日本時間)")
                    archive_link = f"[🪦]({target['archive_url']}){{.external target=\"_blank\"}}" if target.get('archive_url') else ""
                    content += f"* {formatted_time}、[{target['owner']}]({target['ownerurl']}){{.external target=\"_blank\"}} の [{target['title']}]({target['url']}){{.external target=\"_blank\"}} が更新されました。{archive_link} ([💾]({target['qmd_name']}.qmd){{.external target=\"_blank\"}})\n"
                content += "\n"

    content += '# 全ての更新履歴\n\n'
    for target in qmd_targets:
        content += f"* [{target['owner']} の {target['title']}]({target['qmd_name']}.qmd){{.external target=\"_blank\"}}\n"

    return content

def render_qmd(qmd_filename):
    try:
        subprocess.run(["quarto", "render", qmd_filename], check=True)
        logger.info(f"{qmd_filename} のレンダリングが完了しました。")
    except subprocess.CalledProcessError as e:
        logger.error(f"{qmd_filename} のレンダリング中にエラーが発生しました: {e}")

def process_qmd_updates(db_name):
    conn = get_db_connection(db_name)
    if conn is None:
        logger.error("データベース接続の取得に失敗しました。")
        return

    try:
        cursor = conn.cursor(dictionary=True)
        
        # 更新されたターゲットを取得
        cursor.execute("SELECT * FROM updated_targets_view")
        updated_targets = cursor.fetchall()

        # QMDターゲットを取得
        cursor.execute("SELECT * FROM qmd_view")
        qmd_targets = cursor.fetchall()

        # 個別のQMDファイルを生成
        for target in qmd_targets:
            qmd_filename = f"{target['qmd_name']}.qmd"
            qmd_content = generate_qmd_content(updated_targets, target['qmd_name'], target['title'], target['owner'])
            with open(qmd_filename, 'w', encoding='utf-8') as f:
                f.write(qmd_content)
            logger.info(f"{qmd_filename} ファイルを生成しました。")
            render_qmd(qmd_filename)

        # トップページを生成
        top_page_content = generate_top_page_content(updated_targets, qmd_targets)
        with open(TOP_PAGE_FILENAME, 'w', encoding='utf-8') as f:
            f.write(top_page_content)
        logger.info(f"{TOP_PAGE_FILENAME} ファイルを生成しました。")
        render_qmd(TOP_PAGE_FILENAME)

    except Exception as e:
        logger.error(f"QMD更新処理中にエラーが発生: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate QMD files for updated targets")
    parser.add_argument("--db", required=True, help="Database name")
    args = parser.parse_args()

    process_qmd_updates(args.db)
