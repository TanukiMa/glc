#!/usr/bin/env python3
# glc_qmd.py
# Version 1.0.0
# - QMDファイル生成機能を担当

import yaml
import logging
from collections import defaultdict
from glc_utils import get_db_connection, sort_key

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
    content += MSG_CONFIG['QuartoContent']['top_page_header'] + "\n"

    grouped_results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for target in updated_targets:
        last_update = sort_key(target)
        grouped_results[last_update.year][last_update.month][last_update.day].append(target)

    for year in sorted(grouped_results.keys(), reverse=True):
        content += f'\n## {year}年\n\n'
        for month in sorted(grouped_results[year].keys(), reverse=True):
            content += f'### {year}年{month}月\n\n'
            for day in sorted(grouped_results[year][month].keys(), reverse=True):
                content += f'#### {year}年{month}月{day}日\n\n'
                for target in sorted(grouped_results[year][month][day], key=sort_key, reverse=True):
                    formatted_time = sort_key(target).strftime("%Y年%m月%d日 %H時%M分(日本時間)")
                    archive_link = f"[🪦]({target['archive_url']}){{.external target=\"_blank\"}}" if target.get('archive_url') else ""
                    content += f"* {formatted_time}、[{target['owner']}]({target['ownerurl']}){{.external target=\"_blank\"}} の [{target['title']}]({target['url']}){{.external target=\"_blank\"}} が更新されました。{archive_link} ([💾]({target['qmd_name']}.qmd){{.external target=\"_blank\"}})\n"

    content += '\n# 全ての更新履歴\n\n'
    for target in qmd_targets:
        content += f"* [{target['owner']} の {target['title']}]({target['qmd_name']}.qmd){{.external target=\"_blank\"}}\n"

    return content

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

        if not updated_targets:
            logger.info("更新されたターゲットはありません。QMDファイルは生成しません。")
            return

        # QMDターゲットを取得
        cursor.execute("SELECT * FROM qmd_view")
        qmd_targets = cursor.fetchall()

        # 個別のQMDファイルを生成
        for target in qmd_targets:
            qmd_content = generate_qmd_content(updated_targets, target['qmd_name'], target['title'], target['owner'])
            if qmd_content:
                with open(f"{target['qmd_name']}.qmd", 'w', encoding='utf-8') as f:
                    f.write(qmd_content)
                logger.info(f"{target['qmd_name']}.qmd ファイルを生成しました。")

        # トップページを生成
        top_page_content = generate_top_page_content(updated_targets, qmd_targets)
        with open(TOP_PAGE_FILENAME, 'w', encoding='utf-8') as f:
            f.write(top_page_content)
        logger.info(f"{TOP_PAGE_FILENAME} ファイルを生成しました。")

    except Exception as e:
        logger.error(f"QMD更新処理中にエラーが発生: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate QMD files for updated targets")
    parser.add_argument("--db", required=True, help="Database name")
    args = parser.parse_args()

    process_qmd_updates(args.db)
