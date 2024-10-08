#!/usr/bin/env python3
# glc.py
# Version 3.5.0
# - 処理順序の変更: glc_csr.py を3回実行し、glc_diff.py, glc_msg.py, glc_qmd.py の実行順序を調整

import os
import sys
import argparse
import logging
from dotenv import load_dotenv
from glc_utils import get_db_connection, is_within_time_range
from glc_url import process_urls
from glc_diff import check_updates
from glc_spn import archive_updated_urls
from glc_msg import process_updates
from glc_qmd import process_qmd_updates
from glc_csr import compress_scraping_results

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def set_log_level(debug):
    logger.setLevel(logging.DEBUG if debug else logging.INFO)



def process_targets(db_name, force=False, no_toot=False, debug=False):
    if not force and not is_within_time_range():
        logger.warning("Execution outside local time 7:00-19:00 requires --force option.")
        return

    try:
        conn = get_db_connection(db_name)
        if conn is None:
            raise Exception("データベース接続の取得に失敗しました。")

        # 1回目: スクレイピング結果の圧縮を実行
        logger.info("スクレイピング結果の初期圧縮を開始します。")
        compress_scraping_results(db_name)
        logger.info("スクレイピング結果の初期圧縮が完了しました。")

        # URLの処理
        process_urls(conn)

        # 更新の確認
        updated_targets = check_updates(conn, debug)

        # 2回目: スクレイピング結果の圧縮を実行
        logger.info("スクレイピング結果の中間圧縮を開始します。")
        compress_scraping_results(db_name)
        logger.info("スクレイピング結果の中間圧縮が完了しました。")

        if updated_targets:
            logger.info(f"更新されたターゲット: {updated_targets}")
            
            # アーカイブの作成
            archive_updated_urls(db_name, updated_targets)

            # メッセージの送信
            updated_qmd_names = [target['qmd_name'] for target in updated_targets if 'qmd_name' in target]
            if updated_qmd_names:
                process_updates(db_name, updated_qmd_names, no_toot)
            else:
                logger.warning("更新されたターゲットに qmd_name が含まれていません。")

        # QMDファイルの生成
        process_qmd_updates(db_name)

        # 3回目: スクレイピング結果の圧縮を実行
        logger.info("スクレイピング結果の最終圧縮を開始します。")
        compress_scraping_results(db_name)
        logger.info("スクレイピング結果の最終圧縮が完了しました。")

    except Exception as e:
        logger.error(f"処理中に予期せぬエラーが発生しました: {e}")
    finally:
        if conn:
            conn.close()
    
    

def main():
    parser = argparse.ArgumentParser(description="Webページの更新をチェックし、更新があればデータベースに記録します。")
    parser.add_argument("--force", action="store_true", help="現地時間7:00-19:00以外でも実行します。")
    parser.add_argument("--no-toot", action="store_true", help="メッセージの送信を抑制します。")
    parser.add_argument("--db", required=True, help="使用するデータベース名")
    parser.add_argument("--debug", action="store_true", help="デバッグモードを有効にします")
    args = parser.parse_args()

    set_log_level(args.debug)
    logger.debug("Starting main function")
    process_targets(args.db, args.force, args.no_toot, args.debug)
    logger.debug("Finished main function")

if __name__ == "__main__":
    main()
