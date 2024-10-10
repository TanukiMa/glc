#!/usr/bin/env python3
# create_qmd_name_view.py

import os
import sys
import argparse
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

def get_db_connection(db_name):
    load_dotenv()
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=db_name
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"データベース接続エラー: {e}", file=sys.stderr)
    return None

def create_qmd_name_views(db_name):
    connection = get_db_connection(db_name)
    if not connection:
        return

    cursor = connection.cursor()
    try:
        # qmd_nameの一覧を取得
        cursor.execute("SELECT DISTINCT qmd_name FROM scraping_targets")
        qmd_names = [row[0] for row in cursor.fetchall()]

        for qmd_name in qmd_names:
            view_name = f"{qmd_name}_view"
            
            # 既存のビューを削除
            cursor.execute(f"DROP VIEW IF EXISTS `{view_name}`")

            # 新しいビューを作成
            view_definition = f"""
            CREATE VIEW `{view_name}` AS
            SELECT 
                `updated_targets_view`.`qmd_name`,
                `updated_targets_view`.`owner`,
                `updated_targets_view`.`ownerurl`,
                `updated_targets_view`.`title`,
                `updated_targets_view`.`url`,
                `updated_targets_view`.`last_update`
            FROM 
                `updated_targets_view`
            WHERE 
                `updated_targets_view`.`qmd_name` = '{qmd_name}'
            """
            cursor.execute(view_definition)

        connection.commit()
        print("全てのqmd_name_viewが正常に作成されました。")

    except Error as e:
        print(f"ビューの作成中にエラーが発生しました: {e}", file=sys.stderr)
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create qmd_name views in the specified database")
    parser.add_argument("--db", required=True, help="Database name")
    args = parser.parse_args()

    create_qmd_name_views(args.db)
