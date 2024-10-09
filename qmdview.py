#!/usr/bin/env python3

import os
import mysql.connector
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

def get_db_connection(db_name):
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=db_name
    )

def get_qmd_names(cursor):
    cursor.execute("SELECT DISTINCT qmd_name FROM scraping_targets")
    return [row[0] for row in cursor.fetchall()]

def check_qmd_views(db_name):
    conn = get_db_connection(db_name)
    cursor = conn.cursor()

    try:
        qmd_names = get_qmd_names(cursor)
        
        print(f"Database: {db_name}")
        print("QMD Views:")
        
        for qmd_name in qmd_names:
            view_name = f"{qmd_name}_view"
            cursor.execute(f"SHOW TABLES LIKE '{view_name}'")
            if cursor.fetchone():
                print(f"\n{view_name}:")
                cursor.execute(f"SELECT * FROM {view_name}")
                results = cursor.fetchall()
                if results:
                    for row in results:
                        print(row)
                else:
                    print("null")
            else:
                print(f"\n{view_name}: Does not exist")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Check QMD views in the database")
    parser.add_argument("--db", required=True, help="Database name")
    args = parser.parse_args()

    check_qmd_views(args.db)
