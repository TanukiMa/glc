#!/usr/bin/env python3

import argparse
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

def get_db_connection(db_name=None):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'goudge-tc01-sid'),
            database=db_name,
            user=os.getenv('DB_USER', 'mguuji'),
            password=os.getenv('DB_PASSWORD')
        )
        return connection
    except Error as e:
        print(f"データベース接続エラー: {e}")
        return None

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
    except Error as e:
        print(f"クエリ実行エラー: {e}")
    finally:
        cursor.close()

def create_database(db_name):
    connection = get_db_connection()
    if connection is not None:
        execute_query(connection, f"CREATE DATABASE IF NOT EXISTS {db_name}")
        connection.close()
        print(f"データベース '{db_name}' を作成しました。")
    else:
        print("データベース接続に失敗しました。")

def create_schema(db_name, schema_file):
    connection = get_db_connection(db_name)
    if connection is None:
        return

    with open(schema_file, 'r') as file:
        schema_sql = file.read()
    
    for query in schema_sql.split(';'):
        if query.strip():
            execute_query(connection, query)
    
    connection.close()
    print(f"スキーマを作成しました。")

def main():
    parser = argparse.ArgumentParser(description="Create GLC database and schema")
    parser.add_argument("--db", required=True, help="New database name")
    parser.add_argument("--schema", required=True, help="Schema definition file")
    args = parser.parse_args()

    create_database(args.db)
    create_schema(args.db, args.schema)

if __name__ == "__main__":
    main()
