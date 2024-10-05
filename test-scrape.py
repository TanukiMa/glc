#!/usr/bin/env python3

import argparse
import json
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from glc_utils import get_db_connection, calculate_sha3_512

load_dotenv()

def fetch_and_parse(url, tag, tag_id, tag_class):
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # エンコーディングの確認と変換
        if response.encoding.lower() != 'utf-8':
            content = response.content.decode(response.encoding).encode('utf-8').decode('utf-8')
        else:
            content = response.text
        
        soup = BeautifulSoup(content, 'html.parser')
        
        if tag:
            elements = soup.find_all(tag, id=tag_id or None, class_=tag_class or None)
            parsed_content = ' '.join([element.get_text(strip=True) for element in elements])
        else:
            parsed_content = soup.get_text(strip=True)
        
        return parsed_content
    except Exception as e:
        return f"Error: {str(e)}"

def main():
    parser = argparse.ArgumentParser(description="Scrape URLs with check_lastmodified=FALSE")
    parser.add_argument("--db", required=True, help="Database name")
    args = parser.parse_args()

    conn = get_db_connection(args.db)
    if conn is None:
        return

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT url, tag, tag_id, tag_class
            FROM scraping_targets
            WHERE check_lastmodified = FALSE
        """)
        targets = cursor.fetchall()

        results = {}
        for target in targets:
            content = fetch_and_parse(target['url'], target['tag'], target['tag_id'], target['tag_class'])
            hash_value = calculate_sha3_512(content)
            results[target['url']] = {
                "content": content,
                "hash": hash_value
            }

        print(json.dumps(results, ensure_ascii=False, indent=2))

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
