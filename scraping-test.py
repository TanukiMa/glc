#!/usr/bin/env python3
# test-scraping.py

import json
import argparse
import requests
from glc_utils import scrape_content, fetch_url_content

def test_scraping(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)

    for item in data:
        url = item['url']
        tag = item['tag']
        tag_id = item['tag_id']
        tag_class = item['tag_class']

        print(f"Testing URL: {url}")
        print(f"Tag: {tag}, Tag ID: {tag_id}, Tag Class: {tag_class}")

        content = fetch_url_content(url)
        if content is None:
            print("Failed to fetch content")
            continue

        scraped_content = scrape_content(content, tag, tag_id, tag_class)
        if scraped_content is None:
            print("Failed to scrape content")
        else:
            print("Scraped content:")
            print(scraped_content)
            print(f"Total length of scraped content: {len(scraped_content)} characters")

        print("\n" + "="*50 + "\n")

def main():
    parser = argparse.ArgumentParser(description="Test scraping using glc2_utils")
    parser.add_argument("--json_file", required=True, help="JSON file containing URL and scraping information")
    args = parser.parse_args()

    test_scraping(args.json_file)

if __name__ == "__main__":
    main()
