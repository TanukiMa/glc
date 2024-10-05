#!/usr/bin/env python3
# update_glc_files.py

import re

def update_glc_utils():
    with open('glc_utils.py', 'r', encoding='utf-8') as file:
        content = file.read()

    # scrape_content 関数を更新
    new_scrape_content = """
def scrape_content(content, tag, tag_id, tag_class):
    soup = BeautifulSoup(content, 'html.parser')
    if tag_id:
        element = soup.find(tag, id=tag_id)
    elif tag_class:
        # 複数のクラスに対応
        classes = tag_class.split()
        element = soup.find(tag, class_=lambda x: x and all(c in x.split() for c in classes))
    else:
        element = soup.find(tag)

    if element:
        return element.text.strip()
    else:
        print(f"要素が見つかりません: tag={tag}, tag_id={tag_id}, tag_class={tag_class}")
        return None
"""
    content = re.sub(r'def scrape_content.*?return None\n', new_scrape_content, content, flags=re.DOTALL)

    with open('glc_utils.py', 'w', encoding='utf-8') as file:
        file.write(content)

    print("glc_utils.py が更新されました。")

def update_urledit():
    with open('urledit.py', 'r', encoding='utf-8') as file:
        content = file.read()

    # バージョン番号を更新
    content = re.sub(r'# Version \d+\.\d+\.\d+', '# Version 1.11.0', content)

    # 変更内容を追加
    changes = """# Changes:
# - Updated delete_url function to use URL instead of ID
# - Added cascade delete for related records in other tables
# - Updated edit_url function to handle multiple classes in tag_class
# - Removed add_from_json and export_json actions"""
    content = re.sub(r'# Changes:.*?(?=\n\n)', changes, content, flags=re.DOTALL)

    # edit_url 関数を更新
    new_edit_url = """
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
"""
    content = re.sub(r'def edit_url.*?conn\.close\(\)', new_edit_url, content, flags=re.DOTALL)

    # main 関数から add_from_json と export_json アクションを削除
    new_main = """
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
"""
    content = re.sub(r'def main\(\):.*?list_urls\(args\.db\)', new_main, content, flags=re.DOTALL)

    # add_urls_from_json と export_to_json 関数を削除
    content = re.sub(r'def add_urls_from_json.*?print\("JSONファイルからのURL追加が完了しました。"\)\n', '', content, flags=re.DOTALL)
    content = re.sub(r'def export_to_json.*?conn\.close\(\)\n', '', content, flags=re.DOTALL)

    with open('urledit.py', 'w', encoding='utf-8') as file:
        file.write(content)

    print("urledit.py が更新されました。")

def main():
    update_glc_utils()
    update_urledit()
    print("全てのファイルが正常に更新されました。")

if __name__ == "__main__":
    main()
