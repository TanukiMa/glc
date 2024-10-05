import json
import sys

def process_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # idの欠番を詰める
    new_id = 1
    for item in data:
        item['id'] = new_id
        new_id += 1

    for item in data:
        # qmd_name を削除
        if 'qmd_name' in item:
            del item['qmd_name']

        # check_lastmodified を true/false に変換
        if 'check_lastmodified' in item:
            item['check_lastmodified'] = True if item['check_lastmodified'] == 1 else False

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python process_json.py <json_file_path>")
        sys.exit(1)

    file_path = sys.argv[1]
    process_json(file_path)
    print(f"{file_path} の処理が完了しました。")

