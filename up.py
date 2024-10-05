import re

def update_glc_utils():
    with open('glc_utils.py', 'r', encoding='utf-8') as file:
        content = file.read()

    new_scrape_content = """
def scrape_content(content, tag, tag_id, tag_class):
    soup = BeautifulSoup(content, 'html.parser')
    if tag_id:
        elements = soup.find_all(tag, id=tag_id)
    elif tag_class:
        classes = tag_class.split()
        elements = soup.find_all(tag, class_=lambda x: x and all(c in x.split() for c in classes))
    else:
        elements = soup.find_all(tag)

    if elements:
        return '\\n'.join([element.text.strip() for element in elements])
    else:
        print(f"要素が見つかりません: tag={tag}, tag_id={tag_id}, tag_class={tag_class}")
        print(f"ページの内容（最初の500文字）: {soup.prettify()[:500]}...")
        return None
"""
    content = re.sub(r'def scrape_content.*?return None\n', new_scrape_content, content, flags=re.DOTALL)

    with open('glc_utils.py', 'w', encoding='utf-8') as file:
        file.write(content)

    print("glc_utils.py が更新されました。")

if __name__ == "__main__":
    update_glc_utils()
