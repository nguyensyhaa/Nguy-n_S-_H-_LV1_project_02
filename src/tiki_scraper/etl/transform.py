
import re
from bs4 import BeautifulSoup

def clean_description(html_content):
    """
    Loại bỏ các thẻ HTML để lấy text thuần.
    Chuẩn hóa khoảng trắng.
    """
    if not html_content:
        return ""
    
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        text = soup.get_text(separator=' ', strip=True) 
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    except Exception:
        return str(html_content)

def get_product_image_url(product_data):
    """Trích xuất URL ảnh Thumbnail chuẩn"""
    try:
        return product_data.get('thumbnail_url', '') or product_data.get('images', [{}])[0].get('base_url', '')
    except Exception:
        return ""
