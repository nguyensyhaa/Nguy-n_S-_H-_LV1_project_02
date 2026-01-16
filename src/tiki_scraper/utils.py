
import logging
import os
import re
from bs4 import BeautifulSoup

def setup_logger(log_dir="logs"):
    """
    Thiết lập cấu hình logging chuyên nghiệp.
    Tạo ra 2 file log: 
    - application.log: Chứa toàn bộ info vận hành
    - error.log: Chỉ chứa lỗi để dễ trace
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Logger chung
    logger = logging.getLogger("TikiScraper")
    logger.setLevel(logging.INFO)
    
    # Formatter chuẩn
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Handler ghi file info
    info_handler = logging.FileHandler(os.path.join(log_dir, "application.log"), encoding='utf-8')
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(formatter)

    # Handler ghi file error
    error_handler = logging.FileHandler(os.path.join(log_dir, "error.log"), encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    # Console Handler (in ra màn hình)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(info_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    
    return logger

def clean_description(html_content):
    """
    Loại bỏ các thẻ HTML để lấy text thuần.
    Chuẩn hóa khoảng trắng.
    """
    if not html_content:
        return ""
    
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        # Lấy text và strip khoảng trắng thừa đầu đuôi
        text = soup.get_text(separator=' ', strip=True) 
        # Loại bỏ các khoảng trắng kép (double spaces)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    except Exception:
        return str(html_content)

def get_product_image_url(product_data):
    """Trích xuất URL ảnh Thumbnail chuẩn"""
    try:
        # Ưu tiên lấy ảnh thumbnail chính
        return product_data.get('thumbnail_url', '') or product_data.get('images', [{}])[0].get('base_url', '')
    except Exception:
        return ""
