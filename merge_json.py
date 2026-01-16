
import json
import os
import glob
import time

OUTPUT_FILE = "all_products.json"
DATA_DIR = "data"

def merge_results():
    print("⏳ Đang gộp dữ liệu...")
    all_data = []
    
    # Lấy danh sách tất cả file json trong folder data
    files = glob.glob(os.path.join(DATA_DIR, "products_batch_*.json"))
    
    for f_path in files:
        try:
            with open(f_path, 'r', encoding='utf-8') as f:
                batch_data = json.load(f)
                all_data.extend(batch_data)
        except Exception as e:
            print(f"⚠️ Lỗi đọc file {f_path}: {e}")

    # Ghi ra file tổng
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ ĐÃ XONG! Gộp {len(files)} file thành công.")
    print(f"📁 Tổng cộng: {len(all_data)} sản phẩm.")
    print(f"👉 File kết quả: {os.path.abspath(OUTPUT_FILE)}")

if __name__ == "__main__":
    merge_results()
