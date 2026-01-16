import csv
import os
import sys

# Cấu hình đường dẫn
INPUT_FILE = "/Users/syha/Downloads/products-0-200000.csv"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "input_validation.log")

def validate_input():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        
    print(f"Bắt đầu kiểm tra file: {INPUT_FILE}")
    
    unique_ids = set()
    duplicates = []
    invalid_ids = []
    total_rows = 0
    
    try:
        with open(INPUT_FILE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            # Kiểm tra header
            if 'id' not in reader.fieldnames:
                print("LỖI: File CSV không có cột 'id'. Header tìm thấy: ", reader.fieldnames)
                return

            for row in reader:
                total_rows += 1
                raw_id = row.get('id', '').strip()
                
                # Check rỗng
                if not raw_id:
                    continue
                    
                # Check format số
                if not raw_id.isdigit():
                    invalid_ids.append(raw_id)
                    continue
                    
                # Check duplicate
                if raw_id in unique_ids:
                    duplicates.append(raw_id)
                else:
                    unique_ids.add(raw_id)

    except Exception as e:
        print(f"LỖI FATAL khi đọc file: {str(e)}")
        return

    # Ghi log kết quả
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write("=== KẾT QUẢ INPUT VALIDATION ===\n")
        f.write(f"File nguồn: {INPUT_FILE}\n")
        f.write(f"Tổng số dòng dữ liệu: {total_rows}\n")
        f.write(f"Số ID hợp lệ (Unique): {len(unique_ids)}\n")
        f.write(f"Số ID trùng lặp: {len(duplicates)}\n")
        f.write(f"Số ID lỗi định dạng: {len(invalid_ids)}\n")
        
        if duplicates:
            f.write("\n--- DANH SÁCH ID TRÙNG LẶP ---\n")
            # Ghi tối đa 100 id trùng lặp đại diện
            f.write(", ".join(duplicates[:100])) 
            if len(duplicates) > 100: f.write("...\n")
            
        if invalid_ids:
            f.write("\n--- DANH SÁCH ID LỖI ---\n")
            f.write(", ".join(invalid_ids[:100]))
            
    # In ra terminal
    print("-" * 30)
    print(f"✅ Hoàn thành kiểm tra!")
    print(f"Tổng dòng: {total_rows}")
    print(f"Unique Valid IDs: {len(unique_ids)}")
    print(f"Duplicate IDs: {len(duplicates)}")
    print(f"Invalid IDs: {len(invalid_ids)}")
    if len(duplicates) > 0:
        print(f"⚠️ CẢNH BÁO: Phát hiện {len(duplicates)} ID trùng lặp. Hệ thống sẽ tự động loại bỏ duplicate khi crawl.")
    else:
        print("Tuyệt vời! Dữ liệu sạch không có duplicate.")
    
    if len(invalid_ids) > 0:
        print(f"⚠️ CẢNH BÁO: Phát hiện {len(invalid_ids)} ID sai định dạng.")
        
    print("-" * 30)
    print(f"Chi tiết log đã lưu tại: {os.path.abspath(LOG_FILE)}")

if __name__ == "__main__":
    validate_input()
