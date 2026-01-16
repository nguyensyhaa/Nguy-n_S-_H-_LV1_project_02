
import argparse
import asyncio
import sys
import os
import csv
import json
import glob
from .pipeline import TikiPipeline

def cmd_crawl(args):
    """Lệnh cào dữ liệu"""
    input_file = args.input
    if not os.path.exists(input_file):
        print(f"❌ LỖI: Không tìm thấy file input '{input_file}'")
        return

    print(f"🚀 Bắt đầu crawl từ file: {input_file}")
    pipeline = TikiPipeline(input_file=input_file)
    try:
        asyncio.run(pipeline.run())
    except KeyboardInterrupt:
        print("\n⚠️ Đã dừng thủ công (Ctrl+C).")
    except Exception as e:
        print(f"❌ Lỗi không mong muốn: {e}")

def cmd_validate(args):
    """Lệnh kiểm tra input"""
    input_file = args.input
    if not os.path.exists(input_file):
        print(f"❌ LỖI: Không tìm thấy file '{input_file}'")
        return

    print(f"🔍 Đang kiểm tra file: {input_file} ...")
    unique_ids = set()
    duplicates = []
    invalid_ids = []
    total_rows = 0

    try:
        with open(input_file, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            if 'id' not in reader.fieldnames:
                print(f"❌ LỖI: File CSV thiếu cột 'id'. Found: {reader.fieldnames}")
                return

            for row in reader:
                total_rows += 1
                raw_id = row.get('id', '').strip()
                if not raw_id: continue
                if not raw_id.isdigit():
                    invalid_ids.append(raw_id)
                    continue
                
                if raw_id in unique_ids:
                    duplicates.append(raw_id)
                else:
                    unique_ids.add(raw_id)
                    
        print("-" * 30)
        print(f"✅ Hoàn thành kiểm tra!")
        print(f"• Tổng dòng: {total_rows}")
        print(f"• ID hợp lệ: {len(unique_ids)}")
        print(f"• ID trùng lặp: {len(duplicates)}")
        print(f"• ID lỗi format: {len(invalid_ids)}")
        
        if duplicates:
            print(f"⚠️ CẢNH BÁO: Có {len(duplicates)} ID trùng lặp.")
        else:
            print("✨ Dữ liệu sạch, không trùng lặp.")
            
    except Exception as e:
        print(f"❌ Lỗi đọc file: {e}")

def cmd_merge(args):
    """Lệnh gộp file JSON"""
    data_dir = args.data_dir
    output_file = args.output
    
    print(f"⏳ Đang gộp dữ liệu từ '{data_dir}' vào '{output_file}'...")
    all_data = []
    pattern = os.path.join(data_dir, "products_batch_*.json")
    files = glob.glob(pattern)
    
    if not files:
        print(f"⚠️ Không tìm thấy file batch nào trong {data_dir}")
        return

    for f_path in files:
        try:
            with open(f_path, 'r', encoding='utf-8') as f:
                batch_data = json.load(f)
                all_data.extend(batch_data)
        except Exception as e:
            print(f"⚠️ Bỏ qua file lỗi {f_path}: {e}")

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        print(f"✅ ĐÃ XONG! Gộp {len(files)} file thành công.")
        print(f"📁 Tổng cộng: {len(all_data)} sản phẩm.")
    except Exception as e:
        print(f"❌ Lỗi ghi file output: {e}")

def main():
    parser = argparse.ArgumentParser(description="Tiki Scraper Tool - High Performance Crawler")
    subparsers = parser.add_subparsers(dest="command", help="Lệnh cần chạy")

    # Command: crawl
    crawl_parser = subparsers.add_parser("crawl", help="Bắt đầu cào dữ liệu")
    crawl_parser.add_argument("--input", "-i", required=True, help="Đường dẫn file CSV input")
    crawl_parser.set_defaults(func=cmd_crawl)

    # Command: validate
    validate_parser = subparsers.add_parser("validate", help="Kiểm tra file input CSV")
    validate_parser.add_argument("--input", "-i", required=True, help="Đường dẫn file CSV input")
    validate_parser.set_defaults(func=cmd_validate)

    # Command: merge
    merge_parser = subparsers.add_parser("merge", help="Gộp các file JSON thành 1 file lớn")
    merge_parser.add_argument("--data-dir", "-d", default="data", help="Thư mục chứa file batch (default: data)")
    merge_parser.add_argument("--output", "-o", default="all_products.json", help="File output (default: all_products.json)")
    merge_parser.set_defaults(func=cmd_merge)

    args = parser.parse_args()
    
    if args.command:
        args.func(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
