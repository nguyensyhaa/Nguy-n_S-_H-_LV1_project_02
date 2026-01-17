
import argparse
import asyncio
import sys
import os
import csv
import json
import glob
from .pipeline import TikiPipeline
from .etl import run_etl_pipeline
import logging

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

def cmd_ingest(args):
    """Lệnh Ingest dữ liệu vào Postgres"""
    data_dir = args.data_dir
    pattern = os.path.join(data_dir, "products_batch_*.json")
    files = glob.glob(pattern)
    
    if not files:
        print(f"⚠️ Không tìm thấy file batch nào trong {data_dir}")
        return

    print(f"🚀 Bắt đầu Ingest {len(files)} files vào Database...")
    
    # Setup basic console logging for the user to see progress
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    for i, f_path in enumerate(files, 1):
        print(f"[{i}/{len(files)}] Processing {os.path.basename(f_path)}...")
        try:
            run_etl_pipeline(f_path)
        except Exception as e:
             print(f"❌ Failed to ingest {f_path}: {e}")
    
    print("✅ Ingest hoàn tất.")

def cmd_retry(args):
    """Lệnh thử lại các ID bị lỗi (404/Failed)"""
    log_file = args.log_file
    if not os.path.exists(log_file):
        print(f"❌ LỖI: Không tìm thấy file log '{log_file}'")
        return

    print(f"🔄 Đang đọc ID lỗi từ: {log_file} ...")
    retry_ids = []
    try:
        with open(log_file, 'r') as f:
            for line in f:
                clean_id = line.strip()
                if clean_id.isdigit():
                    retry_ids.append(clean_id)
    except Exception as e:
        print(f"❌ Lỗi đọc file log: {e}")
        return

    if not retry_ids:
        print("⚠️ Không tìm thấy ID nào để retry.")
        return

    print(f"🔥 Tìm thấy {len(retry_ids)} ID cần thử lại.")
    
    # Tạo file input tạm thời
    temp_input = "temp_retry_input.csv"
    try:
        with open(temp_input, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id'])
            for rid in retry_ids:
                writer.writerow([rid])
        
        print(f"📝 Đã tạo file input tạm: {temp_input}")
        print("-" * 40)
        
        # Chạy Pipeline
        pipeline = TikiPipeline(input_file=temp_input)
        asyncio.run(pipeline.run())
        
    except KeyboardInterrupt:
        print("\n⚠️ Đã dừng thủ công (Ctrl+C).")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        if os.path.exists(temp_input):
            os.remove(temp_input)
            print(f"🗑️ Đã xóa file tạm: {temp_input}")


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

    # Command: ingest
    ingest_parser = subparsers.add_parser("ingest", help="Ingest dữ liệu JSON vào Postgres")
    ingest_parser.add_argument("--data-dir", "-d", default="data", help="Thư mục chứa file batch (default: data)")
    ingest_parser.set_defaults(func=cmd_ingest)

    # Command: retry
    retry_parser = subparsers.add_parser("retry", help="Thử lại các ID bị lỗi")
    retry_parser.add_argument("--log-file", "-l", default="logs/failed_products.txt", help="File chứa danh sách ID lỗi (default: logs/failed_products.txt)")
    retry_parser.set_defaults(func=cmd_retry)

    args = parser.parse_args()
    
    if args.command:
        args.func(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
