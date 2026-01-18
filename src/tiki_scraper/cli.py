
import argparse
import asyncio
import os
from .pipelines.crawl_pipeline import TikiPipeline
from .etl.load import load_data_to_postgres
from .utils.logger import setup_logger
from .config.settings import LOG_DIR, DATA_DIR

def cmd_crawl(args):
    """Lệnh chạy Crawler"""
    pipeline = TikiPipeline(input_file=args.input, output_dir=args.output, log_dir=args.log_dir)
    try:
        asyncio.run(pipeline.run())
    except KeyboardInterrupt:
        print("\n⚠️ User Interrupted (Ctrl+C). Exiting...")

def cmd_retry(args):
    """Lệnh Retry các failed IDs"""
    log_file = args.log_file
    if not os.path.exists(log_file):
        print(f"❌ Log file not found: {log_file}")
        return

    failed_ids = []
    with open(log_file, 'r') as f:
        failed_ids = [line.strip() for line in f if line.strip()]
    
    if not failed_ids:
        print("✅ Không có ID lỗi để retry!")
        return

    print(f"🔄 Đang retry {len(failed_ids)} sản phẩm (chế độ chậm)...")
    
    # Tạo file tạm
    temp_input = "temp_retry_input.csv"
    import pandas as pd
    pd.DataFrame({'id': failed_ids}).to_csv(temp_input, index=False)
    
    # retry_mode=True: Delay lâu hơn (2s, 4s, 6s) và ít concurrent hơn
    pipeline = TikiPipeline(input_file=temp_input, output_dir=DATA_DIR, log_dir=LOG_DIR, retry_mode=True)
    try:
        asyncio.run(pipeline.run())
    finally:
        if os.path.exists(temp_input):
            os.remove(temp_input)

def cmd_ingest(args):
    """Lệnh Ingest vào DB"""
    data_dir = args.data_dir
    if not os.path.exists(data_dir):
        print(f"❌ Data directory not found: {data_dir}")
        return
        
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.json')]
    print(f"📦 Tìm thấy {len(files)} file JSON. Bắt đầu nạp vào Postgres...")
    
    for f in files:
        load_data_to_postgres(f)
        
    print("✅ Hoàn tất Ingest!")

def main():
    parser = argparse.ArgumentParser(description="Tiki Scraper Tool v2.0 (Refactored)")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: crawl
    crawl_parser = subparsers.add_parser("crawl", help="Start scraping from CSV")
    crawl_parser.add_argument("--input", required=True, help="Path to input CSV file")
    crawl_parser.add_argument("--output", default="data", help="Output directory")
    crawl_parser.add_argument("--log-dir", default="logs", help="Log directory")

    # Command: retry
    retry_parser = subparsers.add_parser("retry", help="Retry failed IDs from logs")
    retry_parser.add_argument("--log-file", default="logs/failed_products.txt", help="Path to failed IDs log")
    
    # Command: ingest
    ingest_parser = subparsers.add_parser("ingest", help="Ingest JSON data to PostgreSQL")
    ingest_parser.add_argument("--data-dir", default="data", help="Directory containing JSON files")

    args = parser.parse_args()

    if args.command == "crawl":
        cmd_crawl(args)
    elif args.command == "retry":
        cmd_retry(args)
    elif args.command == "ingest":
        cmd_ingest(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
