
import asyncio
import os
import sys
from src.pipeline import TikiPipeline

# Cấu hình absolute path cho file input
INPUT_CSV = "/Users/syha/Downloads/products-0-200000.csv"

def main():
    # Kiểm tra file input tồn tại
    if not os.path.exists(INPUT_CSV):
        print(f"LỖI: Không tìm thấy file {INPUT_CSV}")
        return

    pipeline = TikiPipeline(input_file=INPUT_CSV)
    
    try:
        # Chạy event loop
        asyncio.run(pipeline.run())
    except KeyboardInterrupt:
        print("\n\n⚠️ Đã dừng thủ công (Ctrl+C).")
        print("Dữ liệu đã lưu vẫn an toàn. Bạn có thể chạy lại lệnh này để tiếp tục (Resume).")
    except Exception as e:
        print(f"\n❌ Lỗi không mong muốn: {str(e)}")

if __name__ == "__main__":
    main()
