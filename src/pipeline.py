
import asyncio
import json
import os
import aiohttp
import pandas as pd
from .crawler import TikiFetcher
from .utils import setup_logger

class TikiPipeline:
    def __init__(self, input_file, output_dir="data", log_dir="logs"):
        self.input_file = input_file
        self.output_dir = output_dir
        self.log_dir = log_dir
        
        # Tạo thư mục output nếu chưa có
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        self.logger = setup_logger(log_dir)
        self.fetcher = TikiFetcher(self.logger)
        self.batch_size = 1000

    def get_completed_ids(self):
        """
        Quét thư mục data xác định (Resume) các ID đã tải xong.
        """
        completed_ids = set()
        if not os.path.exists(self.output_dir):
            return completed_ids
            
        files = [f for f in os.listdir(self.output_dir) if f.endswith('.json')]
        for file in files:
            path = os.path.join(self.output_dir, file)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for item in data:
                        completed_ids.add(str(item['id'])) # Lưu dạng string để so sánh nhất quán
            except Exception:
                continue
                
        self.logger.info(f"🔄 RESUME: Tìm thấy {len(completed_ids)} sản phẩm đã tải trước đó.")
        return completed_ids

    def load_pending_ids(self):
        """
        Đọc file CSV và lọc ra các ID chưa tải.
        """
        try:
            df = pd.read_csv(self.input_file, dtype={'id': str})
            all_ids = set(df['id'].dropna().unique())
            completed_ids = self.get_completed_ids()
            
            pending_ids = list(all_ids - completed_ids)
            self.logger.info(f"Tổng ID: {len(all_ids)} | Đã xong: {len(completed_ids)} | Còn lại: {len(pending_ids)}")
            return pending_ids
        except Exception as e:
            self.logger.critical(f"FATAL: Không thể đọc file input CSV: {str(e)}")
            return []

    async def process_batch(self, session, batch_ids, batch_index):
        """
        Xử lý song song một lô (batch) 1000 IDs.
        """
        tasks = [self.fetcher.fetch_product(session, pid) for pid in batch_ids]
        results = await asyncio.gather(*tasks)
        
        # Lọc kết quả None (do lỗi 404 hoặc max retries)
        valid_results = [r for r in results if r is not None]
        
        if valid_results:
            self.save_batch(valid_results, batch_index)
            
        return len(valid_results)

    def save_batch(self, data, batch_index):
        """
        Lưu kết quả ra file JSON.
        """
        filename = f"products_batch_{batch_index}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"💾 Đã lưu batch {batch_index}: {len(data)} sản phẩm -> {filename}")
        except Exception as e:
            self.logger.error(f"WRITE ERROR: Không thể lưu file {filename}: {str(e)}")

    async def run(self):
        """
        Hàm chính điều phối toàn bộ quy trình.
        """
        pending_ids = self.load_pending_ids()
        if not pending_ids:
            self.logger.info("🎉 Tất cả dữ liệu đã được tải xong!")
            return

        total_pending = len(pending_ids)
        self.logger.info("🚀 Bắt đầu tiến trình crawling...")

        async with aiohttp.ClientSession() as session:
            # Chia nhỏ thành các batch
            for i in range(0, total_pending, self.batch_size):
                batch_ids = pending_ids[i : i + self.batch_size]
                batch_index = (i // self.batch_size) + 1 # Đánh số batch tương đối cho lần chạy này
                
                # Để tránh trùng lặp tên file khi resume, ta nên đánh số batch dựa theo timestamp hoặc UUID 
                # Tuy nhiên user yêu cầu đơn giản, ta dùng index + timestamp.
                # Ở đây để đơn giản và dễ kiểm tra, ta dùng index của mảng pending hiện tại
                # (Lưu ý: Nếu resume nhiều lần sẽ sinh ra nhiều file nhỏ lẻ, post-process có thể merge sau).
                
                # Cách tốt hơn: Dùng UUID cho filename để an toàn tuyệt đối
                import uuid
                safe_batch_name = f"{uuid.uuid4().hex[:8]}" 
                
                self.logger.info(f"Đang xử lý Batch {i//self.batch_size + 1} ({len(batch_ids)} items)...")
                
                await self.process_batch(session, batch_ids, safe_batch_name)
        
        self.logger.info("✅ HOÀN THÀNH TOÀN BỘ CÔNG VIỆC.")
