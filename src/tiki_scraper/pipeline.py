
import asyncio
import json
import os
import glob
import aiohttp
import pandas as pd
from .crawler import TikiFetcher
from .utils import setup_logger, send_discord_webhook
from dotenv import load_dotenv

load_dotenv()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

import time

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
            return pending_ids, len(all_ids), len(completed_ids)
        except Exception as e:
            self.logger.critical(f"FATAL: Không thể đọc file input CSV: {str(e)}")
            return [], 0, 0

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
            msg = f"💾 Đã lưu batch {batch_index}: {len(data)} sxp -> {filename}"
            self.logger.info(msg)
            # Không gửi notify ở đây để tránh spam quá nhiều, hoặc gửi dạng silent
        except Exception as e:
            self.logger.error(f"WRITE ERROR: Không thể lưu file {filename}: {str(e)}")

    def _get_temp_file_path(self):
        return os.path.join(self.output_dir, "temp_buffer.jsonl")

    def _load_buffer_from_disk(self):
        """Khôi phục dữ liệu từ file nháp (nếu có) sau khi bị crash"""
        buffer = []
        temp_path = self._get_temp_file_path()
        if os.path.exists(temp_path):
            try:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            buffer.append(json.loads(line))
                self.logger.info(f"❤️ PHỤC HỒI DỮ LIỆU: Tìm thấy {len(buffer)} sản phẩm trong file nháp!")
            except Exception as e:
                self.logger.error(f"⚠️ Lỗi đọc file nháp: {e}")
        return buffer

    def _append_to_temp_file(self, item):
        """Ghi ngay lập tức 1 item xuống đĩa (WAL)"""
        try:
            with open(self._get_temp_file_path(), 'a', encoding='utf-8') as f:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        except Exception as e:
            self.logger.error(f"❌ WAL ERROR: Không thể ghi file nháp: {e}")

    def _rewrite_temp_file(self, buffer):
        """Ghi lại file nháp mới (dùng sau khi đã cắt bớt 1000 item)"""
        try:
            with open(self._get_temp_file_path(), 'w', encoding='utf-8') as f:
                for item in buffer:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
        except Exception as e:
            self.logger.error(f"❌ WAL ERROR: Không thể làm mới file nháp: {e}")

    async def run(self):
        """
        Hàm chính điều phối toàn bộ quy trình.
        """
        pending_ids, total_source, processed_count = self.load_pending_ids()
        if not pending_ids:
            self.logger.info("🎉 Tất cả dữ liệu đã được tải xong!")
            # await send_discord_webhook(DISCORD_WEBHOOK_URL, "🎉 **Tiki Crawler**: Tất cả dữ liệu đã được tải xong!")
            return

        total_pending = len(pending_ids)
        
        # --- RICH START NOTIFICATION ---
        start_time = time.time()
        
        if DISCORD_WEBHOOK_URL:
            # Gửi Embed mở màn (Blue)
            embed_start = {
                "title": "🚀 TIKI CRAWLER: KHỞI ĐỘNG!",
                "description": f"Bắt đầu chiến dịch lấy **{total_source:,}** sản phẩm.",
                "color": 3447003, # Blue
                "fields": [
                    {"name": "📦 Tổng Input", "value": f"{total_source:,}", "inline": True},
                    {"name": "✅ Đã xong", "value": f"{processed_count:,}", "inline": True},
                    {"name": "⏳ Còn lại", "value": f"{total_pending:,}", "inline": True},
                ],
                "footer": {"text": "Tiki Scraper `v2.0` | Mode: Async/WAL"}
            }
            await send_discord_webhook(DISCORD_WEBHOOK_URL, embed=embed_start)

        self.logger.info(f"🚀 Tiki Crawler: Bắt đầu chạy! Còn lại {total_pending} ID.")

        # 1. Khôi phục buffer từ đĩa (Crash Recovery)
        result_buffer = self._load_buffer_from_disk()
        
        batch_counter = 1
        existing_files = glob.glob(os.path.join(self.output_dir, "products_batch_*.json"))
        if existing_files:
            batch_counter = len(existing_files) + 1

        initial_batch = batch_counter

        async with aiohttp.ClientSession() as session:
            input_chunk_size = 200 
            
            try:
                for i in range(0, total_pending, input_chunk_size):
                    chunk_ids = pending_ids[i : i + input_chunk_size]
                    self.logger.info(f"Đang xử lý chunk input {i}/{total_pending}...")
                    
                    # Fetch song song
                    tasks = [self.fetcher.fetch_product(session, pid) for pid in chunk_ids]
                    results = await asyncio.gather(*tasks)
                    
                    for idx, res in enumerate(results):
                        pid = chunk_ids[idx]
                        if res:
                            # Check trùng trong buffer để tránh duplicate khi resume chồng chéo
                            if not any(d['id'] == res['id'] for d in result_buffer):
                                result_buffer.append(res)
                                # 2. Ghi ngay xuống đĩa (WAL)
                                self._append_to_temp_file(res)
                        else:
                            self.log_failed_id(pid)

                    # 3. Kiểm tra buffer xem đủ 1000 chưa
                    while len(result_buffer) >= 1000:
                        batch_to_save = result_buffer[:1000]
                        filename = f"{batch_counter:03d}"
                        self.save_batch(batch_to_save, filename)
                        
                        # --- RICH PROGRESS NOTIFICATION (Every 5 batches) ---
                        if DISCORD_WEBHOOK_URL and (batch_counter % 5 == 0 or batch_counter == 1):
                            elapsed = time.time() - start_time
                            
                            # Metrics Calculation
                            items_done_session = (batch_counter - initial_batch + 1) * 1000
                            avg_speed = items_done_session / elapsed if elapsed > 0 else 0
                            
                            remaining_items = total_source - processed_count - items_done_session
                            eta_min = (remaining_items / avg_speed) / 60 if avg_speed > 0 else 0
                            
                            # Progress Bar
                            current_total = processed_count + items_done_session
                            pct = min(100, int(current_total / total_source * 100))
                            bar_len = 10
                            filled = int(pct / 10)
                            bar = "▓" * filled + "░" * (bar_len - filled)
                            
                            embed_prog = {
                                "title": f"🚀 TIẾN ĐỘ: BATCH {batch_counter}",
                                "color": 3447003, # Blue
                                "fields": [
                                    {"name": "📈 Tiến độ", "value": f"`[{bar}]` **{pct}%**", "inline": False},
                                    {"name": "⚡ Tốc độ", "value": f"**{avg_speed:.1f}** item/s", "inline": True},
                                    {"name": "⏱️ ETA (Dự kiến)", "value": f"~ {eta_min:.1f} phút", "inline": True},
                                    {"name": "📦 Mới tải", "value": f"{items_done_session:,} sp", "inline": True}
                                ]
                            }
                            await send_discord_webhook(DISCORD_WEBHOOK_URL, embed=embed_prog)
                        elif DISCORD_WEBHOOK_URL:
                             # Log nhẹ nhàng cho các batch lẻ
                             self.logger.info(f"💾 Saved Batch {batch_counter} (Silent)")

                        # Cắt buffer và update lại file nháp
                        result_buffer = result_buffer[1000:]
                        self._rewrite_temp_file(result_buffer)
                        
                        batch_counter += 1
            
            except asyncio.CancelledError:
                self.logger.warning("⚠️ Task bị hủy (Ctrl+C)!")
                embed_stop = {
                    "title": "⚠️ CRAWLER STOPPED",
                    "description": "User đã dừng thủ công (Ctrl+C).",
                    "color": 16776960 # Yellow
                }
                await send_discord_webhook(DISCORD_WEBHOOK_URL, embed=embed_stop)
                raise
            except Exception as e:
                self.logger.error(f"❌ Lỗi không mong muốn trong loop: {e}")
                embed_err = {
                    "title": "❌ CRAWLER CRASHED!",
                    "description": f"Lỗi nghiêm trọng: `{str(e)}`",
                    "color": 15158332 # Red
                }
                await send_discord_webhook(DISCORD_WEBHOOK_URL, embed=embed_err)
                raise
            finally:
                # Lưu nốt phần dư còn lại trong buffer ra file JSON luôn (thay vì chỉ để trong WAL)
                if result_buffer:
                    self.logger.info(f"💾 GRACEFUL SHUTDOWN: Lưu nốt {len(result_buffer)} sản phẩm cuối cùng vào file...")
                    self.save_batch(result_buffer, f"{batch_counter:03d}")
            
            # --- RICH FINISH NOTIFICATION ---
            elapsed = time.time() - start_time
            # Ước lượng sản phẩm đã chạy trong session này
            final_session_items = (batch_counter - initial_batch) * 1000 
            if result_buffer: final_session_items += len(result_buffer) # Add buffer if any

            if DISCORD_WEBHOOK_URL:
                 embed_finish = {
                    "title": "✅ CRAWLER HOÀN THÀNH!",
                    "description": "Toàn bộ dữ liệu đã được tải về an toàn.",
                    "color": 3066993, # Green
                    "fields": [
                        {"name": "⏱️ Tổng thời gian", "value": f"{elapsed/60:.1f} phút", "inline": True},
                        {"name": "📦 Tổng sản phẩm (Session)", "value": f"{final_session_items:,}", "inline": True},
                        {"name": "🔥 Trạng thái", "value": "Sẵn sàng Ingest DB", "inline": False}
                    ]
                }
                 await send_discord_webhook(DISCORD_WEBHOOK_URL, embed=embed_finish)
            
            msg_finish = "✅ **Tiki Crawler**: HOÀN THÀNH TOÀN BỘ CÔNG VIỆC."
            self.logger.info(msg_finish.replace("**", ""))

    def log_failed_id(self, product_id):
        """Ghi ID bị lỗi vào file riêng để retry sau"""
        file_path = os.path.join(self.log_dir, "failed_products.txt")
        with open(file_path, "a") as f:
            f.write(f"{product_id}\n")
