
import asyncio
import json
import os
import glob
import aiohttp
import pandas as pd
import time
from ..config.settings import DISCORD_WEBHOOK_URL, DATA_DIR, LOG_DIR
from ..etl.extract import TikiFetcher
from ..utils.logger import setup_logger
from ..utils.discord import send_discord_webhook, edit_discord_message

class TikiPipeline:
    def __init__(self, input_file, output_dir=DATA_DIR, log_dir=LOG_DIR, retry_mode=False):
        self.input_file = input_file
        self.output_dir = output_dir
        self.log_dir = log_dir
        self.retry_mode = retry_mode
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        self.logger = setup_logger(log_dir)
        self.fetcher = TikiFetcher(self.logger, retry_mode=retry_mode)
        self.batch_size = 1000

    def get_completed_ids(self):
        completed_ids = set()
        if not os.path.exists(self.output_dir):
            return completed_ids
        try:  
            files = [f for f in os.listdir(self.output_dir) if f.endswith('.json')]
            for file in files:
                path = os.path.join(self.output_dir, file)
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for item in data:
                            completed_ids.add(str(item['id']))
                except Exception:
                    continue
        except Exception:
            pass
                
        self.logger.info(f"🔄 RESUME: Tìm thấy {len(completed_ids)} sản phẩm đã tải trước đó.")
        return completed_ids

    def load_pending_ids(self):
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

    def save_batch(self, data, batch_index):
        filename = f"products_batch_{batch_index}.json"
        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"💾 Đã lưu batch {batch_index}: {len(data)} sxp -> {filename}")
        except Exception as e:
            self.logger.error(f"WRITE ERROR: Không thể lưu file {filename}: {str(e)}")

    def _get_temp_file_path(self):
        return os.path.join(self.output_dir, "temp_buffer.jsonl")

    def _load_buffer_from_disk(self):
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
        try:
            with open(self._get_temp_file_path(), 'a', encoding='utf-8') as f:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        except Exception as e:
            self.logger.error(f"❌ WAL ERROR: Không thể ghi file nháp: {e}")

    def _rewrite_temp_file(self, buffer):
        try:
            with open(self._get_temp_file_path(), 'w', encoding='utf-8') as f:
                for item in buffer:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
        except Exception as e:
            self.logger.error(f"❌ WAL ERROR: Không thể làm mới file nháp: {e}")
            
    def log_failed_id(self, product_id):
        file_path = os.path.join(self.log_dir, "failed_products.txt")
        with open(file_path, "a") as f:
            f.write(f"{product_id}\n")

    async def run(self):
        pending_ids, total_source, processed_count = self.load_pending_ids()
        if not pending_ids:
            self.logger.info("🎉 Tất cả dữ liệu đã được tải xong!")
            return

        total_pending = len(pending_ids)
        start_time = time.time()
        
        # Send START notification
        if DISCORD_WEBHOOK_URL:
            embed_start = {
                "title": "🚀 TIKI CRAWLER: KHỞI ĐỘNG!",
                "description": f"Bắt đầu chiến dịch lấy **{total_source:,}** sản phẩm.",
                "color": 3447003, # Blue
                "fields": [
                    {"name": "📦 Tổng Input", "value": f"{total_source:,}", "inline": True},
                    {"name": "✅ Đã xong", "value": f"{processed_count:,}", "inline": True},
                    {"name": "⏳ Còn lại", "value": f"{total_pending:,}", "inline": True},
                ],
                "footer": {"text": "Tiki Scraper v2.1"}
            }
            await send_discord_webhook(embed=embed_start)
            
            # Send initial PROGRESS message and get its ID for editing later
            embed_progress_init = {
                "title": "📊 TIẾN ĐỘ CRAWL",
                "color": 3447003,
                "fields": [
                    {"name": "📈 Tiến độ", "value": "`[░░░░░░░░░░░░░░░░░░░░]` **0%**", "inline": False},
                    {"name": "⚡ Tốc độ", "value": "Đang khởi động...", "inline": True},
                    {"name": "⏱️ ETA", "value": "Đang tính...", "inline": True},
                ]
            }
            progress_msg_id = await send_discord_webhook(embed=embed_progress_init, wait_for_id=True)
        else:
            progress_msg_id = None

        self.logger.info(f"🚀 Tiki Crawler: Bắt đầu chạy! Còn lại {total_pending} ID.")

        result_buffer = self._load_buffer_from_disk()
        batch_counter = 1
        existing_files = glob.glob(os.path.join(self.output_dir, "products_batch_*.json"))
        if existing_files:
            batch_counter = len(existing_files) + 1
        initial_batch = batch_counter
        
        # Counters for final report
        success_count = 0
        fail_count = 0
        last_notified_pct = -1  # Track last notified percentage to avoid spam

        async with aiohttp.ClientSession() as session:
            input_chunk_size = 100  # Giảm từ 200 xuống 100 để ổn định 
            try:
                for i in range(0, total_pending, input_chunk_size):
                    chunk_ids = pending_ids[i : i + input_chunk_size]
                    self.logger.info(f"Đang xử lý chunk input {i}/{total_pending}...")
                    
                    tasks = [self.fetcher.fetch_product(session, pid) for pid in chunk_ids]
                    results = await asyncio.gather(*tasks)
                    
                    for idx, res in enumerate(results):
                        pid = chunk_ids[idx]
                        if res:
                            if not any(d['id'] == res['id'] for d in result_buffer):
                                result_buffer.append(res)
                                self._append_to_temp_file(res)
                                success_count += 1
                        else:
                            self.log_failed_id(pid)
                            fail_count += 1

                    # --- PROGRESS NOTIFICATION (Every 1% - EDIT SINGLE MESSAGE) ---
                    processed_so_far = i + len(chunk_ids)
                    pct = int(processed_so_far / total_pending * 100)
                    
                    # Edit the progress message at every 1% milestone
                    if progress_msg_id and pct > last_notified_pct:
                        last_notified_pct = pct
                        elapsed = time.time() - start_time
                        avg_speed = success_count / elapsed if elapsed > 0 else 0
                        remaining_items = total_pending - processed_so_far
                        eta_min = (remaining_items / avg_speed) / 60 if avg_speed > 0 else 0
                        
                        bar_len = 20
                        filled = int(pct / 5)  # 20 chars = 100%, so each 5% = 1 char
                        bar = "▓" * filled + "░" * (bar_len - filled)
                        
                        embed_prog = {
                            "title": f"📊 TIẾN ĐỘ CRAWL: {pct}%",
                            "color": 3447003, # Blue
                            "fields": [
                                {"name": "📈 Tiến độ", "value": f"`[{bar}]` **{pct}%**", "inline": False},
                                {"name": "⚡ Tốc độ", "value": f"**{avg_speed:.1f}** item/s", "inline": True},
                                {"name": "⏱️ ETA", "value": f"~ {eta_min:.1f} phút", "inline": True},
                                {"name": "✅ OK", "value": f"{success_count:,}", "inline": True},
                                {"name": "❌ Lỗi", "value": f"{fail_count:,}", "inline": True},
                            ]
                        }
                        await edit_discord_message(progress_msg_id, embed=embed_prog)

                    while len(result_buffer) >= 1000:
                        batch_to_save = result_buffer[:1000]
                        filename = f"{batch_counter:03d}"
                        self.save_batch(batch_to_save, filename)

                        result_buffer = result_buffer[1000:]
                        self._rewrite_temp_file(result_buffer)
                        batch_counter += 1
            
            except asyncio.CancelledError:
                self.logger.warning("⚠️ Crawler bị hủy!")
                if DISCORD_WEBHOOK_URL:
                    embed_stop = {
                        "title": "⚠️ CRAWLER DỪNG!",
                        "description": "Quá trình crawl đã bị dừng giữa chừng.",
                        "color": 16776960, # Yellow
                        "fields": [
                            {"name": "✅ Đã thu thập", "value": f"{success_count:,}", "inline": True},
                            {"name": "❌ Lỗi", "value": f"{fail_count:,}", "inline": True},
                        ]
                    }
                    await send_discord_webhook(embed=embed_stop)
                raise
            except KeyboardInterrupt:
                self.logger.warning("⚠️ User dừng bằng Ctrl+C!")
                if DISCORD_WEBHOOK_URL:
                    embed_stop = {
                        "title": "⚠️ CRAWLER DỪNG (Ctrl+C)!",
                        "description": "User đã dừng thủ công.",
                        "color": 16776960, # Yellow
                        "fields": [
                            {"name": "✅ Đã thu thập", "value": f"{success_count:,}", "inline": True},
                            {"name": "❌ Lỗi", "value": f"{fail_count:,}", "inline": True},
                        ]
                    }
                    await send_discord_webhook(embed=embed_stop)
                raise
            except Exception as e:
                self.logger.error(f"❌ Lỗi Pipeline: {e}")
                if DISCORD_WEBHOOK_URL:
                    await send_discord_webhook(embed={"title":"❌ CRASHED!", "description":str(e), "color":15158332})
                raise
            finally:
                if result_buffer:
                    self.logger.info(f"保存 buffer cuối: {len(result_buffer)}")
                    self.save_batch(result_buffer, f"{batch_counter:03d}")
            
            elapsed = time.time() - start_time
            if DISCORD_WEBHOOK_URL:
                 embed_finish = {
                    "title": "✅ CRAWLER HOÀN THÀNH!",
                    "description": "Dưới đây là thống kê cuối cùng.",
                    "color": 3066993, # Green
                    "fields": [
                        {"name": "⏱️ Tổng thời gian", "value": f"{elapsed/60:.1f} phút", "inline": True},
                        {"name": "� Tổng thu thập", "value": f"{success_count:,} sản phẩm", "inline": True},
                        {"name": "☠️ Link hỏng (404)", "value": f"{fail_count:,} ID", "inline": True},
                        {"name": "🔥 Trạng thái DB", "value": "Ready to Use", "inline": False}
                    ],
                    "footer": {"text": "Tiki Scraper v2.1"}
                }
                 await send_discord_webhook(embed=embed_finish)
