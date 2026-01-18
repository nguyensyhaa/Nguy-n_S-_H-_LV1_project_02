
import asyncio
import aiohttp
import logging
from ..config.settings import BASE_URL, HEADERS
from .transform import clean_description, get_product_image_url

class TikiFetcher:
    def __init__(self, logger=None, retry_mode=False):
        self.base_url = BASE_URL
        self.headers = HEADERS
        self.logger = logger or logging.getLogger("TikiScraper")
        self.retry_mode = retry_mode
        # Normal: 20 concurrent, Retry mode: 10 concurrent
        self.sem = asyncio.Semaphore(10 if retry_mode else 20)

    async def fetch_product(self, session, product_id):
        async with self.sem:
            url = f"{self.base_url}{product_id}"
            retries = 3
            
            for attempt in range(retries):
                try:
                    # Chỉ delay ở retry mode để kiên nhẫn hơn
                    if self.retry_mode:
                        await asyncio.sleep(0.05)
                    
                    async with session.get(url, headers=self.headers, timeout=10, ssl=False) as response:
                        if response.status == 200:
                            data = await response.json()
                            return self._parse_data(product_id, data)
                        elif response.status == 429:
                            if self.retry_mode:
                                wait_time = (attempt + 1) * 1  # 1s, 2s, 3s
                                self.logger.warning(f"⚠️ RATE LIMIT (429) cho ID {product_id}. Đợi {wait_time}s...")
                                await asyncio.sleep(wait_time)
                            else:
                                self.logger.warning(f"⚠️ RATE LIMIT (429) cho ID {product_id}. Retry nhanh...")
                                # Chỉ delay 50ms rồi retry
                        elif response.status == 404:
                            if attempt < retries - 1:
                                if self.retry_mode:
                                    # Retry mode: retry 404 với delay 1s, 2s, 3s (kỹ hơn)
                                    wait_time = (attempt + 1) * 1
                                    self.logger.warning(f"⚠️ ID {product_id} trả 404, retry sau {wait_time}s...")
                                    await asyncio.sleep(wait_time)
                                else:
                                    # Normal mode: retry 404 nhanh (chỉ delay 100ms)
                                    self.logger.warning(f"⚠️ ID {product_id} trả 404, retry nhanh...")
                            else:
                                self.logger.error(f"❌ ID {product_id} không tồn tại (404 sau {retries} lần).")
                                return None
                        elif response.status >= 500:
                            if self.retry_mode:
                                wait_time = (attempt + 1) * 1
                                self.logger.warning(f"SERVER ERROR {response.status} cho ID {product_id}. Retry sau {wait_time}s...")
                                await asyncio.sleep(wait_time)
                            else:
                                self.logger.warning(f"SERVER ERROR {response.status} cho ID {product_id}. Retry nhanh...")
                        else:
                            self.logger.error(f"❌ Lỗi HTTP {response.status} cho ID {product_id}")
                            return None

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if self.retry_mode:
                        wait_time = (attempt + 1) * 1
                        self.logger.warning(f"NETWORK ERROR cho ID {product_id}: {str(e)}. Retry sau {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        self.logger.warning(f"NETWORK ERROR cho ID {product_id}: {str(e)}. Retry nhanh...")
                    
            self.logger.error(f"⛔ THẤT BẠI: Đã thử {retries} lần cho ID {product_id} nhưng không thành công.")
            return None

    def _parse_data(self, product_id, data):
        try:
            item = {
                'id': data.get('id', product_id),
                'name': data.get('name', ''),
                'url_key': data.get('url_key', ''),
                'price': data.get('price', 0),
                'description': clean_description(data.get('description', '')),
                'images_url': get_product_image_url(data)
            }
            return item
        except Exception as e:
            self.logger.error(f"PARSE ERROR cho ID {product_id}: {str(e)}")
            return None
