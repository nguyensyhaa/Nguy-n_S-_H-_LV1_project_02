
import asyncio
import aiohttp
import json
import logging
from .utils import clean_description, get_product_image_url

class TikiFetcher:
    def __init__(self, logger=None):
        self.base_url = "https://api.tiki.vn/product-detail/api/v1/products/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://tiki.vn/',
        }
        self.logger = logger or logging.getLogger("TikiScraper")
        # Semaphore để giới hạn concurrency (20 requests cùng lúc)
        self.sem = asyncio.Semaphore(20)

    async def fetch_product(self, session, product_id):
        """
        Gửi request lấy thông tin sản phẩm.
        Có cơ chế Retry 3 lần với Exponential Backoff.
        """
        async with self.sem: # Giới hạn số lượng request đồng thời
            url = f"{self.base_url}{product_id}"
            retries = 3
            
            for attempt in range(retries):
                try:
                    async with session.get(url, headers=self.headers, timeout=10, ssl=False) as response:
                        if response.status == 200:
                            data = await response.json()
                            return self._parse_data(product_id, data)
                        
                        elif response.status == 429: # Rate limit
                            wait_time = (2 ** attempt) * 2 # 2s, 4s, 8s
                            self.logger.warning(f"⚠️ RATE LIMIT (429) cho ID {product_id}. Đợi {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            
                        elif response.status == 404:
                            self.logger.error(f"❌ ID {product_id} không tồn tại (404).")
                            return None # Bỏ qua ID này
                            
                        elif response.status >= 500:
                            wait_time = (2 ** attempt) * 1
                            self.logger.warning(f"SERVER ERROR {response.status} cho ID {product_id}. Retry {attempt+1}/{retries}...")
                            await asyncio.sleep(wait_time)
                            
                        else:
                            self.logger.error(f"❌ Lỗi HTTP {response.status} cho ID {product_id}")
                            return None

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    wait_time = (2 ** attempt) * 1
                    self.logger.warning(f"NETWORK ERROR cho ID {product_id}: {str(e)}. Retry {attempt+1}/{retries}...")
                    await asyncio.sleep(wait_time)
                    
            self.logger.error(f"⛔ THẤT BẠI: Đã thử {retries} lần cho ID {product_id} nhưng không thành công.")
            return None # Trả về None nếu hết lượt retry

    def _parse_data(self, product_id, data):
        """
        Trích xuất và chuẩn hóa dữ liệu.
        Input: Raw JSON response
        Output: Clean Dictionary
        """
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
