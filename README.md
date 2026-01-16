
# Tiki Scraper v2 - High Performance Crawler

Hệ thống crawler hiệu năng cao để thu thập dữ liệu sản phẩm Tiki.

## 🚀 Tính năng nổi bật
*   **Siêu tốc độ**: Sử dụng AsyncIO + Aiohttp (20 concurrent requests).
*   **Bền bỉ**: Tự động Retry (Backoff) khi mạng lỗi, tự động Resume khi chạy lại.
*   **An toàn**: Lưu dữ liệu thành nhiều file nhỏ để tránh mất mát.
*   **Sạch sẽ**: Code phân tách rõ ràng (Crawler, Pipeline, Utils), tuân thủ giao thức Data Engineering.

## 🛠️ Cài đặt & Sử dụng

1.  **Cài đặt thư viện:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Cấu hình:**
    *   Mở file `main.py` để trỏ đường dẫn file CSV đầu vào (`INPUT_CSV`).

3.  **Chạy Crawler:**
    ```bash
    python3 main.py
    ```
    Hệ thống sẽ tự động quét và tải dữ liệu vào folder `data/`.

## 📂 Cấu trúc dự án
```
.
├── src/                # Source code chính
│   ├── crawler.py      # Logic call API
│   ├── pipeline.py     # Logic điều phối luồng
│   └── utils.py        # Hàm tiện ích
├── data/               # Chứa dữ liệu output (JSON)
├── logs/               # Chứa log vận hành
├── input/              # Chứa file CSV đầu vào
├── main.py             # Entry point
└── requirements.txt    # Danh sách thư viện
```

## ⚠️ Lưu ý
*   Dữ liệu crawl được (trong folder `data/`) không được upload lên GitHub này do kích thước lớn (>300MB).
*   File log lỗi nằm ở `logs/error.log`.

---
**Author**: [Nguyen Sy Ha]
