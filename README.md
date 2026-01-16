
# Tiki Scraper - Final Version

Hệ thống crawler hiệu năng cao để thu thập dữ liệu sản phẩm Tiki.

## 🚀 Tính năng nổi bật
*   **Siêu tốc độ**: Sử dụng AsyncIO + Aiohttp (20 concurrent requests).
*   **Bền bỉ**: Tự động Retry (Backoff) khi mạng lỗi, tự động Resume khi chạy lại.
*   **An toàn**: Lưu dữ liệu thành nhiều file nhỏ để tránh mất mát.
*   **Sạch sẽ**: Code phân tách rõ ràng (Crawler, Pipeline, Utils), tuân thủ giao thức Data Engineering.

## 🛠️ Cài đặt & Sử dụng chuyên nghiệp

1.  **Cài đặt Crawl Tool:**
    ```bash
    pip3 install -e .
    ```

2.  **Sử dụng (Unified Command):**

    *   **🕷️ Chạy Crawl:**
        ```bash
        tiki-scraper crawl --input "/Users/syha/Downloads/products-0-200000.csv"
        ```

    *   **🛡️ Kiểm tra Input:**
        ```bash
        tiki-scraper validate --input "/Users/syha/Downloads/products-0-200000.csv"
        ```

    *   **🧩 Gộp File:**
        ```bash
        tiki-scraper merge --data-dir "data" --output "all_products.json"
        ```

## 📂 Cấu trúc dự án (Refactored)
```
.
├── src/
│   └── tiki_scraper/
│       ├── cli.py      # Unified CLI Entry point
│       ├── crawler.py  # Core Async Logic
│       └── ...
├── pyproject.toml      # Modern Build Config
├── setup.py            # Install Script
└── ...
```

## ⚠️ Lưu ý
*   Dữ liệu crawl được (trong folder `data/`) không được upload lên GitHub này do kích thước lớn (>300MB).
*   File log lỗi nằm ở `logs/error.log`.

---
**Author**: [Nguyen Sy Ha]
