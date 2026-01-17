# DỰ ÁN TIKI SCRAPER (v2.0)

Công cụ thu thập dữ liệu sản phẩm Tiki.vn hiệu năng cao (Asynchronous), được thiết kế theo chuẩn Data Engineering 5 bước, hỗ trợ nạp tự động vào PostgreSQL.

## 🚀 Tính năng Nổi bật

*   **Hiệu năng Cực cao**: Sử dụng `asyncio` và `aiohttp` để tải song song (lên tới 20 requests/giây).
*   **Chống Mất Dữ liệu (WAL)**: Cơ chế Write-Ahead Logging giúp bảo toàn dữ liệu ngay cả khi mất điện hoặc crash app.
*   **Tự động Nối lại (Resume)**: Thông minh phát hiện các file đã tải và chỉ tải tiếp phần còn thiếu.
*   **Dữ liệu Sạch**: 
    *   Kiểm tra đầu vào nghiêm ngặt.
    *   **Làm sạch Description**: Tự động loại bỏ HTML, chuẩn hóa văn bản.
    *   Chia file (Batching) mỗi 1000 sản phẩm.
*   **Thông báo "Dashboard" (Discord)**: Gửi báo cáo tiến độ Real-time (Tốc độ, ETA, Thanh tiến độ) qua Discord Webhook.
*   **Nạp Database (Postgres)**: Tích hợp sẵn pipeline ETL để đẩy dữ liệu JSON vào PostgreSQL (có chống trùng lặp Upsert).
*   **Vét Cạn (Auto Retry)**: Lệnh chuyên dụng để tự động quét và chạy lại các ID bị lỗi (404/Network Error).

## 📂 Cấu trúc Dự án

```text
Project_02_Tiki_Scraper/
├── data/                  # Chứa file kết quả JSON (VD: products_batch_001.json)
├── logs/                  # Chứa log vận hành và file failed_products.txt
├── src/
│   └── tiki_scraper/
│       ├── __init__.py
│       ├── cli.py         # Bộ chỉ huy (Các lệnh: crawl, retry, ingest...)
│       ├── crawler.py     # Robot thu thập (Gọi API, xử lý lỗi mạng, parse data)
│       ├── pipeline.py    # Luồng xử lý chính (Batching, WAL, Dashboard Discord)
│       ├── etl.py         # Logic nạp vào Postgres
│       ├── database.py    # Kết nối Database
│       └── utils.py       # Tiện ích (Logger, Discord Embeds, Xử lý text)
├── input.csv              # Danh sách ID sản phẩm đầu vào
├── requirements.txt       # Các thư viện Python cần thiết
├── setup.py               # Cấu hình gói cài đặt
└── .env                   # Cấu hình mật (Database & Webhook)
```

## 🛠️ Hướng dẫn Cài đặt

1.  **Thiết lập Môi trường**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

2.  **Cấu hình**:
    Tạo file `.env` với nội dung sau:
    ```env
    DB_HOST=localhost
    DB_NAME=tiki_db
    DB_USER=your_user
    DB_PASS=your_password
    DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/... (Link Webhook của bạn)
    ```

## 💻 Hướng dẫn Sử dụng (Command Line)

Tool được điều khiển hoàn toàn qua dòng lệnh (CLI):

### 1. Bắt đầu Cào Dữ liệu (Crawl)
Chạy lệnh sau để bắt đầu cào từ file CSV:
```bash
python3 -m tiki_scraper.cli crawl --input input.csv
```

### 2. Thử lại các ID lỗi (Retry - Vét cạn)
Tự động quét file log lỗi và chạy lại để không bỏ sót sản phẩm nào:
```bash
python3 -m tiki_scraper.cli retry
# Hoặc chỉ định file log lỗi riêng:
python3 -m tiki_scraper.cli retry --log-file logs/error_ids.txt
```

### 3. Nạp vào PostgreSQL (Ingest)
Đẩy toàn bộ file JSON trong thư mục `data/` vào Database:
```bash
python3 -m tiki_scraper.cli ingest --data-dir data
```

### 4. Công cụ phụ trợ
*   **Kiểm tra Input**: `python3 -m tiki_scraper.cli validate --input input.csv`
*   **Gộp file JSON**: `python3 -m tiki_scraper.cli merge --output all_data.json`

## 📊 Giám sát & Theo dõi

*   **Màn hình Console**: Hiện log chi tiết quá trình chạy.
*   **File Logs**: Xem kỹ hơn tại `logs/application.log` (thông tin) và `logs/error.log` (lỗi).
*   **Discord**: Join kênh Discord đã cấu hình để xem Bảng điều khiển trực quan (Thẻ bài xanh/đỏ, Thanh tiến độ).

## 🛡️ Độ tin cậy

*   **Bị dừng đột ngột?** Chỉ cần chạy lại lệnh `crawl`. Tool sẽ tự động bỏ qua các file đã xong và chạy tiếp.
*   **Máy bị sập (Crash)?** Đừng lo! File `temp_buffer.jsonl` (WAL) đã lưu lại những gì chưa kịp ghi. Tool sẽ tự động phục hồi nó trong lần chạy tới.
