# TIKI SCRAPER v2.1

## 📋 Giới thiệu

Công cụ thu thập dữ liệu sản phẩm từ sàn thương mại điện tử Tiki.vn.

**Mục đích**: Tự động lấy thông tin chi tiết của hàng trăm ngàn sản phẩm (ID, tên, giá, mô tả, hình ảnh) từ API Tiki và lưu vào file JSON hoặc PostgreSQL để phục vụ phân tích dữ liệu.

**Thiết kế theo chuẩn Data Engineering 5 bước**:
1. Input Validation - Kiểm tra dữ liệu đầu vào
2. Pre-processing - Tiền xử lý và chuẩn hóa
3. Core Analysis - Xử lý logic chính
4. Retry & Error Handling - Cơ chế chịu lỗi
5. Post-validation - Kiểm tra chất lượng đầu ra

## 🚀 Tính năng

- Thu thập song song nhanh (20 concurrent requests)
- Tự động phục hồi khi bị dừng (Resume)
- Chống mất dữ liệu bằng WAL (Write-Ahead Logging)
- Thông báo Discord real-time (tiến độ, lỗi, hoàn thành)
- Retry Mode vét cạn các ID lỗi với delay thông minh
- Nạp dữ liệu vào PostgreSQL (Upsert chống trùng)

## 📂 Cấu trúc dự án

```
src/tiki_scraper/
├── config/settings.py     # Cấu hình hệ thống
├── etl/
│   ├── extract.py         # Crawler lấy dữ liệu từ API
│   ├── transform.py       # Làm sạch HTML, chuẩn hóa text
│   └── load.py            # Nạp vào PostgreSQL
├── pipelines/
│   └── crawl_pipeline.py  # Điều phối toàn bộ luồng
├── utils/
│   ├── logger.py          # Logging
│   └── discord.py         # Discord notifications
└── cli.py                 # Giao diện dòng lệnh
```

## 🛠️ Cài đặt

```bash
pip install -r requirements.txt
```

Tạo file `.env`:
```env
DB_HOST=localhost
DB_NAME=tiki_db
DB_USER=your_user
DB_PASS=your_password
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

## 💻 Sử dụng

### Crawl dữ liệu
```bash
python3 -m tiki_scraper.cli crawl --input input.csv
```

### Retry các ID lỗi
```bash
python3 -m tiki_scraper.cli retry --log-file logs/failed_products.txt
```

### Nạp vào PostgreSQL
```bash
python3 -m tiki_scraper.cli ingest --data-dir data
```
