# Hướng dẫn Chạy Tiki Scraper (Project 02) trên PyCharm

Dưới đây là các bước để chạy Crawler (cào dữ liệu) và ETL (nạp DB) ngay trong PyCharm.

## 1. Cấu hình Project
1.  Mở PyCharm, chọn **File -> Open...** -> Chọn thư mục `Project_02_Tiki_Scraper`.
2.  Mở tab **Terminal** (bên dưới), cài đặt thư viện:
    ```bash
    pip install -r requirements.txt
    ```
3.  Mở file `.env`, điền mật khẩu Postgres của bạn (nếu có). (Mặc định user `syha` đang không pass).

## 2. Tạo Run Configurations (Cấu hình nút Chạy ▶️)

Bạn cần tạo 2 cấu hình: Một cái để **Cào Dữ Liệu**, một cái để **Nạp Dữ Liệu**.

### Cấu hình 1: Tiki Crawl (Cào dữ liệu)
1.  Nhấn menu Run (góc trên phải) -> **Edit Configurations...**
2.  Nhấn dấu **+** -> **Python**.
3.  Điền thông tin:
    *   **Name**: `Tiki Crawl`
    *   **Script path**: Chọn file `cli.py` trong thư mục `src/tiki_scraper`.
    *   **Parameters**: `crawl --input input.csv`
    *   **Environment variables**: `PYTHONPATH=src` (Bắt buộc).
    *   **Working directory**: Chọn thư mục gốc `Project_02_Tiki_Scraper`.

### Cấu hình 2: Tiki Ingest (Nạp vào DB - Lab 1 & 2)
1.  Nhấn dấu **+** -> **Python** (hoặc copy cấu hình trên).
2.  Điền thông tin:
    *   **Name**: `Tiki Ingest`
    *   **Script path**: Giống trên (`cli.py`).
    *   **Parameters**: `ingest --data-dir data`
    *   **Environment variables**: `PYTHONPATH=src` (Bắt buộc).
    *   **Working directory**: Chọn thư mục gốc `Project_02_Tiki_Scraper`.

## 3. Cách chạy
*   **Bước 1**: Chọn `Tiki Crawl` trên thanh công cụ và nhấn **Run (▶️)**. Tool sẽ bắt đầu tải dữ liệu về folder `data/`. Bạn có thể dừng (Stop) và chạy lại (Resume) bất cứ lúc nào.
*   **Bước 2**: Khi đã có dữ liệu, chọn `Tiki Ingest` và nhấn **Run (▶️)**. Tool sẽ đọc file JSON và nạp vào Postgres DB.

## 4. Kiểm tra Database
Bấm vào tab **Database** bên phải PyCharm để query xem dữ liệu đã vào chưa:
```sql
SELECT count(*) FROM tiki_products;
SELECT * FROM tiki_products LIMIT 10;
```
