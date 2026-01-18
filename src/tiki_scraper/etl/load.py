
import json
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from ..config.settings import DB_HOST, DB_NAME, DB_USER, DB_PASS

def get_db_url():
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

def get_engine():
    try:
        engine = create_engine(get_db_url(), echo=False)
        return engine
    except Exception as e:
        print(f"❌ DB Connection Error: {e}")
        raise

def init_db():
    """Tạo bảng nếu chưa có"""
    engine = get_engine()
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS tiki_products (
        id BIGINT PRIMARY KEY,
        name TEXT,
        url_key TEXT,
        price NUMERIC,
        description TEXT,
        images_url TEXT,
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()

def load_data_to_postgres(file_path):
    """Load JSON data vào Postgres"""
    logger = logging.getLogger("TikiScraper")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if not data:
            return

        engine = get_engine()
        init_db() # Ensure table exists

        # UPSERT Logic (Insert or Update if exists)
        upsert_sql = text("""
            INSERT INTO tiki_products (id, name, url_key, price, description, images_url)
            VALUES (:id, :name, :url_key, :price, :description, :images_url)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                price = EXCLUDED.price,
                description = EXCLUDED.description,
                images_url = EXCLUDED.images_url,
                crawled_at = CURRENT_TIMESTAMP;
        """)
        
        with engine.connect() as conn:
            conn.execute(upsert_sql, data)
            conn.commit()
            
        logger.info(f"✅ Database: Loaded {len(data)} items from {file_path}")
        
    except FileNotFoundError:
        logger.error(f"❌ File not found: {file_path}")
    except json.JSONDecodeError:
        logger.error(f"❌ JSON Error in file: {file_path}")
    except SQLAlchemyError as e:
        logger.error(f"❌ Database Error: {e}")
    except Exception as e:
        logger.error(f"❌ Unknown Load Error: {e}")
