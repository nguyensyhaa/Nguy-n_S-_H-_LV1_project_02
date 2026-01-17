
import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

# Logger setup
logger = logging.getLogger(__name__)

def get_db_connection_url() -> str:
    """Constructs the database connection URL from environment variables."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    dbname = os.getenv("POSTGRES_DB", "tiki_db")
    
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

def get_engine() -> Engine:
    """Creates and returns a SQLAlchemy engine."""
    url = get_db_connection_url()
    try:
        engine = create_engine(url, pool_pre_ping=True)
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise

def init_db(engine: Engine):
    """Initializes the database schema."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS tiki_products (
        id BIGINT PRIMARY KEY,
        name TEXT,
        url_key TEXT,
        price BIGINT,
        description TEXT,
        images_url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.info("Database table 'tiki_products' checked/created successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise
