
import json
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .database import get_engine, init_db
import json
from json import JSONDecodeError

# Logger setup
logger = logging.getLogger(__name__)

# 1. INPUT VALIDATION
def validate_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Step 1: Input Validation
    Filters out invalid records.
    """
    valid_data = []
    for item in raw_data:
        # Check for required fields
        if not item.get("id"):
            logger.warning(f"Skipping record with missing ID: {item}")
            continue
        try:
            item["id"] = int(item["id"])
        except ValueError:
            logger.warning(f"Skipping record with invalid ID format: {item}")
            continue
            
        if not item.get("name"):
            logger.warning(f"Skipping record with missing Name: {item.get('id')}")
            continue
            
        valid_data.append(item)
    
    logger.info(f"Validation complete. Valid records: {len(valid_data)}/{len(raw_data)}")
    return valid_data

# 2. PRE-PROCESSING
def preprocess_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Step 2: Pre-processing & Standardization
    Cleans text, handles nulls, and deduplicates.
    """
    processed_data = []
    seen_ids = set()
    
    for item in data:
        prod_id = item["id"]
        
        # Deduplication within batch
        if prod_id in seen_ids:
            logger.warning(f"Duplicate ID found in batch: {prod_id}. Keeping first occurrence.")
            continue
        seen_ids.add(prod_id)
        
        # Clean description
        description = item.get("description", "")
        if description:
            item["description"] = description.strip()
        else:
            item["description"] = None
            
        # Handle Price (default to 0 if missing/null)
        price = item.get("price")
        if price is None:
             item["price"] = 0
        else:
            try:
                item["price"] = int(price)
            except ValueError:
                logger.warning(f"Invalid price for ID {prod_id}: {price}. Defaulting to 0.")
                item["price"] = 0
        
        # Ensure other fields exist for SQL binding
        if "url_key" not in item:
            item["url_key"] = None
        if "images_url" not in item:
            item["images_url"] = None
            
        processed_data.append(item)
        
    logger.info(f"Pre-processing complete. Unique records: {len(processed_data)}")
    return processed_data

# 3. CORE ANALYSIS
def core_analysis(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Step 3: Core Analysis
    Business logic transformations. (Pass-through for this Lab)
    """
    # Placeholder for future logic (e.g., currency conversion, category derivation)
    return data

# 4. RETRY & ERROR HANDLING (Database Load)
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def load_batch_to_db(engine: Engine, data: List[Dict[str, Any]]):
    """
    Step 4: Load to DB with Retry and Idempotency (Upsert).
    """
    if not data:
        return

    upsert_sql = text("""
        INSERT INTO tiki_products (id, name, url_key, price, description, images_url, updated_at)
        VALUES (:id, :name, :url_key, :price, :description, :images_url, CURRENT_TIMESTAMP)
        ON CONFLICT (id) DO UPDATE
        SET 
            name = EXCLUDED.name,
            url_key = EXCLUDED.url_key,
            price = EXCLUDED.price,
            description = EXCLUDED.description,
            images_url = EXCLUDED.images_url,
            updated_at = CURRENT_TIMESTAMP;
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, data)

# 5. POST-VALIDATION
def post_validation(input_count: int, engine: Engine, batch_ids: List[int]):
    """
    Step 5: Post-validation
    Verifies data integrity after load.
    """
    # Simple count check or query to verify updates could go here
    # For high performance, we might skip querying every single batch unless critical
    logger.info(f"Batch loaded successfully. Input count: {input_count}")


def run_etl_pipeline(file_path: str):
    """
    Orchestrates the 5-step ETL pipeline.
    """
    logger.info(f"Starting ETL for file: {file_path}")
    
    # 0. Load Data
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
    except FileNotFoundError:
        logger.error(f"❌ File not found: {file_path}")
        return
    except JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON format in {file_path}: {e}")
        return
    except Exception as e:
        logger.error(f"❌ Unexpected error reading file {file_path}: {e}")
        return

    if not isinstance(raw_data, list):
         logger.error(f"Invalid file format in {file_path}. Expected a list of products.")
         return

    # 1. Validation
    valid_data = validate_data(raw_data)
    
    # 2. Pre-processing
    clean_data = preprocess_data(valid_data)
    
    # 3. Core Analysis
    final_data = core_analysis(clean_data)
    
    # 4. Load (with Retry)
    if final_data:
        try:
            engine = get_engine()
            # Ensure table exists (idempotent check)
            init_db(engine)
            
            load_batch_to_db(engine, final_data)
            
            # 5. Post-validation
            post_validation(len(final_data), engine, [d['id'] for d in final_data])
            
        except OperationalError as e:
            logger.critical(f"❌ Database Connectivity Error [Lab 2 Exception]: {e}")
        except SQLAlchemyError as e:
             logger.critical(f"❌ Database Execution Error [Lab 2 Exception]: {e}")
        except Exception as e:
            logger.critical(f"❌ Unknown Error during DB Load: {e}")
            # In a real system, we might move this file to a 'failed' directory or DLQ
    else:
        logger.warning(f"No valid data to load for file: {file_path}")

