# from fastapi import HTTPException
# import sqlite3
# import logging
# from pathlib import Path

# # # Paths
# # BASE_DIR = Path(__file__).resolve().parent
# # DB_FILE = BASE_DIR / "data/parcels.db"

# DB_FILE = Path("/dataB/parcels.db")

# # Configure logging
# LOG_FILE = "database.log"
# logger = logging.getLogger("database")  # Use a named logger for database logs
# logger.setLevel(logging.INFO)

# # Ensure handlers don't duplicate logs
# if not logger.handlers:
#     file_handler = logging.FileHandler(LOG_FILE)
#     file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
#     logger.addHandler(file_handler)

# def get_db_connection():
#     """Return a new database connection with a timeout to prevent locking issues."""
#     return sqlite3.connect(DB_FILE, timeout=10, isolation_level=None)

# def init_db():
#     """Initialize the SQLite database."""
#     with get_db_connection() as conn:
#         cursor = conn.cursor()
#         cursor.execute('''
#             CREATE TABLE IF NOT EXISTS parcels (
#                 id INTEGER PRIMARY KEY,  -- Keep the original ID from Capstone
#                 barcode TEXT,
#                 scan_status TEXT,
#                 last_scanned_when TEXT,
#                 address_name TEXT,
#                 address1 TEXT,
#                 address2 TEXT,
#                 city TEXT,
#                 state TEXT,
#                 zip TEXT,
#                 pod TEXT,
#                 inserted_at DATETIME DEFAULT CURRENT_TIMESTAMP
#             )
#         ''')
#         cursor.execute("CREATE INDEX IF NOT EXISTS idx_barcode ON parcels (barcode)")
#         conn.commit()
#         logger.info("✅ Database initialized.")

# def get_parcels_week(begin_date, end_date):
#     """Fetch parcels within the given begin_date and end_date."""
#     try:
#         with get_db_connection() as conn:
#             cursor = conn.cursor()

#             # Query to fetch parcels within the date range
#             query = """
#                 SELECT * FROM parcels
#                 WHERE inserted_at BETWEEN ? AND ?
#                 ORDER BY inserted_at ASC
#             """
#             params = (begin_date, end_date)

#             cursor.execute(query, params)
#             columns = [desc[0] for desc in cursor.description]
#             result = [dict(zip(columns, row)) for row in cursor.fetchall()]

#             logger.info(f"📦 Fetched {len(result)} parcels between {begin_date} and {end_date}.")
#             return result
#     except Exception as e:
#         logger.error(f"❌ Error fetching parcels for the week: {e}")
#         return []

# def get_parcels(sort_by="barcode", order="asc", limit=1000, city=None, state=None, scan_status=None, parcel_id=None):
#     """Fetch parcels with optional sorting, limits, and filtering by city, state, scan_status, or ID."""
#     try:
#         with get_db_connection() as conn:
#             cursor = conn.cursor()

#             # ✅ Only allow these fields to prevent SQL Injection
#             valid_sort_columns = {"id", "barcode", "scan_status", "last_scanned_when", "city", "state"}
#             if sort_by not in valid_sort_columns:
#                 sort_by = "barcode"  # Default fallback
            
#             # ✅ Enforce ASC or DESC only
#             order_clause = "ASC" if order.lower() == "asc" else "DESC"
            
#             # Base query
#             query = "SELECT * FROM parcels WHERE 1=1"
#             params = []

#             # Apply filters if provided
#             if city:
#                 query += " AND city LIKE ?"
#                 params.append(f"%{city}%")  # Partial match
            
#             if state:
#                 query += " AND state LIKE ?"
#                 params.append(f"%{state}%")
            
#             if scan_status:
#                 query += " AND scan_status LIKE ?"
#                 params.append(f"%{scan_status}%")

#             if parcel_id:
#                 query += " AND id = ?"
#                 params.append(parcel_id)
            
#             # Sorting
#             query += f" ORDER BY {sort_by} {order_clause} LIMIT ?"
#             params.append(limit)

#             cursor.execute(query, tuple(params))
#             columns = [desc[0] for desc in cursor.description]
#             result = [dict(zip(columns, row)) for row in cursor.fetchall()]

#             logger.info(f"Fetched {len(result)} parcels from the database.")
#             return result
#     except Exception as e:
#         logger.error(f"Error fetching parcels: {e}")
#         return []

# def get_parcel_by_barcode(barcode):
#     """Fetch a specific parcel by barcode."""
#     try:
#         with get_db_connection() as conn:
#             cursor = conn.cursor()
#             cursor.execute("SELECT * FROM parcels WHERE barcode = ?", (barcode,))
#             row = cursor.fetchone()
#             if row:
#                 columns = [desc[0] for desc in cursor.description]
#                 logger.info(f"Fetched parcel with barcode {barcode}")
#                 return dict(zip(columns, row))
#             else:
#                 logger.warning(f"Parcel not found with barcode {barcode}")
#                 return None
#     except Exception as e:
#         logger.error(f"Error fetching parcel by barcode {barcode}: {e}")
#         return None

# def update_parcels(updated_parcels):
#     """Efficiently update the database with new or modified parcel records."""
#     with get_db_connection() as conn:
#         try:
#             cursor = conn.cursor()

#             updates = 0
#             inserts = 0
#             skipped = 0  # Track skipped updates

#             for parcel in updated_parcels:
#                 parcel_id = parcel.get("id")  # Keep the original ID
#                 barcode = parcel["barcode"]
#                 new_status = parcel["scanStatus"]
#                 new_timestamp = parcel["lastScannedWhen"]["formattedDate"] + " " + parcel["lastScannedWhen"]["formattedTime"]

#                 # Check if record exists based on ID
#                 cursor.execute("SELECT id, scan_status, last_scanned_when FROM parcels WHERE id = ?", (parcel_id,))
#                 existing_record = cursor.fetchone()

#                 if existing_record:
#                     existing_id, existing_status, existing_timestamp = existing_record

#                     # Track changes before updating
#                     change_log = []
#                     if new_status != existing_status:
#                         change_log.append(f"status changed from '{existing_status}' to '{new_status}'")
#                     if new_timestamp != existing_timestamp:
#                         change_log.append(f"last scanned time changed from '{existing_timestamp}' to '{new_timestamp}'")

#                     if change_log:
#                         # Update existing record
#                         cursor.execute("""
#                             UPDATE parcels SET 
#                                 scan_status = ?, 
#                                 last_scanned_when = ?, 
#                                 address_name = ?, 
#                                 address1 = ?, 
#                                 address2 = ?, 
#                                 city = ?, 
#                                 state = ?, 
#                                 zip = ?, 
#                                 pod = ?
#                             WHERE id = ?
#                         """, (new_status, new_timestamp, parcel["address"]["name"], parcel["address"]["address1"], 
#                               parcel["address"]["address2"], parcel["address"]["city"], parcel["address"]["state"], 
#                               parcel["address"]["zip"], parcel.get("pod"), parcel_id))
#                         updates += 1
#                         logger.info(f"📦 Parcel {parcel_id} updated: {', '.join(change_log)}")
#                     else:
#                         skipped += 1  # No changes detected
#                         logger.info(f"⚡ Parcel {parcel_id} skipped (no changes detected).")

#                 else:
#                     # Insert new record
#                     cursor.execute("""
#                         INSERT INTO parcels (id, barcode, scan_status, last_scanned_when, address_name, address1, address2, city, state, zip, pod, inserted_at) 
#                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
#                     """, (parcel_id, barcode, new_status, new_timestamp, parcel["address"]["name"], 
#                           parcel["address"]["address1"], parcel["address"]["address2"], parcel["address"]["city"], 
#                           parcel["address"]["state"], parcel["address"]["zip"], parcel.get("pod")))
#                     inserts += 1
#                     logger.info(f"🆕 New parcel inserted: {parcel_id} (Barcode: {barcode})")

#             conn.commit()

#             # Logging summary
#             logger.info(f"🔄 {updates} parcels updated, {inserts} new parcels inserted, {skipped} unchanged.")

            

#             return {
#                 "updated": updates,
#                 "inserted": inserts,
#                 "skipped": skipped
#             }

#         except Exception as e:
#             conn.rollback()
#             logger.error(f"❌ Database error: {e}")
#             raise HTTPException(status_code=500, detail=f"Database update failed: {str(e)}")

import asyncpg
from datetime import datetime
import logging
import os
from fastapi import HTTPException
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database URL (Replace with your Aiven PostgreSQL URL)
DATABASE_URL = os.getenv("AIVEN_URL")

# Configure logging
LOG_FILE = "database.log"
logger = logging.getLogger("database")
logger.setLevel(logging.INFO)

# Ensure handlers don't duplicate logs
if not logger.handlers:
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

# Connection pool
db_pool = None

async def connect_to_db():
    """Initialize the PostgreSQL connection pool."""
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        logger.info("✅ Connected to PostgreSQL")

async def close_db():
    """Close the database connection pool."""
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("🔌 Database connection closed")

async def get_db_connection():
    """Ensure the database pool is initialized before acquiring a connection."""
    if db_pool is None:
        await connect_to_db()
    return db_pool

async def init_db():
    """Create the parcels table if it does not exist."""
    async with db_pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS parcels (
                id SERIAL PRIMARY KEY,  
                barcode TEXT UNIQUE,
                scan_status TEXT,
                last_scanned_when TEXT,
                destination_name TEXT,                
                address_name TEXT,
                address1 TEXT,
                address2 TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                pod TEXT,
                inserted_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_barcode ON parcels (barcode)")
        logger.info("✅ Database initialized.")

async def get_parcels_week(begin_date: str, end_date: str):
    """Fetch parcels within a given date range."""
    try:
        # Convert string dates to datetime objects
        begin_date_obj = datetime.strptime(begin_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

        async with db_pool.acquire() as conn:
            query = """
                SELECT * FROM parcels
                WHERE inserted_at BETWEEN $1 AND $2
                ORDER BY inserted_at ASC
            """
            rows = await conn.fetch(query, begin_date_obj, end_date_obj)
            logger.info(f"📦 Fetched {len(rows)} parcels between {begin_date} and {end_date}.")
            return [dict(row) for row in rows]
    
    except Exception as e:
        logger.error(f"❌ Error fetching parcels for the week: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")

async def get_parcels(sort_by="barcode", order="asc", limit=1000, city=None, state=None, scan_status=None, parcel_id=None):
    """Fetch parcels with optional sorting, limits, and filtering."""
    try:
        async with db_pool.acquire() as conn:
            valid_sort_columns = {"id", "barcode", "scan_status", "last_scanned_when", "city", "state"}
            if sort_by not in valid_sort_columns:
                sort_by = "barcode"  # Default sorting

            order_clause = "ASC" if order.lower() == "asc" else "DESC"

            query = "SELECT * FROM parcels WHERE 1=1"
            params = []
            param_index = 1  # PostgreSQL placeholders start with $1

            if city:
                query += f" AND city ILIKE ${param_index}"
                params.append(f"%{city}%")
                param_index += 1
            if state:
                query += f" AND state ILIKE ${param_index}"
                params.append(f"%{state}%")
                param_index += 1
            if scan_status:
                query += f" AND scan_status ILIKE ${param_index}"
                params.append(f"%{scan_status}%")
                param_index += 1
            if parcel_id:
                query += f" AND id = ${param_index}"
                params.append(parcel_id)
                param_index += 1

            # Add limit correctly with a dynamic placeholder
            query += f" ORDER BY {sort_by} {order_clause} LIMIT ${param_index}"
            params.append(limit)

            # Execute query with dynamically assigned parameters
            rows = await conn.fetch(query, *params)
            logger.info(f"📦 Fetched {len(rows)} parcels.")
            return [dict(row) for row in rows]

    except Exception as e:
        logger.error(f"❌ Error fetching parcels: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")

async def get_parcel_by_barcode(barcode):
    """Fetch a specific parcel by barcode."""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM parcels WHERE barcode = $1", barcode)
            if row:
                logger.info(f"📦 Parcel found with barcode {barcode}")
                return dict(row)
            else:
                logger.warning(f"❌ Parcel not found with barcode {barcode}")
                return None
    except Exception as e:
        logger.error(f"❌ Error fetching parcel by barcode {barcode}: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")

async def update_parcels(updated_parcels):
    
    """Efficiently update the database with new or modified parcel records."""
    try:
        async with db_pool.acquire() as conn:
            updates = 0
            inserts = 0
            skipped = 0  
            logging.info(f"🔍 Received {len(updated_parcels)} parcels for updating.")

            for parcel in updated_parcels:
                # logging.info(f"📩 Raw API Parcel Data: {parcel}")  # Log entire incoming parcel
                parcel_id = parcel.get("id")
                barcode = parcel["barcode"]
                new_status = parcel["scanStatus"]
                new_timestamp = parcel["lastScannedWhen"]["formattedDate"] + " " + parcel["lastScannedWhen"]["formattedTime"]
                destination_name = parcel.get("destination_name", "Unknown")
                # logging.info(f"📦 Processing Parcel ID {parcel.get('id')}, Delivery Address: {destination_name}")

                # Check if record exists
                existing_record = await conn.fetchrow("SELECT id, scan_status, last_scanned_when, destination_name FROM parcels WHERE barcode = $1", barcode)

                if existing_record:
                    existing_id, existing_status, existing_timestamp, existing_destination = existing_record["id"], existing_record["scan_status"], existing_record["last_scanned_when"],existing_record[
        "destination_name"]

                    change_log = []
                    if new_status != existing_status:
                        change_log.append(f"status changed from '{existing_status}' to '{new_status}'")
                    if new_timestamp != existing_timestamp:
                        change_log.append(f"last scanned time changed from '{existing_timestamp}' to '{new_timestamp}'")
                    if destination_name != existing_destination:
                        change_log.append(
                            f"destination changed from '{existing_destination}' to '{destination_name}'"
        )

                    if change_log:
                        await conn.execute("""
                            UPDATE parcels SET 
                                scan_status = $1, 
                                last_scanned_when = $2, 
                                destination_name = $3,
                                address_name = $4, 
                                address1 = $5, 
                                address2 = $6, 
                                city = $7, 
                                state = $8, 
                                zip = $9, 
                                pod = $10
                            WHERE barcode = $11
                        """, new_status, new_timestamp, destination_name, parcel["address"]["name"], parcel["address"]["address1"], parcel["address"]["address2"],
                           parcel["address"]["city"], parcel["address"]["state"], parcel["address"]["zip"], parcel.get("pod"), barcode)
                        updates += 1
                        logger.info(f"📦 Parcel {barcode} updated: {', '.join(change_log)}")
                    else:
                        skipped += 1
                else:
                    await conn.execute("""
                        INSERT INTO parcels (id, barcode, scan_status, last_scanned_when, address_name, address1, address2, city, state, zip, pod, inserted_at) 
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
                    """, parcel_id, barcode, new_status, new_timestamp, destination_name, parcel["address"]["name"], parcel["address"]["address1"],
                       parcel["address"]["address2"], parcel["address"]["city"], parcel["address"]["state"], parcel["address"]["zip"], parcel.get("pod"))
                    inserts += 1
                    logger.info(f"🆕 New parcel inserted: {barcode}")

            logger.info(f"🔄 {updates} updated, {inserts} inserted, {skipped} unchanged.")

            return {"updated": updates, "inserted": inserts, "skipped": skipped}

    except Exception as e:
        logger.error(f"❌ Database error: {e}")
        raise HTTPException(status_code=500, detail="Database update failed.")