# import sqlite3
# import logging
# from pathlib import Path
# from datetime import datetime

# # Paths
# DB_FILE = Path("parcels.db")

# # Configure logging
# # LOG_FILE = "database.log"
# # logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# # Configure logging (Ensure it writes to `database.log`)

# # Configure logging for `database.log`
# LOG_FILE = "database.log"
# logger = logging.getLogger("database")  # Use a named logger for database logs
# logger.setLevel(logging.INFO)

# # Ensure handlers don't duplicate logs
# if not logger.handlers:
#     file_handler = logging.FileHandler(LOG_FILE)
#     file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
#     logger.addHandler(file_handler)

# def init_db():
#     """Initialize the SQLite database."""
#     with sqlite3.connect(DB_FILE) as conn:
#         cursor = conn.cursor()
#         cursor.execute('''
#             CREATE TABLE IF NOT EXISTS parcels (
#                 id INTEGER PRIMARY KEY,  -- Keep the original ID from Capstone
#                 barcode TEXT UNIQUE,
#                 scan_status TEXT,
#                 last_scanned_when TEXT,
#                 address_name TEXT,
#                 address1 TEXT,
#                 address2 TEXT,
#                 city TEXT,
#                 state TEXT,
#                 zip TEXT,
#                 pod TEXT
#             )
#         ''')
#         conn.commit()
#         logger.info("✅ Database initialized.")

# def get_parcels(sort_by="barcode", order="asc", limit=100, city=None, state=None, scan_status=None, parcel_id=None):
#     """Fetch parcels with optional sorting, limits, and filtering by city, state, scan_status, or ID."""
#     try:
#         with sqlite3.connect(DB_FILE) as conn:
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
#         with sqlite3.connect(DB_FILE) as conn:
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
#     with sqlite3.connect(DB_FILE) as conn:
#         cursor = conn.cursor()

#         # Fetch only IDs, barcodes, statuses, and timestamps from the database
#         cursor.execute("SELECT id, barcode, scan_status, last_scanned_when FROM parcels")
#         existing_data = {
#             row[1]: {"id": row[0], "status": row[2], "timestamp": row[3]}
#             for row in cursor.fetchall()
#         }

#         updates = []
#         inserts = []
#         skipped = 0  # Track skipped updates

#         for parcel in updated_parcels:
#             parcel_id = parcel.get("id")  # Keep the original ID
#             barcode = parcel["barcode"]
#             new_status = parcel["scanStatus"]
#             new_timestamp = parcel["lastScannedWhen"]["formattedDate"] + " " + parcel["lastScannedWhen"]["formattedTime"]

#             if barcode in existing_data:
#                 existing = existing_data[barcode]
#                 if new_status != existing["status"] or new_timestamp != existing["timestamp"]:
#                     updates.append((
#                         new_status, new_timestamp,
#                         parcel["address"]["name"], parcel["address"]["address1"], parcel["address"]["address2"],
#                         parcel["address"]["city"], parcel["address"]["state"], parcel["address"]["zip"],
#                         parcel.get("pod"), barcode
#                     ))
#                 else:
#                     skipped += 1  # Count records that didn't change

#             else:
#                 # Insert new records
#                 inserts.append((
#                     parcel_id, barcode, new_status, new_timestamp,
#                     parcel["address"]["name"], parcel["address"]["address1"], parcel["address"]["address2"],
#                     parcel["address"]["city"], parcel["address"]["state"], parcel["address"]["zip"],
#                     parcel.get("pod")
#                 ))

#         # Perform batch updates
#         if updates:
#             cursor.executemany("""
#                 UPDATE parcels 
#                 SET scan_status = ?, last_scanned_when = ?, address_name = ?, address1 = ?, address2 = ?, 
#                     city = ?, state = ?, zip = ?, pod = ?
#                 WHERE barcode = ?
#             """, updates)

#         # Perform batch inserts
#         if inserts:
#             cursor.executemany("""
#                 INSERT INTO parcels (id, barcode, scan_status, last_scanned_when, address_name, address1, address2, city, state, zip, pod) 
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#             """, inserts)

#         conn.commit()
        
#         # logger details
#         logger.info(f"🔄 {len(updates)} parcels updated, {len(inserts)} new parcels inserted, {skipped} unchanged.")

#         # Log updated barcodes
#         if updates:
#             updated_barcodes = [record[-1] for record in updates]
#             logger.info(f"✅ Updated barcodes: {', '.join(updated_barcodes)}")

#         # Log inserted barcodes
#         if inserts:
#             inserted_barcodes = [record[1] for record in inserts]
#             logger.info(f"🆕 Inserted barcodes: {', '.join(inserted_barcodes)}")

#         # Log skipped updates
#         if skipped:
#             logger.info(f"⚡ Skipped {skipped} records (no changes detected).")

from fastapi import HTTPException
import sqlite3
import logging
from pathlib import Path

# # Paths
# BASE_DIR = Path(__file__).resolve().parent
# DB_FILE = BASE_DIR / "data/parcels.db"

DB_FILE = Path("/dataB/parcels.db")

# Configure logging
LOG_FILE = "database.log"
logger = logging.getLogger("database")  # Use a named logger for database logs
logger.setLevel(logging.INFO)

# Ensure handlers don't duplicate logs
if not logger.handlers:
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

def get_db_connection():
    """Return a new database connection with a timeout to prevent locking issues."""
    return sqlite3.connect(DB_FILE, timeout=10, isolation_level=None)

def init_db():
    """Initialize the SQLite database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parcels (
                id INTEGER PRIMARY KEY,  -- Keep the original ID from Capstone
                barcode TEXT,
                scan_status TEXT,
                last_scanned_when TEXT,
                address_name TEXT,
                address1 TEXT,
                address2 TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                pod TEXT,
                inserted_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_barcode ON parcels (barcode)")
        conn.commit()
        logger.info("✅ Database initialized.")

def get_parcels_week(begin_date, end_date):
    """Fetch parcels within the given begin_date and end_date."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Query to fetch parcels within the date range
            query = """
                SELECT * FROM parcels
                WHERE inserted_at BETWEEN ? AND ?
                ORDER BY inserted_at ASC
            """
            params = (begin_date, end_date)

            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]

            logger.info(f"📦 Fetched {len(result)} parcels between {begin_date} and {end_date}.")
            return result
    except Exception as e:
        logger.error(f"❌ Error fetching parcels for the week: {e}")
        return []

def get_parcels(sort_by="barcode", order="asc", limit=1000, city=None, state=None, scan_status=None, parcel_id=None):
    """Fetch parcels with optional sorting, limits, and filtering by city, state, scan_status, or ID."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # ✅ Only allow these fields to prevent SQL Injection
            valid_sort_columns = {"id", "barcode", "scan_status", "last_scanned_when", "city", "state"}
            if sort_by not in valid_sort_columns:
                sort_by = "barcode"  # Default fallback
            
            # ✅ Enforce ASC or DESC only
            order_clause = "ASC" if order.lower() == "asc" else "DESC"
            
            # Base query
            query = "SELECT * FROM parcels WHERE 1=1"
            params = []

            # Apply filters if provided
            if city:
                query += " AND city LIKE ?"
                params.append(f"%{city}%")  # Partial match
            
            if state:
                query += " AND state LIKE ?"
                params.append(f"%{state}%")
            
            if scan_status:
                query += " AND scan_status LIKE ?"
                params.append(f"%{scan_status}%")

            if parcel_id:
                query += " AND id = ?"
                params.append(parcel_id)
            
            # Sorting
            query += f" ORDER BY {sort_by} {order_clause} LIMIT ?"
            params.append(limit)

            cursor.execute(query, tuple(params))
            columns = [desc[0] for desc in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]

            logger.info(f"Fetched {len(result)} parcels from the database.")
            return result
    except Exception as e:
        logger.error(f"Error fetching parcels: {e}")
        return []

def get_parcel_by_barcode(barcode):
    """Fetch a specific parcel by barcode."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM parcels WHERE barcode = ?", (barcode,))
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                logger.info(f"Fetched parcel with barcode {barcode}")
                return dict(zip(columns, row))
            else:
                logger.warning(f"Parcel not found with barcode {barcode}")
                return None
    except Exception as e:
        logger.error(f"Error fetching parcel by barcode {barcode}: {e}")
        return None

# def update_parcels(updated_parcels):
#     """Efficiently update the database with new or modified parcel records."""
#     with get_db_connection() as conn:
#         try:
#             cursor = conn.cursor()

#             # Fetch only IDs, barcodes, statuses, and timestamps from the database
#             cursor.execute("SELECT id, scan_status, last_scanned_when FROM parcels")
#             existing_data = {row[0]: {"status": row[1], "timestamp": row[2]} for row in cursor.fetchall()}

#             updates = []
#             inserts = []
#             skipped = 0  # Track skipped updates

#             for parcel in updated_parcels:
#                 parcel_id = parcel.get("id")  # Keep the original ID
#                 barcode = parcel["barcode"]
#                 new_status = parcel["scanStatus"]
#                 new_timestamp = parcel["lastScannedWhen"]["formattedDate"] + " " + parcel["lastScannedWhen"]["formattedTime"]

#                 change_log = []  # Track specific changes for this parcel

#                 if parcel_id in existing_data:
#                     existing = existing_data[barcode]
#                     if new_status != existing["status"]:
#                         change_log.append(f"status changed from '{existing['status']}' to '{new_status}'")
#                     if new_timestamp != existing["timestamp"]:
#                         change_log.append(f"last scanned time changed from '{existing['timestamp']}' to '{new_timestamp}'")

#                     if change_log:  # Only update if there are actual changes
#                         updates.append((
#                             new_status, new_timestamp,
#                             parcel["address"]["name"], parcel["address"]["address1"], parcel["address"]["address2"],
#                             parcel["address"]["city"], parcel["address"]["state"], parcel["address"]["zip"],
#                             parcel.get("pod"), barcode
#                         ))
#                         logger.info(f"📦 Parcel {barcode} updated: {', '.join(change_log)}")
#                     else:
#                         skipped += 1  # Count records that didn't change
#                         logger.info(f"⚡ Parcel {barcode} skipped (no changes detected).")
#                 # if barcode in existing_data:
#                 #     existing = existing_data[barcode]
#                 #     if new_status != existing["status"] or new_timestamp != existing["timestamp"]:
#                 #         updates.append((
#                 #             new_status, new_timestamp,
#                 #             parcel["address"]["name"], parcel["address"]["address1"], parcel["address"]["address2"],
#                 #             parcel["address"]["city"], parcel["address"]["state"], parcel["address"]["zip"],
#                 #             parcel.get("pod"), barcode
#                 #         ))
#                 #     else:
#                 #         skipped += 1  # Count records that didn't change
#                 else:
#                     # Insert new records
#                     inserts.append((
#                         parcel_id, barcode, new_status, new_timestamp,
#                         parcel["address"]["name"], parcel["address"]["address1"], parcel["address"]["address2"],
#                         parcel["address"]["city"], parcel["address"]["state"], parcel["address"]["zip"],
#                         parcel.get("pod")
#                     ))

#             # Perform batch updates
#             if updates:
#                 cursor.executemany("""
#                     UPDATE parcels 
#                     SET scan_status = ?, last_scanned_when = ?, address_name = ?, address1 = ?, address2 = ?, 
#                         city = ?, state = ?, zip = ?, pod = ?
#                     WHERE id = ?
#                 """, updates)

#             # Perform batch inserts
#             if inserts:
#                 cursor.executemany("""
#                     INSERT INTO parcels (id, barcode, scan_status, last_scanned_when, address_name, address1, address2, city, state, zip, pod) 
#                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                     ON CONFLICT(id) DO UPDATE SET 
#                         scan_status = excluded.scan_status,
#                         last_scanned_when = excluded.last_scanned_when,
#                         address_name = excluded.address_name,
#                         address1 = excluded.address1,
#                         address2 = excluded.address2,
#                         city = excluded.city,
#                         state = excluded.state,
#                         zip = excluded.zip,
#                         pod = excluded.pod
#                 """, inserts)

#             conn.commit()
            
#             # Logging details
#             logger.info(f"🔄 {len(updates)} parcels updated, {len(inserts)} new parcels inserted, {skipped} unchanged.")
            

#             # Log updated barcodes
#             if updates:
#                 updated_barcodes = [record[-1] for record in updates]
#                 logger.info(f"✅ Updated barcodes: {', '.join(updated_barcodes)}")

#             # Log inserted barcodes
#             if inserts:
#                 inserted_barcodes = [record[1] for record in inserts]
#                 logger.info(f"🆕 Inserted barcodes: {', '.join(inserted_barcodes)}")

#             # Log skipped updates
#             if skipped:
#                 logger.info(f"⚡ Skipped {skipped} records (no changes detected).")

#             # ✅ **Return the structured response**
#             return {
#                 "updated": len(updates),
#                 "inserted": len(inserts),
#                 "skipped": skipped
#             }


#         except Exception as e:
#             conn.rollback()  
#             logger.error(f"❌ Database error: {e}")
#             raise HTTPException(status_code=500, detail=f"Database update failed: {str(e)}")  

def update_parcels(updated_parcels):
    """Efficiently update the database with new or modified parcel records."""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()

            updates = 0
            inserts = 0
            skipped = 0  # Track skipped updates

            for parcel in updated_parcels:
                parcel_id = parcel.get("id")  # Keep the original ID
                barcode = parcel["barcode"]
                new_status = parcel["scanStatus"]
                new_timestamp = parcel["lastScannedWhen"]["formattedDate"] + " " + parcel["lastScannedWhen"]["formattedTime"]

                # Check if record exists based on ID
                cursor.execute("SELECT id, scan_status, last_scanned_when FROM parcels WHERE id = ?", (parcel_id,))
                existing_record = cursor.fetchone()

                if existing_record:
                    existing_id, existing_status, existing_timestamp = existing_record

                    # Track changes before updating
                    change_log = []
                    if new_status != existing_status:
                        change_log.append(f"status changed from '{existing_status}' to '{new_status}'")
                    if new_timestamp != existing_timestamp:
                        change_log.append(f"last scanned time changed from '{existing_timestamp}' to '{new_timestamp}'")

                    if change_log:
                        # Update existing record
                        cursor.execute("""
                            UPDATE parcels SET 
                                scan_status = ?, 
                                last_scanned_when = ?, 
                                address_name = ?, 
                                address1 = ?, 
                                address2 = ?, 
                                city = ?, 
                                state = ?, 
                                zip = ?, 
                                pod = ?
                            WHERE id = ?
                        """, (new_status, new_timestamp, parcel["address"]["name"], parcel["address"]["address1"], 
                              parcel["address"]["address2"], parcel["address"]["city"], parcel["address"]["state"], 
                              parcel["address"]["zip"], parcel.get("pod"), parcel_id))
                        updates += 1
                        logger.info(f"📦 Parcel {parcel_id} updated: {', '.join(change_log)}")
                    else:
                        skipped += 1  # No changes detected
                        logger.info(f"⚡ Parcel {parcel_id} skipped (no changes detected).")

                else:
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO parcels (id, barcode, scan_status, last_scanned_when, address_name, address1, address2, city, state, zip, pod, inserted_at) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                    """, (parcel_id, barcode, new_status, new_timestamp, parcel["address"]["name"], 
                          parcel["address"]["address1"], parcel["address"]["address2"], parcel["address"]["city"], 
                          parcel["address"]["state"], parcel["address"]["zip"], parcel.get("pod")))
                    inserts += 1
                    logger.info(f"🆕 New parcel inserted: {parcel_id} (Barcode: {barcode})")

            conn.commit()

            # Logging summary
            logger.info(f"🔄 {updates} parcels updated, {inserts} new parcels inserted, {skipped} unchanged.")

            

            return {
                "updated": updates,
                "inserted": inserts,
                "skipped": skipped
            }

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Database error: {e}")
            raise HTTPException(status_code=500, detail=f"Database update failed: {str(e)}")