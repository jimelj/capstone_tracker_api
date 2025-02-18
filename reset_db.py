import sqlite3
import json
from pathlib import Path

# Database file path
DB_FILE = Path("parcels.db")
DATA_FILE = Path("backup/test.json")  # Modify this if switching to API later

def delete_database():
    """Completely remove the database file."""
    if DB_FILE.exists():
        DB_FILE.unlink()
        print("🗑️ Database file deleted.")

def init_db():
    """Recreate the SQLite database schema."""
    with sqlite3.connect(DB_FILE) as conn:
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
                pod TEXT
            )
        ''')
        conn.commit()
    print("✅ Database schema initialized.")

# def load_json_to_db():
#     """Load JSON data into SQLite."""
#     if not DATA_FILE.exists():
#         print("⚠️ No JSON file found. Skipping import.")
#         return

#     with sqlite3.connect(DB_FILE) as conn:
#         cursor = conn.cursor()
#         print("🔄 Loading JSON into SQLite...")

#         with open(DATA_FILE, "r") as f:
#             data = json.load(f)
#             parcels = data.get("parcelSummaries", [])

#             for parcel in parcels:
#                 parcel_id = parcel.get("id") #Keep original ID
#                 cursor.execute('''
#                     INSERT OR IGNORE INTO parcels 
#                     (id, barcode, scan_status, last_scanned_when, address_name, address1, address2, city, state, zip, pod) 
#                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                 ''', (
#                     parcel_id, # keep original ID
#                     parcel.get("barcode"),
#                     parcel.get("scanStatus"),
#                     parcel["lastScannedWhen"]["formattedDate"] + " " + parcel["lastScannedWhen"]["formattedTime"],
#                     parcel["address"]["name"],
#                     parcel["address"]["address1"],
#                     parcel["address"]["address2"],
#                     parcel["address"]["city"],
#                     parcel["address"]["state"],
#                     parcel["address"]["zip"],
#                     parcel.get("pod")
#                 ))
#         conn.commit()
#         print("✅ Data successfully loaded into SQLite.")

def reset_database():
    """Reset the database by deleting, recreating, and re-importing data."""
    print("🔄 Resetting the database...")

    # Step 1: Fully delete the database
    delete_database()

    # Step 2: Reinitialize the database structure
    init_db()

    # Step 3: Re-import fresh data from JSON
    # load_json_to_db()

    print("✅ Database reset and reloaded successfully.")

if __name__ == "__main__":
    reset_database()