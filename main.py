# LIVE API CALL ONLY USE IN PRODUCTION



# from fastapi import FastAPI, HTTPException
# import requests
# import time

# # Initialize FastAPI app
# app = FastAPI()

# # API Credentials
# AUTH_URL = "https://03830.cxtsoftware.net/CxtWebService/CXTWCF.svc/v2/Authentication/InetUser"
# API_URL = "https://03830.cxtsoftware.net/CxtWebService/CXTWCF.svc/v2/Parcels/Track/Summaries"

# # Credentials for authentication
# CREDENTIALS = {
#     "username": "cbadistribution",
#     "password": "cbadistribution1"
# }

# # Global token storage
# TOKEN = None

# def authenticate():
#     """Fetches a new authentication token."""
#     global TOKEN
#     headers = {
#         "Content-Type": "application/json",
#         "Accept": "application/json"
#     }
#     response = requests.post(AUTH_URL, json=CREDENTIALS, headers=headers)

#     if response.status_code == 200:
#         TOKEN = response.json().get("token")
#         return TOKEN
#     else:
#         raise HTTPException(status_code=response.status_code, detail="Authentication failed")


# def fetch_parcel_summaries():
#     """Fetches parcel summaries using the latest token."""
#     global TOKEN

#     if not TOKEN:
#         authenticate()

#     headers = {
#         "Content-Type": "application/json",
#         "Accept": "application/json",
#         "Authorization": f"Token {TOKEN}"
#     }

#     params = {
#         "includePartialMatch": "false",
#         "beginDate": "2025-01-12",
#         "endDate": "2025-03-12",
#         "numRecentParcels": 100,
#         "pageNum": 1,
#         "pageSize": 100,
#         "sortColumn": "BARCODE",
#         "sortDirection": "ASCENDING"
#     }

#     response = requests.post(API_URL, headers=headers, params=params)

#     if response.status_code == 200:
#         return response.json()
#     elif response.status_code == 401:
#         TOKEN = authenticate()  # Refresh token and retry
#         return fetch_parcel_summaries()
#     elif response.status_code == 429:
#         time.sleep(5)  # Handle rate limit
#         return fetch_parcel_summaries()
#     else:
#         raise HTTPException(status_code=response.status_code, detail=response.text)


# @app.get("/parcels")
# def get_parcel_data():
#     """API endpoint to fetch parcel summaries"""
#     return fetch_parcel_summaries()


# @app.get("/")
# def root():
#     return {"message": "Welcome to Parcel API 🚀"}

# -----------------------------------------------------------------------------------

# import logging
# import sys
# from fastapi.logger import logger as fastapi_logger

# from fastapi import FastAPI, HTTPException, Query, Request
# from apscheduler.schedulers.background import BackgroundScheduler
# from datetime import datetime, timedelta
# from database import get_parcels, get_parcel_by_barcode, update_parcels

# # Prevent database logs from being logged in `server.log`
# logging.getLogger("database").propagate = False  

# for handler in logging.root.handlers[:]:
#     logging.root.removeHandler(handler)

# # Configure Logging
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[
#         logging.FileHandler("server.log"),  # ✅ Logs to file
#         logging.StreamHandler(sys.stdout)  # ✅ Logs to console
#     ]
# )

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

# app = FastAPI()

# logger.info("🚀 FastAPI Server is starting...")

# # Scheduler Configuration
# SCHEDULED_CALLS = 30  # 30 updates per day
# WINDOW_HOURS = 14  # From 6 AM - 8 PM
# CALL_INTERVAL = int((WINDOW_HOURS * 60) / SCHEDULED_CALLS)  # ≈ 28 minutes per update

# # 🔹 Background Scheduler for Database Updates
# scheduler = BackgroundScheduler()

# def fetch_and_update_parcels():
#     """Fetch new data and update only modified records in the database."""
#     logging.info("🚀 Fetching updated parcels from external source.")

#     # Temporary placeholder until real API is connected
#     updated_parcels = [
#         {
#             "id": 994499,  # Simulated ID
#             "barcode": "TEST123456789",
#             "scanStatus": "Delivered",
#             "lastScannedWhen": {
#                 "formattedDate": "2025-02-13",
#                 "formattedTime": "12:00 PM"
#             },
#             "address": {
#                 "name": "Test Location",
#                 "address1": "123 Test St",
#                 "address2": "",
#                 "city": "Test City",
#                 "state": "NY",
#                 "zip": "12345"
#             },
#             "pod": "John Doe"
#         }
#     ]  # Simulate getting updated parcels
    
#     # Replace this with an actual API call when switching to production
#     # updated_parcels = get_updated_parcels_from_capstone()  # Simulated function
    
#     # Update database
#     update_parcels(updated_parcels)
    
#     logging.info("✅ Database successfully updated with new parcel data.")

# def schedule_updates():
#     """Schedule database updates at fixed intervals on Tuesdays & Wednesdays."""
#     scheduler.remove_all_jobs()  # Remove old jobs
    
#     now = datetime.now()
#     if now.weekday() in [1, 2]:  # 1 = Tuesday, 2 = Wednesday
#         for i in range(SCHEDULED_CALLS):
#             next_run = now.replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(minutes=i * CALL_INTERVAL)
#             if next_run > now:
#                 scheduler.add_job(fetch_and_update_parcels, 'date', run_date=next_run)
#                 logging.info(f"📅 Scheduled database update at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    
#     scheduler.start()

# # Start scheduling
# schedule_updates()

# # 🔹 FastAPI Endpoints
# @app.get("/")
# def home():
#     logging.info("🏠 Home endpoint accessed.")
#     return {"message": "FastAPI Database Server is Running"}

# @app.get("/parcels")
# def get_parcels_api(
#     request: Request,
#     sort_by: str = Query("barcode", description="Sort key (e.g., barcode, city, state)"),
#     order: str = Query("asc", description="Sort order: 'asc' or 'desc'"),
#     limit: int = Query(100, description="Max number of results"),
#     city: str = Query(None, description="Filter by city"),
#     state: str = Query(None, description="Filter by state"),
#     scan_status: str = Query(None, description="Filter by scan status"),
#     parcel_id: int = Query(None, description="Filter by specific parcel ID")
# ):
#     # Convert query params to a string
#     query_params_str = request.query_params if request.query_params else "No filters applied"
#     """Fetch parcels with optional sorting, limits, and filtering."""
#     logging.info(f"📦 GET /parcels called with {query_params_str}")
#     return get_parcels(sort_by, order, limit, city, state, scan_status, parcel_id)

# @app.get("/parcels/{barcode}")
# def get_parcel_by_barcode_endpoint(barcode: str):
#     """Fetch a specific parcel by barcode."""
#     logging.info(f"🔍 Searching for parcel with barcode: {barcode}")
#     parcel = get_parcel_by_barcode(barcode)
#     if not parcel:
#         logging.warning(f"⚠️ Parcel with barcode {barcode} not found.")
#         raise HTTPException(status_code=404, detail="Parcel not found")
#     return parcel

# @app.post("/reload")
# def trigger_manual_update():
#     """Manually trigger a database update."""
#     logging.info("🔄 Manual database update triggered.")
#     fetch_and_update_parcels()
#     return {"message": "Database update triggered"}

import logging
import sys
import requests
import os
import pytz
from dotenv import load_dotenv
import uvicorn
import shutil
from fastapi import FastAPI, HTTPException, Query, Request, Depends, BackgroundTasks, UploadFile, File
from fastapi.security.api_key import APIKeyHeader
from pathlib import Path
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from slowapi import Limiter
from slowapi.util import get_remote_address
from database import get_parcels, get_parcel_by_barcode, update_parcels, get_parcels_week, init_db

# Prevent database logs from being logged in `server.log`
logging.getLogger("database").propagate = False  

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Load environment variables
load_dotenv()

# API Credentials
AUTH_URL = os.getenv("AUTH_URL")
API_URL = os.getenv("API_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
RAILWAY_API_KEY = os.getenv("RAILWAY_API_KEY")  # Set this in Railway environment variables
PROJECT_ID = os.getenv("RAILWAY_PROJECT_ID")    # Set this in Railway environment variables
SERVICE_ID = os.getenv("RAILWAY_SERVICE_ID")    # Set this in Railway environment variables
LOGS_API_KEY = os.getenv("LOGS_API_KEY")
API_KEY_HEADER = APIKeyHeader(name="X-API-Key")


# Global token storage
TOKEN = None

LOGS_PATH = Path(__file__).parent
SERVER_LOG_PATH = LOGS_PATH / "server.log"
DATABASE_LOG_PATH = LOGS_PATH / "database.log"
DB_FILE = Path("/dataB/parcels.db")

# Define your desired timezone (change if needed)
LOCAL_TZ = pytz.timezone("America/New_York")  # Adjust based on your timezone

def get_local_time():
    """Returns the current time in the local timezone."""
    return datetime.now(pytz.utc).astimezone(LOCAL_TZ)

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("server.log"),  # ✅ Logs to file
        logging.StreamHandler(sys.stdout)  # ✅ Logs to console
    ]
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

limiter = Limiter(key_func=get_remote_address)  # ✅ Rate limiter to prevent abuse

# 🚀 FastAPI Server Start
logger.info("🚀 FastAPI Server is starting...")

# Security check
def verify_api_key(api_key: str = Depends(API_KEY_HEADER)):
    if api_key != LOGS_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# Read log file helper function
def read_log_file(file_path: Path, lines: int = 100):
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Log file not found: {file_path.name}")

    with file_path.open('r') as file:
        log_lines = file.readlines()
        return log_lines[-lines:]


def authenticate():
    """Fetches a new authentication token."""
    global TOKEN
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    credentials = {
        "username": USERNAME,
        "password": PASSWORD
    }
    response = requests.post(AUTH_URL, json=credentials, headers=headers)

    if response.status_code == 200:
        TOKEN = response.json().get("token")
        return TOKEN
    else:
        raise HTTPException(status_code=response.status_code, detail="Authentication failed")
    
def fetch_parcel_summaries():
    """Fetches parcel summaries using the latest token."""
    global TOKEN

    if not TOKEN:
        authenticate()

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {TOKEN}"
    }

    params = {
        "includePartialMatch": "true",
        "beginDate": begin_date,
        "endDate": end_date,
        # "numRecentParcels": 3000,
        "pageNum": 1,
        "pageSize": 3000,
        "sortColumn": "BARCODE",
        "sortDirection": "ASCENDING",
        "filter" : "ALL"
    }

    payload = [
        {
            "searchColumn": "BARCODE",
            "searchValues": ["99M"]
        }
    ]


    response = requests.post(API_URL, headers=headers, params=params, json=payload)

    logging.info(f"🛜 API Status Code: {response.status_code}")
    # if response.status_code = 401
    #     authenticate()
    logging.info(f"📩 API Response Content: {response.text[:500]}")  # Log first 500 chars

    # Handle expired token case
    if response.status_code == 401:
        logging.warning("🔑 Token expired. Re-authenticating...")
        TOKEN = authenticate()  # Refresh token
        headers["Authorization"] = f"Token {TOKEN}"  # Update with new token
        response = requests.post(API_URL, headers=headers, params=params, json=payload)
        logging.info(f"🔄 Retried API call, Status Code: {response.status_code}")

    # Handle rate limiting
    elif response.status_code == 429:
        logging.warning("⏳ Rate limit reached. Retrying in 5 seconds...")
        time.sleep(5)
        return fetch_parcel_summaries()

    # Raise exception for unexpected errors
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    response_json = response.json()
    logging.info(f"📩 Parsed API Response Type: {type(response_json)}")
    return response_json

    # if response.status_code == 200:
    #     return response.json()
    # elif response.status_code == 401:
    #     TOKEN = authenticate()  # Refresh token and retry
    #     return fetch_parcel_summaries()
    # elif response.status_code == 429:
    #     time.sleep(5)  # Handle rate limit
    #     return fetch_parcel_summaries()
    # else:
    #     raise HTTPException(status_code=response.status_code, detail=response.text)

# ✅ Create a single instance of BackgroundScheduler
# scheduler = BackgroundScheduler(daemon=True)

# Scheduler Configuration
SCHEDULED_CALLS = 30  # 30 updates per day
WINDOW_HOURS = 14  # From 6 AM - 8 PM
CALL_INTERVAL = int((WINDOW_HOURS * 60) / SCHEDULED_CALLS)  # ≈ 28 minutes per update

# 🔹 Background Scheduler for Database Updates
scheduler = BackgroundScheduler(daemon=True)

def fetch_and_update_parcels():
    """Fetch new data and update only modified records in the database."""
    logging.info("🚀 Fetching updated parcels from external source.")

    # # Temporary placeholder until real API is connected
    # updated_parcels = [
    #     {
    #         "id": 456456654,  # Simulated ID
    #         "barcode": "Bob132343443333244444333456789",
    #         "scanStatus": "Picked up",
    #         "lastScannedWhen": {
    #             "formattedDate": "2025-02-26",
    #             "formattedTime": "12:00 PM"
    #         },
    #         "address": {
    #             "name": "Test Location",
    #             "address1": "123 Test St",
    #             "address2": "",
    #             "city": "Test City",
    #             "state": "NY",
    #             "zip": "12345"
    #         },
    #         "pod": "John Doe"
    #     }
    # ]  # Simulate getting updated parcels

    # Replace this with an actual API call when switching to production
    # updated_parcels = get_updated_parcels_from_capstone()  # Simulated function

    try:
        updated_parcels = fetch_parcel_summaries()  # Fetch real data
    except HTTPException as e:
        logging.error(f"❌ API Error: {e.detail}")
        return {"updated": 0, "inserted": 0, "skipped": 0}

    # ✅ Ensure response is valid
    if not updated_parcels or "parcelSummaries" not in updated_parcels:
        logging.warning("⚠️ No valid data received from API.")
        return {"updated": 0, "inserted": 0, "skipped": 0}

    parcels_list = updated_parcels.get("parcelSummaries", [])

    # ✅ Ensure the parcel list is valid
    if not isinstance(parcels_list, list):
        logging.error(f"❌ API returned an invalid parcel list: {parcels_list}")
        return {"updated": 0, "inserted": 0, "skipped": 0}

    # ✅ Process all parcels, ensuring missing fields have default values
    processed_parcels = []
    for parcel in parcels_list:
        if not isinstance(parcel, dict):
            logging.error(f"❌ Invalid parcel format: {parcel}")
            continue  # Skip non-dictionary entries

        # ✅ Ensure lastScannedWhen is a dictionary before accessing keys
        last_scanned = parcel.get("lastScannedWhen", {})
        if not isinstance(last_scanned, dict):  
            last_scanned = {}

        # ✅ Ensure address is a dictionary before accessing keys
        address_info = parcel.get("address", {})
        if not isinstance(address_info, dict):
            address_info = {}

        processed_parcels.append({
            "id": parcel.get("id", -1),  # Default ID to -1 if missing
            "barcode": parcel.get("barcode", "UNKNOWN"),  # Default barcode if missing
            "scanStatus": parcel.get("scanStatus", "Unknown"),  # Default scan status
            "lastScannedWhen": {
                "formattedDate": last_scanned.get("formattedDate", "0000-00-00"),
                "formattedTime": last_scanned.get("formattedTime", "00:00 AM")
            },
            "address": {
                "name": address_info.get("name", "Unknown Location"),
                "address1": address_info.get("address1", ""),
                "address2": address_info.get("address2", ""),
                "city": address_info.get("city", "Unknown City"),
                "state": address_info.get("state", "Unknown State"),
                "zip": address_info.get("zip", "00000")
            },
            "pod": parcel.get("pod", "N/A")  # Default proof of delivery
        })

    if not processed_parcels:
        logging.warning("⚠️ No valid parcels found after processing.")
        return {"updated": 0, "inserted": 0, "skipped": 0}

    # ✅ Update database with all valid records
    result = update_parcels(processed_parcels)

    if not isinstance(result, dict):
        logging.error("❌ Error: update_parcels() did not return a dictionary!")
        return {"updated": 0, "inserted": 0, "skipped": 0}  # Return safe default

    # ✅ Log update summary when triggered by the scheduler
    logging.info(f"✅ Update completed: {result['updated']} updated, {result['inserted']} inserted, {result['skipped']} unchanged.")
    logging.info("✅ Database successfully updated with new parcel data.")

    # ✅ Return structured update information
    return result
    

# def schedule_updates():
#     """Schedule database updates at fixed intervals on Tuesdays & Wednesdays."""
#     scheduler.remove_all_jobs()  # Remove old jobs
    
#     now = datetime.now()
#     if now.weekday() in [1, 2]:  # 1 = Tuesday, 2 = Wednesday
#         for i in range(SCHEDULED_CALLS):
#             next_run = now.replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(minutes=i * CALL_INTERVAL)
#             if next_run > now:
#                 scheduler.add_job(fetch_and_update_parcels, 'date', run_date=next_run)
#                 logging.info(f"📅 Scheduled database update at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    
#     scheduler.start()

# def schedule_updates():
#     """Schedule database updates at fixed intervals on Tuesdays & Wednesdays, preventing duplicates."""
#     scheduler.remove_all_jobs()  # Remove old jobs to avoid duplicates

#     now = datetime.now()
#     if now.weekday() in [1, 2]:  # 1 = Tuesday, 2 = Wednesday
#         existing_jobs = {job.id for job in scheduler.get_jobs()}  # Get all existing job IDs

#         for i in range(SCHEDULED_CALLS):
#             next_run = now.replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(minutes=i * CALL_INTERVAL)
            
#             if next_run > now:
#                 job_id = f"update_{next_run.strftime('%Y-%m-%d_%H-%M')}"
                
#                 if job_id not in existing_jobs:  # ✅ Prevents duplicate jobs
#                     scheduler.add_job(fetch_and_update_parcels, 'date', run_date=next_run, id=job_id)
#                     logging.info(f"📅 Scheduled database update at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    
#     scheduler.start()
def get_weekly_date_range():
    """Dynamically calculate beginDate (Saturday) and endDate (Thursday) of the current week."""
    today = datetime.today()
    weekday = today.weekday()  # Monday = 0, Sunday = 6

    # Find last Saturday (if today is Saturday, use today)
    last_saturday = today - timedelta(days=(weekday - 5) % 7)

    # Find upcoming Thursday (if today is Thursday, use today)
    next_thursday = last_saturday + timedelta(days=5)

    return last_saturday.strftime("%Y-%m-%d"), next_thursday.strftime("%Y-%m-%d")

#  ✅ Generate dynamic beginDate and endDate
begin_date, end_date = get_weekly_date_range()
logging.info(f"📅 Dynamic API Date Range: {begin_date} to {end_date}")



# def schedule_updates():
#     """Schedule database updates at fixed intervals on Tuesdays & Wednesdays, preventing duplicates."""
#     scheduler.remove_all_jobs()  # ✅ Ensure old jobs are removed to avoid duplication
#     job_ids = {job.id for job in scheduler.get_jobs()}  # ✅ Get all existing job IDs

#     # now = datetime.now()
#     now_utc = datetime.now(pytz.utc)  # Get current UTC time
#     now_local = now_utc.astimezone(LOCAL_TZ)  # Convert to local time
#     # if now_local.weekday() in [1, 2]:  # ✅ Only schedule on Tuesdays & Wednesdays
#     if now_local.weekday() in [0, 1, 2, 3]:  # ✅ Only schedule on Sunday, Monday, Tuesday, and Wednesday
#         for i in range(30):  # ✅ SCHEDULED_CALLS is always 30
#             next_run = now_local.replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(minutes=i * 28)
#             job_id = f"update_{next_run.strftime('%Y-%m-%d_%H-%M')}"

#             if next_run > now_local and job_id not in job_ids:
#                 scheduler.add_job(fetch_and_update_parcels, 'date', run_date=next_run, id=job_id)
#                 logging.info(f"📅 Scheduled database update at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
#             else:
#                 logging.info(f"⏩ Skipped scheduling past job: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")    

def schedule_updates():
    """Schedule database updates at fixed intervals on Sundays, Mondays, Tuesdays, and Wednesdays, preventing duplicates."""
    job_ids = {job.id for job in scheduler.get_jobs()}  # ✅ Get all existing job IDs

    now_utc = datetime.now(pytz.utc)  # Get current UTC time
    now_local = now_utc.astimezone(LOCAL_TZ)  # Convert to local time

    # ✅ Run only on Sunday, Monday, Tuesday, and Wednesday
    if now_local.weekday() in [0, 1, 2, 3]:  
        next_run = now_local.replace(hour=6, minute=0, second=0, microsecond=0)  # Start at 6 AM

        for i in range(30):  # ✅ SCHEDULED_CALLS is always 30
            next_run += timedelta(minutes=28)  # ✅ Increment for the next run

            job_id = f"update_{next_run.strftime('%Y-%m-%d_%H-%M')}"

            # ✅ Ensure it's scheduled for the future
            if next_run > now_local and job_id not in job_ids:
                scheduler.add_job(fetch_and_update_parcels, 'date', run_date=next_run, id=job_id)
                logging.info(f"📅 Scheduled database update at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                logging.info(f"⏩ Skipped scheduling past job: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")    

# schedule_updates() 
# @app.on_event("startup")
# def startup_event():
#     logging.info("🚀 FastAPI Startup: Scheduling updates...")
#     if not scheduler.running:  # ✅ Prevent scheduler from starting multiple times
#         scheduler.start()
#     schedule_updates()  # ✅ Start scheduler at FastAPI startup

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("🚀 FastAPI Startup: Scheduling updates...")
    if not scheduler.running:  # ✅ Prevent scheduler from starting multiple times
        scheduler.start()
    schedule_updates()  # ✅ Start scheduler at FastAPI startup
    init_db()
    yield  # 🚀 This ensures proper cleanup when the app stops

app = FastAPI(lifespan=lifespan)  # ✅ Use new lifespan method

# 🔹 FastAPI Endpoints
@app.get("/")
@limiter.limit("10/minute")  # ✅ Limit home page calls to 10 per minute
def home(request: Request):
    logging.info("🏠 Home endpoint accessed.")
    return {"message": "FastAPI Database Server is Running"}

@app.get("/jobs")
def get_scheduled_jobs():
    return [{"id": job.id, "run_date": str(job.next_run_time)} for job in scheduler.get_jobs()]

@app.get("/time")
def get_current_time():
    return {"server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

@app.get("/parcelsweek")
@limiter.limit("30/minute")  # ✅ Limit queries to 30 per minute
def get_parcels_week_api(request: Request):
    """Fetch all parcels scanned within the dynamically calculated date range."""
    global begin_date, end_date  # Use the previously calculated date range

    logging.info(f"📦 GET /parcelsweek called for range {begin_date} to {end_date}")

    # Fetch parcels from the database within the calculated date range
    return get_parcels_week(begin_date, end_date)

@app.get("/parcels")
@limiter.limit("30/minute")  # ✅ Limit parcel queries to 30 per minute
def get_parcels_api(
    request: Request,
    sort_by: str = Query("barcode", description="Sort key (e.g., barcode, city, state)"),
    order: str = Query("asc", description="Sort order: 'asc' or 'desc'"),
    limit: int = Query(1000, description="Max number of results"),
    city: str = Query(None, description="Filter by city"),
    state: str = Query(None, description="Filter by state"),
    scan_status: str = Query(None, description="Filter by scan status"),
    parcel_id: int = Query(None, description="Filter by specific parcel ID")
):
    # Convert query params to a string
    query_params_str = request.query_params if request.query_params else "No filters applied"
    """Fetch parcels with optional sorting, limits, and filtering."""
    logging.info(f"📦 GET /parcels called with {query_params_str}")
    return get_parcels(sort_by, order, limit, city, state, scan_status, parcel_id)

@app.get("/parcels/{barcode}")
@limiter.limit("30/minute")  # ✅ Limit barcode lookups to 30 per minute
def get_parcel_by_barcode_endpoint(request: Request, barcode: str):
    """Fetch a specific parcel by barcode."""
    try:
        logging.info(f"🔍 Searching for parcel with barcode: {barcode}")
        parcel = get_parcel_by_barcode(barcode)
        if not parcel:
            logging.warning(f"⚠️ Parcel with barcode {barcode} not found.")
            raise HTTPException(status_code=404, detail="Parcel not found")
        return parcel
    except Exception as e:
        logging.error(f"❌ Error fetching parcel {barcode}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error. Please try again later.")

@app.get("/logs/server", dependencies=[Depends(verify_api_key)])
def get_server_logs(lines: int = Query(100, description="Number of recent lines to retrieve from the server log")):
    return {
        "log_file": "server.log",
        "lines_requested": lines,
        "log_data": read_log_file(SERVER_LOG_PATH, lines)
    }

# Endpoint for database logs
# @app.get("/logs/database", dependencies=[Depends(verify_api_key)])
# def get_database_logs(lines: int = Query(100, description="Number of recent lines to retrieve from the database log")):
#     return {
#         "log_file": "database.log",
#         "lines_requested": lines,
#         "log_data": read_log_file(DATABASE_LOG_PATH, lines)
#     }

@app.get("/logs/database")
async def get_database_logs(api_key: str = Depends(verify_api_key)):
    log_path = "database.log"
    if not os.path.exists(log_path):
        return {"log_file": log_path, "exists": False, "log_data": []}

    with open(log_path, "r") as file:
        lines = file.readlines()

    return {
        "log_file": log_path,
        "exists": True,
        "lines_requested": 100,
        "log_data": lines[-100:]
    }

@app.get("/download-db", dependencies=[Depends(verify_api_key)])
async def download_db():
    if not os.path.exists(DB_FILE):
        raise HTTPException(status_code=404, detail="Database file not found")
    return FileResponse(DB_FILE, filename="parcels.db", media_type="application/octet-stream")

@app.post("/upload-db", dependencies=[Depends(verify_api_key)])
async def upload_database(file: UploadFile = File(...)):
    """Upload a new database file to replace the existing one in Railway."""
    try:
        with open(DB_FILE, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {"message": "Database uploaded successfully"}
    except Exception as e:
        return {"error": str(e)}

@app.post("/reload")
@limiter.limit("5/minute")  # ✅ Limit reloads to 5 per minute to avoid spam
def trigger_manual_update(request: Request):
    """Manually trigger a database update."""
    logging.info("🔄 Manual database update triggered.")
    # Fetch and update parcels
    updated_parcels = fetch_and_update_parcels()

    # Logging the result
    total_updated = updated_parcels.get("updated", 0)
    total_inserted = updated_parcels.get("inserted", 0)
    total_skipped = updated_parcels.get("skipped", 0)

    response_message = (
        f"✅ Update completed: {total_updated} updated, {total_inserted} inserted, {total_skipped} unchanged."
    )

    logging.info(response_message)
    return {"message": response_message}
    
    
    # fetch_and_update_parcels()
    # return {"message": "Database update triggered"}
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)