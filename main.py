from fastapi import FastAPI, HTTPException
import requests
import time

# Initialize FastAPI app
app = FastAPI()

# API Credentials
AUTH_URL = "https://03830.cxtsoftware.net/CxtWebService/CXTWCF.svc/v2/Authentication/InetUser"
API_URL = "https://03830.cxtsoftware.net/CxtWebService/CXTWCF.svc/v2/Parcels/Track/Summaries"

# Credentials for authentication
CREDENTIALS = {
    "username": "cbadistribution",
    "password": "cbadistribution1"
}

# Global token storage
TOKEN = None

def authenticate():
    """Fetches a new authentication token."""
    global TOKEN
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    response = requests.post(AUTH_URL, json=CREDENTIALS, headers=headers)

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
        "includePartialMatch": "false",
        "beginDate": "2025-01-12",
        "endDate": "2025-03-12",
        "numRecentParcels": 100,
        "pageNum": 1,
        "pageSize": 100,
        "sortColumn": "BARCODE",
        "sortDirection": "ASCENDING"
    }

    response = requests.post(API_URL, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 401:
        TOKEN = authenticate()  # Refresh token and retry
        return fetch_parcel_summaries()
    elif response.status_code == 429:
        time.sleep(5)  # Handle rate limit
        return fetch_parcel_summaries()
    else:
        raise HTTPException(status_code=response.status_code, detail=response.text)


@app.get("/parcels")
def get_parcel_data():
    """API endpoint to fetch parcel summaries"""
    return fetch_parcel_summaries()


@app.get("/")
def root():
    return {"message": "Welcome to Parcel API 🚀"}