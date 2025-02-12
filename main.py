import requests
import time
import json

# API Credentials
AUTH_URL = "https://03830.cxtsoftware.net/CxtWebService/CXTWCF.svc/v2/Authentication/InetUser"
API_URL = "https://03830.cxtsoftware.net/CxtWebService/CXTWCF.svc/v2/Parcels/Track/Summaries"

# Credentials for authentication
CREDENTIALS = {
    "username": "cbadistribution",
    "password": "cbadistribution1"
}

# Global variable to store the token
TOKEN = None


def authenticate():
    """Fetches a new authentication token."""
    global TOKEN
    print("🔐 Authenticating...")
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    response = requests.post(AUTH_URL, json=CREDENTIALS, headers=headers)
    
    if response.status_code == 200:
        TOKEN = response.json().get("token")
        print(f"✅ New Token Obtained: {TOKEN}")
    else:
        print(f"❌ Authentication Failed! Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        exit()


def fetch_parcel_summaries():
    """Fetches parcel summaries using the latest token."""
    global TOKEN

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

    print("\n📦 Fetching Parcel Summaries...")
    print(f"🔄 Request Sent: {API_URL}")
    print(f"📩 Headers Sent: {json.dumps(headers, indent=4)}")

    response = requests.post(API_URL, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        print("\n✅ Success! Parcel Data Received:")
        print(json.dumps(data, indent=4))  # Pretty-print JSON response
        return data

    elif response.status_code == 401:
        print("\n🔄 Token might have expired. Re-authenticating...")
        authenticate()  # Get a new token
        return fetch_parcel_summaries()  # Retry the request with a new token

    elif response.status_code == 429:
        print("\n⚠️ Rate limit hit! Waiting 5 seconds before retrying...")
        time.sleep(5)
        return fetch_parcel_summaries()  # Retry with the same token

    else:
        print(f"\n❌ Request Failed! Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return None


if __name__ == "__main__":
    authenticate()  # Get initial token
    fetch_parcel_summaries()  # Fetch parcel data