import os
import requests
import datetime

RAILWAY_API_KEY = os.getenv("RAILWAY_API_KEY")
PROJECT_ID = os.getenv("RAILWAY_PROJECT_ID")
SERVICE_ID = os.getenv("RAILWAY_SERVICE_ID")

HEADERS = {"Authorization": f"Bearer {RAILWAY_API_KEY}"}

def start_service():
    url = f"https://backboard.railway.app/v1/projects/{PROJECT_ID}/services/{SERVICE_ID}/start"
    response = requests.post(url, headers=HEADERS)
    print("✅ Service Started" if response.status_code == 200 else f"❌ Start Failed: {response.text}")

def stop_service():
    url = f"https://backboard.railway.app/v1/projects/{PROJECT_ID}/services/{SERVICE_ID}/stop"
    response = requests.post(url, headers=HEADERS)
    print("✅ Service Stopped" if response.status_code == 200 else f"❌ Stop Failed: {response.text}")

# Define when to start/stop
now = datetime.datetime.now()
if now.weekday() in [0, 1, 2, 6]:  # Sunday to Wednesday
    start_service()
else:
    stop_service()