from fastapi import FastAPI, Request
import json

app = FastAPI()

@app.post("/webhook")
async def receive_webhook(request: Request):
    """Handle incoming webhook events from Capstone API."""
    data = await request.json()
    print("📩 Webhook Received:", json.dumps(data, indent=4))  # Log data for debugging

    # Process the incoming data as needed (e.g., store it in a database, trigger actions)
    
    return {"status": "success", "message": "Webhook received"}