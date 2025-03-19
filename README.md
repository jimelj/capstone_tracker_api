# Capstone Tracker API

A FastAPI-based RESTful API for tracking and managing parcel deliveries, designed for CBA Distribution.

## 🚀 Features

- Real-time parcel tracking and status updates
- Address information retrieval and storage
- Weekly parcel summary reports
- Scheduled automatic data updates
- Rate limiting for API security
- Comprehensive logging system
- Database backup and restore functionality
- Secure API key authentication for sensitive operations

## 🛠️ Tech Stack

- **Backend**: FastAPI, Python 3
- **Database**: SQLite (local), PostgreSQL (Aiven Cloud)
- **Deployment**: Railway
- **Scheduling**: APScheduler
- **Authentication**: Token-based authentication

## 📋 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Home page and API health check |
| `/time` | GET | Get current server time |
| `/parcelsweek` | GET | Get parcels for the current week |
| `/parcels` | GET | Query parcels with filtering and sorting |
| `/parcels/{barcode}` | GET | Get parcel details by barcode |
| `/logs/server` | GET | View server logs (requires API key) |
| `/logs/database` | GET | View database logs (requires API key) |
| `/download-db` | GET | Download database backup (requires API key) |
| `/upload-db` | POST | Upload database for restore (requires API key) |
| `/reload` | POST | Trigger manual data update |

## 🔧 Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/capstone_tracker_api.git
   cd capstone_tracker_api
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   Create a `.env` file with the following variables:
   ```
   AUTH_URL="https://example.com/auth"
   API_URL="https://example.com/api"
   USERNAME="username"
   PASSWORD="password"
   LOGS_API_KEY="your-api-key"
   ```

5. **Initialize the database**
   ```bash
   python reset_db.py
   ```

6. **Start the server**
   ```bash
   uvicorn main:app --reload
   ```

## 🔄 Scheduled Tasks

The application includes scheduled tasks for:
- Updating parcel information
- Fetching delivery addresses
- Database maintenance

## 🔒 Security

- API key authentication for sensitive operations
- Rate limiting to prevent abuse
- Secure credential management through environment variables

## 🧪 Testing

Test the logging functionality with:
```bash
python test_logging.py
```

## 📄 License

This project is licensed under the terms included in the LICENSE file.

## 👥 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request 