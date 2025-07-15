# Client Invoicing UI

This is a Streamlit-based web application for managing client price sheets, uploading delivery notes, and generating invoices. The app is fully database-driven and does not require Google Drive folder access.

## Features
- **Generate Invoice:** Select a client, view their price sheet, upload delivery notes, and submit for invoice generation.
- **Add Client:** Add new clients with their price sheet (Google Spreadsheet link) and customer number.
- **Remove Client:** Remove clients and their associated price sheet records from the database.

## Setup Instructions

### 1. Clone the Repository
```bash
https://github.com/yourusername/Invoicing_UI.git
cd Invoicing_UI
```

### 2. Install Dependencies
It is recommended to use a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Environment Variables
Create a `.env` file in the project root with the following variables:

```
GOOGLE_API_KEY=your_google_api_key  # Only needed for fetching spreadsheet names
N8N_WEBHOOK_URL=your_n8n_webhook_url
```

- `GOOGLE_API_KEY`: Used to fetch the spreadsheet name from a Google Spreadsheet link. Required for correct display of price sheet names.
- `N8N_WEBHOOK_URL`: The endpoint where invoice data and uploaded files are sent for processing.

### 4. Database Setup
The app uses a local SQLite database (`clients.db`). The database and required tables are created automatically when you add your first client.

## Usage
Run the app with:
```bash
streamlit run app.py
```

### Tabs Overview

#### 1. Generate Invoice
- **Select Client Name:** Choose a client from the dropdown.
- **Price Sheet Display:** After selecting a client, their price sheet name (from the Google Spreadsheet link) is shown.
- **Upload Delivery Notes:** Upload one or more delivery note files (PDF, images).
- **Submit:** Sends the data to the configured webhook for invoice generation.

#### 2. Add Client
- Enter the client name, Google Spreadsheet link (for their price sheet), and customer number.
- Click "Add Client" to save the client to the database.

#### 3. Remove Client
- Select a client to delete.
- Type `CONFIRM` to confirm deletion.
- Click "Delete Client" to remove the client and their price sheet record.

## Notes
- The app is now fully database-driven for price sheets. No Google Drive folder access or folder ID is required.
- Make sure your `.env` file is set up correctly before running the app.
- Uploaded files and invoice data are sent to the webhook specified by `N8N_WEBHOOK_URL`.

## Troubleshooting
- If you see errors about missing environment variables, check your `.env` file.
- If you have issues with Google Spreadsheet links, ensure your `GOOGLE_API_KEY` is valid and the spreadsheet is accessible.
- For database errors, ensure you have write permissions in the project directory.

---

For further questions or issues, please open an issue in this repository.