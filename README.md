# Automated Invoicing UI

## Abstract
This project is a Streamlit-based web application that allows users to upload PDF or image files and select files from a Google Drive folder. The uploaded files, along with user information, are sent to an n8n webhook for further processing. The app is designed to streamline the process of submitting client files for automated invoicing workflows.

## Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/ai-shubham-mishra/Automated_Invoicing.git
cd Automated_Invoicing/Invoicing_UI
```

### 2. Create a `.env` File
Create a `.env` file in the root of the `Invoicing_UI` directory with the following variables:

```
GOOGLE_API_KEY=your_google_api_key_here
GOOGLE_FOLDER_ID=your_google_drive_folder_id_here
N8N_WEBHOOK_URL=your_n8n_webhook_url_here
```

- **GOOGLE_API_KEY**: Your Google API key with access to Google Drive API.
- **GOOGLE_FOLDER_ID**: The ID of the Google Drive folder containing files to display in the app.
- **N8N_WEBHOOK_URL**: The URL of your n8n webhook endpoint to receive uploaded files and user data.

### 3. Install Dependencies
It is recommended to use a virtual environment (e.g., `venv` or `conda`). Then install dependencies:

```bash
pip install -r requirements.txt
```

### 4. Run the Application
Start the Streamlit app with:

```bash
streamlit run app.py
```

The app will open in your default web browser. Follow the on-screen instructions to upload files and submit data.

## Requirements
- Python 3.8+
- Google API key with Drive API enabled
- n8n workflow with a webhook endpoint