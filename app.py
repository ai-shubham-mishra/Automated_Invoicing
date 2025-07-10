import streamlit as st
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set page configuration
st.set_page_config(page_title="Client File Uploader", page_icon="📤", layout="centered")

# Environment variables with fallback defaults
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_FOLDER_ID = os.getenv('GOOGLE_FOLDER_ID')
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL')

# Function to fetch files from Google Drive
def fetch_drive_files():
    if not GOOGLE_API_KEY or not GOOGLE_FOLDER_ID:
        st.error("Google API key or Folder ID is missing. Please check your environment variables.")
        return []
    url = f"https://www.googleapis.com/drive/v3/files?q='{GOOGLE_FOLDER_ID}'+in+parents+and+trashed=false&fields=files(id,name)&key={GOOGLE_API_KEY}"
    try:
        res = requests.get(url)
        st.write("Debug: Drive API response status:", res.status_code)
        st.write("Debug: Drive API response:", res.json())
        if res.status_code != 200:
            st.error(f"Google Drive API error: {res.json().get('error', {}).get('message', 'Unknown error')}")
            return []
        files = res.json().get('files', [])
        if not files:
            st.warning("No files found in the specified Google Drive folder.")
            return []
        return [f['name'] for f in files]
    except Exception as e:
        st.error(f"Failed to fetch files from Google Drive: {e}")
        return []

# Fetch files
all_files = fetch_drive_files()

# New field: Enter your name
user_name = st.text_input("Enter your name")
# New field: Enter your email
user_email = st.text_input("Enter your email")

# Search-as-you-type dropdown for files
selected_file = st.selectbox(
    "Select File from Drive Folder",
    options=all_files if all_files else ["No files available"],
    index=0 if all_files else 0,
    placeholder="Type to search...",
    disabled=not all_files
)

# File uploader
uploaded_files = st.file_uploader(
    "Upload multiple PDF or image files",
    type=["pdf", "png", "jpg", "jpeg", "gif", "bmp", "tiff", "webp"],
    accept_multiple_files=True,
    help="You can select multiple files."
)

# Submit button
if st.button("Submit", use_container_width=True, type="primary"):
    st.write("Debug: Selected file:", selected_file)
    st.write("Debug: Uploaded files:", [f.name for f in uploaded_files] if uploaded_files else "None")
    if not selected_file or selected_file == "No files available":
        st.error("Please select a valid file from your Drive folder.")
    elif not uploaded_files:
        st.error("Please upload at least one file.")
    else:
        with st.spinner("Uploading..."):
            # Prepare payload for n8n webhook
            files_payload = [("files", (f.name, f.getvalue(), f.type)) for f in uploaded_files]  # Use 'files' key
            data = {"drive_file": selected_file, "user_name": user_name, "user_email": user_email}
            try:
                # Debug: Verify file content before sending
                for f in uploaded_files:
                    content = f.getvalue()
                    st.write(f"Debug: File {f.name} content length: {len(content)}")
                resp = requests.post(N8N_WEBHOOK_URL, data=data, files=files_payload)
                st.write("Debug: Webhook response status:", resp.status_code)
                st.write("Debug: Webhook response:", resp.text)
                if resp.ok:
                    st.success("Upload successful!")
                else:
                    st.error(f"Upload failed: {resp.status_code} {resp.text}")
            except Exception as e:
                st.error(f"Upload failed: {e}")