import streamlit as st
import requests
import os
from dotenv import load_dotenv
import sqlite3
import re
from PIL import Image
import io

# Load environment variables
load_dotenv()

st.set_page_config(page_title="Client File Uploader", page_icon="📤", layout="centered")

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_FOLDER_ID = os.getenv('GOOGLE_FOLDER_ID')
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL')

# Fetch files from Google Drive
def fetch_drive_files():
    if not GOOGLE_API_KEY or not GOOGLE_FOLDER_ID:
        st.error("Google API key or Folder ID is missing. Please check your environment variables.")
        return []
    url = f"https://www.googleapis.com/drive/v3/files?q='{GOOGLE_FOLDER_ID}'+in+parents+and+trashed=false&fields=files(id,name)&key={GOOGLE_API_KEY}"
    try:
        res = requests.get(url)
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

# Extract file ID from Google Spreadsheet link
def extract_file_id_from_link(link):
    patterns = [
        r'/spreadsheets/d/([a-zA-Z0-9-_]+)',
        r'/d/([a-zA-Z0-9-_]+)',
        r'id=([a-zA-Z0-9-_]+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, link)
        if match:
            return match.group(1)
    return None

# Get file name from Google Drive using file ID
def get_file_name_from_drive(file_id):
    if not GOOGLE_API_KEY:
        st.error("Google API key is missing.")
        return None
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?fields=name&key={GOOGLE_API_KEY}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            file_data = response.json()
            return file_data.get('name')
        else:
            st.error(f"Failed to fetch file metadata: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Error fetching file metadata: {e}")
        return None

# Get all clients from database
def get_all_clients():
    try:
        conn = sqlite3.connect("clients.db")
        c = conn.cursor()
        c.execute("SELECT name, price_sheet_link FROM clients")
        clients = c.fetchall()
        conn.close()
        return clients
    except Exception as e:
        st.error(f"Error fetching clients: {e}")
        return []

# Compress image files
def compress_image(image_file, max_size_mb=5):
    try:
        img = Image.open(image_file)
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        original_size = len(image_file.getvalue())
        max_size_bytes = max_size_mb * 1024 * 1024
        if original_size <= max_size_bytes:
            return image_file.getvalue()
        output = io.BytesIO()
        quality = 85
        while quality > 10:
            output.seek(0)
            output.truncate()
            img.save(output, format='JPEG', quality=quality, optimize=True)
            if len(output.getvalue()) <= max_size_bytes:
                break
            quality -= 10
        return output.getvalue()
    except Exception as e:
        st.error(f"Error compressing image: {e}")
        return image_file.getvalue()

# Validate and prepare files for upload
def prepare_files_for_upload(uploaded_files, max_size_mb=10):
    prepared_files = []
    total_size = 0
    for file in uploaded_files:
        file_size = len(file.getvalue())
        total_size += file_size
        if file_size > max_size_mb * 1024 * 1024:
            if file.type.startswith('image/'):
                compressed_data = compress_image(file, max_size_mb)
                prepared_files.append((file.name, compressed_data, file.type))
                st.warning(f"Compressed {file.name} to reduce size")
            else:
                st.error(f"File {file.name} is too large ({file_size / (1024*1024):.1f}MB). Please use a smaller file.")
                return None
        else:
            prepared_files.append((file.name, file.getvalue(), file.type))
    if total_size > max_size_mb * 1024 * 1024:
        st.error(f"Total file size ({total_size / (1024*1024):.1f}MB) exceeds limit. Please upload fewer or smaller files.")
        return None
    return prepared_files

TABS = ["Generate Invoice", "Add Client", "Remove Client"]
tab1, tab2, tab3 = st.tabs(TABS)

with tab1:
    all_files = fetch_drive_files()
    all_clients = get_all_clients()
    client_names = [client[0] for client in all_clients]
    selected_client = st.selectbox(
        "Select Client (Optional)",
        options=[""] + client_names,
        index=0,
        help="Select a client to automatically use their price sheet"
    )
    selected_file = None
    price_sheet_id = None
    if selected_client:
        client_data = next((client for client in all_clients if client[0] == selected_client), None)
        if client_data:
            spreadsheet_link = client_data[1]
            file_id = extract_file_id_from_link(spreadsheet_link)
            price_sheet_id = file_id
            if file_id:
                spreadsheet_name = get_file_name_from_drive(file_id)
                if spreadsheet_name:
                    selected_file = spreadsheet_name
                    st.success(f"Using price sheet: {spreadsheet_name}")
                else:
                    st.error("Could not fetch spreadsheet name from Google Drive")
            else:
                st.error("Invalid Google Spreadsheet link format")
    if not selected_file:
        selected_file = st.selectbox(
            "Select File from Drive Folder",
            options=all_files if all_files else ["No files available"],
            index=0 if all_files else 0,
            placeholder="Type to search...",
            disabled=not all_files
        )
    uploaded_files = st.file_uploader(
        "Upload multiple PDF or image files",
        type=["pdf", "png", "jpg", "jpeg", "gif", "bmp", "tiff", "webp"],
        accept_multiple_files=True,
        help="You can select multiple files."
    )
    if st.button("Submit", use_container_width=True, type="primary"):
        if not selected_file or selected_file == "No files available":
            st.error("Please select a valid file from your Drive folder.")
        elif not uploaded_files:
            st.error("Please upload at least one file.")
        else:
            with st.spinner("Preparing files for upload..."):
                prepared_files = prepare_files_for_upload(uploaded_files)
                if prepared_files is None:
                    st.error("Please fix the file size issues and try again.")
                else:
                    with st.spinner("Uploading..."):
                        files_payload = [("files", (name, data, file_type)) for name, data, file_type in prepared_files]
                        data = {"drive_file": selected_file}
                        if selected_client:
                            data["name"] = selected_client
                        if price_sheet_id:
                            data["price_sheet_id"] = price_sheet_id
                        print("Payload being sent:", data)
                        try:
                            resp = requests.post(N8N_WEBHOOK_URL, data=data, files=files_payload)
                            if resp.ok:
                                st.success("Upload successful!")
                            else:
                                st.error(f"Upload failed: {resp.status_code} {resp.text}")
                        except Exception as e:
                            st.error(f"Upload failed: {e}")

with tab2:
    st.header("Add Client")
    client_name = st.text_input("Client Name", key="client_name")
    price_sheet_link = st.text_input("Google Spreadsheet Link", key="price_sheet_link")
    if st.button("Add Client", key="add_client_btn"):
        if not client_name or not price_sheet_link:
            st.error("Both fields are required.")
        else:
            try:
                conn = sqlite3.connect("clients.db")
                c = conn.cursor()
                c.execute("""
                    CREATE TABLE IF NOT EXISTS clients (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        price_sheet_link TEXT NOT NULL
                    )
                """)
                c.execute("INSERT INTO clients (name, price_sheet_link) VALUES (?, ?)", (client_name, price_sheet_link))
                conn.commit()
                conn.close()
                st.success("Client added successfully!")
            except Exception as e:
                st.error(f"Failed to add client: {e}")

with tab3:
    st.header("Remove Client")
    all_clients = get_all_clients()
    client_names = [client[0] for client in all_clients]
    selected_client = st.selectbox("Select Client to Delete", options=[""] + client_names, index=0)
    if selected_client:
        confirm = st.text_input("Type CONFIRM to delete this client and their price sheet record:", key="delete_confirm")
        if st.button("Delete Client", key="delete_client_btn"):
            if confirm == "CONFIRM":
                try:
                    conn = sqlite3.connect("clients.db")
                    c = conn.cursor()
                    c.execute("DELETE FROM clients WHERE name = ?", (selected_client,))
                    conn.commit()
                    conn.close()
                    st.success(f"Client '{selected_client}' and their price sheet record deleted.")
                except Exception as e:
                    st.error(f"Failed to delete client: {e}")
            else:
                st.error("You must type CONFIRM to delete.")