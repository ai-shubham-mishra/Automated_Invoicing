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
# GOOGLE_FOLDER_ID removed
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL')

# fetch_drive_files function removed

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
        c.execute("SELECT name, price_sheet_link, customer_number FROM clients")
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
    # all_files = fetch_drive_files()  # Removed, not needed
    all_clients = get_all_clients()
    client_names = [client[0] for client in all_clients]
    selected_client = st.selectbox(
        "Select Client Name",
        options=["(Select client name)"] + client_names,
        index=0,
        help="Select a client to automatically use their price sheet"
    )
    selected_file = None
    price_sheet_id = None
    customer_number = None
    if selected_client and selected_client != "(Select client name)":
        client_data = next((client for client in all_clients if client[0] == selected_client), None)
        if client_data:
            spreadsheet_link = client_data[1]
            customer_number = client_data[2]
            file_id = extract_file_id_from_link(spreadsheet_link)
            price_sheet_id = file_id
            if file_id:
                spreadsheet_name = get_file_name_from_drive(file_id)
                if spreadsheet_name:
                    selected_file = spreadsheet_name
                    st.info(f"Price sheet for {selected_client}: {spreadsheet_name}")
                else:
                    st.error("Could not fetch spreadsheet name from Google Drive")
            else:
                st.error("Invalid Google Spreadsheet link format")
    # Remove the price sheet selectbox entirely
    # selected_file is only set if a client is selected and spreadsheet is found
    uploaded_files = st.file_uploader(
        "Upload Delivery Note(s)",
        type=["pdf", "png", "jpg", "jpeg", "gif", "bmp", "tiff", "webp"],
        accept_multiple_files=True,
        help="You can select multiple files."
    )
    if st.button("Submit", use_container_width=True, type="primary"):
        if not selected_client or selected_client == "(Select client name)":
            st.error("Please select a valid client.")
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
                        if customer_number:
                            data["customer_number"] = customer_number
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
    customer_number = st.text_input("Customer Number", key="customer_number")
    if st.button("Add Client", key="add_client_btn"):
        if not client_name or not price_sheet_link or not customer_number:
            st.error("All fields are required.")
        else:
            try:
                conn = sqlite3.connect("clients.db")
                c = conn.cursor()
                c.execute("""
                    CREATE TABLE IF NOT EXISTS clients (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        price_sheet_link TEXT NOT NULL,
                        customer_number TEXT NOT NULL
                    )
                """)
                c.execute("INSERT INTO clients (name, price_sheet_link, customer_number) VALUES (?, ?, ?)", (client_name, price_sheet_link, customer_number))
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