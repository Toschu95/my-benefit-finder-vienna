import json
import google.auth
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import requests
import re
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain.schema import Document
import hashlib
from datetime import datetime
import os
import shutil

def load_param_from_config(*args):
    """ Load a certain parameter from the config.json file."""
    with open('config.json') as f:
        result = json.load(f)

    for param_name in args:
        result = result.get(param_name)
    return result

def add_unique_link(all_data, data):
    """Add a unique link to the list if it does not already exist."""
    if data['url'] not in [all_data['url'] for data in all_data]:
        all_data.append(data.copy())

def parse_metadata(col_idx, cell_text):
    """Parse metadata based on column index."""
    keys = {
        0: "kategorie intern",
        1: "thema intern",
        2: "name intern",
        3: "notiz intern"
    }
    return {keys[col_idx]: cell_text} if col_idx in keys else {}

def query_google_spreadsheet(spreadsheet_id, range, api, credentials_path):
    """Query a Google Sheets document."""
    # Set up Google API
    creds = google.auth.load_credentials_from_file(credentials_path, [api,])[0]
    service = build("sheets", "v4", credentials=creds)

    # Retrieve data from Google Sheets
    result = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=[range],
        fields="sheets.data.rowData.values"
    ).execute()
    
    return result.get("sheets", [])[0].get("data", [])[0].get("rowData", [])

def get_hyperlinks_from_google_spreadsheet(spreadsheet_id, range, api, credentials_path, base_metadata):
    """ Retrieve hyperlinks from a Google Sheets document."""
    # Query the Google Sheets document
    result = query_google_spreadsheet(spreadsheet_id, range, api, credentials_path)
    
    # List to store unique hyperlinks
    all_data = []

    # Iterate over all rows in the API response
    for i, row in enumerate(result):
        if "values" in row:
            # crate data from base metadata
            data = base_metadata.copy()
            # iterratore over all columns in the row
            for col_idx, value in enumerate(row["values"]):
                # get cell value and skip if empty
                cell_text = value.get("formattedValue", "").strip()  # Get full cell text
                if not cell_text:
                    continue

                # add more metadata
                metadata_from_sheets = parse_metadata(col_idx, cell_text)
                data.update(
                    {
                        "version" : get_current_date(),
                        **metadata_from_sheets
                    }
                )

                # preformat cell text for link processing
                text_parts = cell_text.split("\n") if cell_text else []  # Split labels by newlines

                # If only one link in cell
                if "hyperlink" in value:
                    # add more metadata
                    data["url name"] = text_parts[0].strip()
                    data["url"] = value["hyperlink"]
                    add_unique_link(all_data, data)

                # If multiple links in one cell
                if "textFormatRuns" in value:
                    for i, run in enumerate(value["textFormatRuns"]):
                        if "format" in run and "link" in run["format"]:
                            # add more metadata
                            data['url'] = run["format"]["link"]["uri"]
                            data['url name'] = text_parts[i] if i < len(text_parts) else f"Link_{i+1}".strip()
                            add_unique_link(all_data, data)
    return all_data

def load_internal_documentation_from_google_spreadsheet(spreadsheet_id, range, api, credentials_path, base_metadata):
    """Retrieve documentation from a Google Sheets document."""
    # Query the Google Sheets document
    result = query_google_spreadsheet(spreadsheet_id, range, api, credentials_path)
    
    # List to store unique hyperlinks
    data_list = []
    # Iterate over all rows in the API response
    for row in result:
        if "values" in row:
            # get cell documentation and skip if empty
            docu = row["values"][5].get("formattedValue", "").strip()  # Get full cell text
            if not docu:
                continue
            
            # create data from base metadata
            data = base_metadata.copy()
            
            # add more metadata
            data.update({
                "personen id": row["values"][0].get("formattedValue", "").strip(),
                "version" : get_current_date()

            })
            
            # add content
            data["inhalt"] = docu

            # append metadata to list
            data_list.append(data)

    return data_list

def save_data_as_json(data:list, data_name:str, data_path:str):
    """Save content as a JSON file."""
    with open(f"{data_path}{data_name}.json", 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"{data_name} saved.")

def scrape_links_from_list(all_data:list):
    """Scrape links from a list of dicts incl. URLs."""
    data = []
    success = 0
    failded = 0
    for data in all_data:
        link = data.get("url")
        if link.startswith("http"):
            # If the link is a valid URL, scrape it
            response = requests.get(link)
            if response.status_code != 200:
                print(f"Failed to retrieve {link}: {response.status_code}")
                failded += 1
                continue
            response.encoding = response.apparent_encoding
            html = response.text
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text()
            cleaned = re.sub(r"\s+", " ", text)
            data["inhalt"] = cleaned

            success += 1
            data.append(data)
    print(f"Successfully scraped {success} out of {len(all_data)} links.")

    return data

def hash_text(text: str):
    return hashlib.md5(text.strip().encode("utf-8")).hexdigest()

def get_current_date():
    """Get the current date."""
    return datetime.today().strftime("%Y-%m")

def text_splitter_with_metadata(data:list, chunk_size:int, overlap:float):
    """Split text into chunks with metadata."""
    
    # Config splitter
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=int(chunk_size * overlap),
    )

    # Split dicts in list
    documents = []

    for entry in data:
        if "inhalt" not in entry:
            continue

        chunks = text_splitter.split_text(entry["inhalt"])
        base_metadata = {k: v for k, v in entry.items() if k != "inhalt"}

        for chunk in chunks:
            chunk_hash = hash_text(chunk)
            metadata = base_metadata.copy()
            metadata["chunk-hash"] = chunk_hash

            doc = Document(
                page_content=chunk,
                metadata=metadata
            )
            documents.append(doc)

    return documents

def load_or_create_vectorstore(
    persist_directory: str,
    vectorstore_name: str,
    model_name: str
):
    """Load or create a vectorstore."""
    embedding = HuggingFaceEmbeddings(model_name=model_name)
    db_path = persist_directory + vectorstore_name
    db = Chroma(persist_directory=db_path, embedding_function=embedding)
    return db

def init_vectorstore(
    vectorstore_name="chroma_db",
    persist_directory="data/vectorstore/",
    model_name="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
):
    """(Re)Initialize a Chroma vectorstore by deleting any existing DB and creating an empty one."""
    
    db_path = os.path.join(persist_directory, vectorstore_name)

    # Delete existing DB if it exists
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
        print(f"🗑️ Existing ChromaDB at '{db_path}' deleted.")

    # Create a new empty DB
    db = load_or_create_vectorstore(persist_directory, vectorstore_name, model_name)
    db.persist()
    print("✅ Empty ChromaDB initialized.")

def load_public_information_from_google_spreadsheet(spreadsheet_id, range, api, credentials_path, base_metadata):
    """Load public information from a Google Sheets document."""
    link_data = get_hyperlinks_from_google_spreadsheet(spreadsheet_id, range, api, credentials_path, base_metadata)
    data = scrape_links_from_list(link_data)
    return data

def get_existing_hashes(db: Chroma):
    """Load all existing hashes from vector db."""
    all = db.get(include=["metadatas"])
    return set(meta.get("chunk-hash") for meta in all["metadatas"] if meta.get("chunk-hash"))

def delete_outdated_version(db: Chroma, version: str, base_metadata: dict):
    """Delete old chunks for a certain data source identified via base-metadata."""
    all = db.get(include=["metadatas", "ids"])
    
    ids_to_delete = [
        id_ for id_, meta in zip(all["ids"], all["metadatas"])
        if meta.get("version") != version and all(item in meta.items() for item in base_metadata.items())
    ]

    if ids_to_delete:
        db.delete(ids=ids_to_delete)
        print(f"🧹 {len(ids_to_delete)} old chunks of source '{base_metadata.values()}' removed.")
    else:
        print(f"ℹ️ No old chunks for source '{base_metadata.values()}' found.")

def add_documents_to_vectorstore(
        spreadsheet_id,
        range,
        api,
        credentials_path,
        base_metadata,
        load_function,
        raw_data_name,
        raw_data_path="data/raw/",
        update_raw_data="true",
        chunk_size=500,
        overlap=0.2,
        vectorstore_name="chroma_db",
        persist_directory="data/vectorstore/",
        model_name="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
        ):
    """Add documents to an existing vectorstore."""

    # Load or create data
    if update_raw_data.lower() == "true":
        data = load_function(spreadsheet_id, range, api, credentials_path, base_metadata)
        save_data_as_json(data, raw_data_name)
    else:
        with open(f"{raw_data_path}{raw_data_name}.json", "r") as f:
            data = json.load(f)

    # Create documents
    documents = text_splitter_with_metadata(data, chunk_size, overlap)

    # Load vectorestore
    db = load_or_create_vectorstore(persist_directory, vectorstore_name, model_name)

    # Load hashes
    existing_hashes = get_existing_hashes(db)

    # Check for new documents by hash
    unique_new_docs = [doc for doc in documents if doc.metadata["chunk-hash"] not in existing_hashes]

    # 6. Delete older versions
    version = documents[0].metadata.get("version", "unknown")
    delete_outdated_version(db, version, base_metadata)

    # Save new documents
    if unique_new_docs:
        db.add_documents(unique_new_docs)
        db.persist()
        print(f"✅ {len(unique_new_docs)} new chunks added (version {version}).")
    else:
        print("ℹ️ No new chunks. DB is up to date.")

# Wrapper schreiben mit den argumenten: spreadsheet_id, range, api, credentials_path, base_metadata, load_function, raw_data_name,
# Main anpassen
# Modularisieren: eigene module für unterschiedliche Datenbanken + helper für Allgemeines