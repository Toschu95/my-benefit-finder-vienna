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


def load_param_from_config(*args):
    """ Load a certain parameter from the config.json file."""
    with open('config.json') as f:
        result = json.load(f)

    for param_name in args:
        result = result.get(param_name)
    return result

def add_unique_link(metadata_links_list, metadata):
    """Add a unique link to the list if it does not already exist."""
    if metadata['url'] not in [metadata['url'] for metadata in metadata_links_list]:
        metadata_links_list.append(metadata.copy())

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

def get_hyperlinks_from_google_spreadsheet(spreadsheet_id, range, api, credentials_path):
    """ Retrieve hyperlinks from a Google Sheets document."""
    # Query the Google Sheets document
    result = query_google_spreadsheet(spreadsheet_id, range, api, credentials_path)
    
    # List to store unique hyperlinks
    data_list = []

    # Iterate over all rows in the API response
    for i, row in enumerate(result):
        if "values" in row:
            # crate data and add first metadata
            data = {"datenquelle" : "öffentlich verfügbare website"}
            # iterratore over all columns in the row
            for col_idx, value in enumerate(row["values"]):
                # get cell value and skip if empty
                cell_text = value.get("formattedValue", "").strip()  # Get full cell text
                if not cell_text:
                    continue

                # add more metadata
                metadata = parse_metadata(col_idx, cell_text)
                data.update(metadata)

                # preformat cell text for link processing
                text_parts = cell_text.split("\n") if cell_text else []  # Split labels by newlines

                # If only one link in cell
                if "hyperlink" in value:
                    data["url name"] = text_parts[0].strip()
                    data["url"] = value["hyperlink"]
                    add_unique_link(data_list, data)

                # If multiple links in one cell
                if "textFormatRuns" in value:
                    for i, run in enumerate(value["textFormatRuns"]):
                        if "format" in run and "link" in run["format"]:
                            data['url'] = run["format"]["link"]["uri"]

                            # Ensuring existence of label
                            data['url name'] = text_parts[i] if i < len(text_parts) else f"Link_{i+1}".strip()

                            add_unique_link(data_list, data)
    return data_list

def load_internal_documentation_from_google_spreadsheet(spreadsheet_id, range, api, credentials_path):
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
            
            # create data
            data = {"inhalt" : docu}
            
            # add metadata
            data.update({
                "personen id": row["values"][0].get("formattedValue", "").strip(),
                "datenquelle" : "interne dokumentation sozialberatung"
            })

            # append metadata to list
            data_list.append(data)

    return data_list

def save_webcontent_as_json(data:list, path="data/raw/web/webcontent.json"):
    """Save web content as a JSON file."""
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Public infomration saved to {path}.")

def scrape_links_from_list(links_with_metadata:list):
    """Scrape links from a list of dicts incl. URLs."""
    data = []
    success = 0
    failded = 0
    for link_dict in links_with_metadata:
        link = link_dict.get("url")
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
            link_dict["inhalt"] = cleaned

            success += 1
            data.append(link_dict)
    print(f"Successfully scraped {success} out of {len(links_with_metadata)} links.")

    return data

def save_internal_documentation_as_json(data:list, path="data/raw/internal_documentation/internal_documentation.json"):
    """Save documentation as a JSON file."""
    with open(path, 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Internal documentation saved to {path}.")

def text_splitter_with_metadata(data:list, chunk_size=500, overlap=0.2):
    """Split text into chunks with metadata."""
    
    # Config splitter
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_size * overlap,
    )

    # Split dicts in list
    documents = []

    for entry in data:
        if "inhalt" not in entry:
            continue 

        chunks = text_splitter.split_text(entry["inhalt"])
        
        # Use all other keys as metadata
        metadata = {k: v for k, v in entry.items() if k != "inhalt"}
        
        for chunk in chunks:
            doc = Document(
                page_content=chunk,
                metadata=metadata.copy()
            )
            documents.append(doc)

    return documents

def create_vectorstore_from_documents(documents, database_name, persist_directory="data/vectorstore/"):
    """Create a vectorstore from documents."""
    # Create embeddings
    embedding = HuggingFaceEmbeddings(
        model_name="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
        )

    # Create vectorstore
    chroma_db = Chroma.from_documents(
        documents=documents,
        embedding=embedding,
        persist_directory=persist_directory + database_name,
    )

    chroma_db.persist()
    print(f"✅ Vectorstore {database_name} created and persisted.")

def init_database_public_information(spreadsheet_id, range, api, credentials_path, update="true"):
    """Initialize the public database."""
    if update.lower() == "true":
        print('for real')
        links_with_metadata = get_hyperlinks_from_google_spreadsheet(spreadsheet_id, range, api, credentials_path)

        data = scrape_links_from_list(links_with_metadata)

        save_webcontent_as_json(data)
    else:
        with open("data/raw/web/webcontent.json", "r") as f:
            data = json.load(f)

    documents = text_splitter_with_metadata(data)

    create_vectorstore_from_documents(documents, "public_information")

def init_database_internal_documentation(spreadsheet_id, range, api, credentials_path, update="true"):
    """Initialize the internal documentation database."""
    if update.lower() == "true":
        data = load_internal_documentation_from_google_spreadsheet(spreadsheet_id, range, api, credentials_path)
        
        save_internal_documentation_as_json(data)
    else:
        with open("data/raw/internal_documentation/internal_documentation.json", "r") as f:
            data = json.load(f)

    documents = text_splitter_with_metadata(data)

    create_vectorstore_from_documents(documents, "internal_documentation")

# Nur EINE Vectordatenbank erstellen aus allen verfügbaren Daten
# Hash und Doppelcheck einfügen