import json
import google.auth
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import requests
import re

def load_param_from_config(param_name):
  """ Load a certain parameter from the config.json file."""
  with open('config.json') as f:
    data = json.load(f)
  return data[param_name]

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

def get_hyperlinks_from_google_spreadsheet(spreadsheet_id, range, api, credentials_path):
    """ Retrieve hyperlinks from a Google Sheets document."""
    # Set up Google API
    creds = google.auth.load_credentials_from_file(credentials_path, [api,])[0]
    service = build("sheets", "v4", credentials=creds)

    # Retrieve data from Google Sheets
    result = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=[range],
        fields="sheets.data.rowData.values"
    ).execute()
    
    # List to store unique hyperlinks
    metadata_links_list = []

    # Iterate over all rows in the API response
    for i, row in enumerate(result.get("sheets", [])[0].get("data", [])[0].get("rowData", [])):
        if "values" in row:
            metadata_link = {"datenquelle" : "öffentlich verfügbare website"}
            # iterratore over all columns in the row
            for col_idx, value in enumerate(row["values"]):
                # get cell value
                cell_text = value.get("formattedValue", "").strip()  # Get full cell text

                metadata = parse_metadata(col_idx, cell_text)
                metadata_link.update(metadata)

                # preformat cell text for link processing
                text_parts = cell_text.split("\n") if cell_text else []  # Split labels by newlines

                # If only one link in cell
                if "hyperlink" in value:
                    metadata_link["url name"] = text_parts[0]
                    metadata_link["url"] = value["hyperlink"]
                    add_unique_link(metadata_links_list, metadata_link)

                # If multiple links in one cell
                if "textFormatRuns" in value:
                    for i, run in enumerate(value["textFormatRuns"]):
                        if "format" in run and "link" in run["format"]:
                            metadata_link['url'] = run["format"]["link"]["uri"]

                            # Ensuring existence of label
                            metadata_link['url name'] = text_parts[i] if i < len(text_parts) else f"Link_{i+1}"

                            add_unique_link(metadata_links_list, metadata_link)
    return metadata_links_list

def scrape_links_from_list(links_list):
    """Scrape links from a list of URLs."""
    success = 0
    failded = 0
    for link in links_list:
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
            success += 1
    print(f"Successfully scraped {success} out of {len(links_list)} links.")