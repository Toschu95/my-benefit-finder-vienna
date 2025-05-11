import json
import google.auth
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import requests
import re
import os

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
                    metadata_link["url name"] = text_parts[0].strip()
                    metadata_link["url"] = value["hyperlink"]
                    add_unique_link(metadata_links_list, metadata_link)

                # If multiple links in one cell
                if "textFormatRuns" in value:
                    for i, run in enumerate(value["textFormatRuns"]):
                        if "format" in run and "link" in run["format"]:
                            metadata_link['url'] = run["format"]["link"]["uri"]

                            # Ensuring existence of label
                            metadata_link['url name'] = text_parts[i] if i < len(text_parts) else f"Link_{i+1}".strip()

                            add_unique_link(metadata_links_list, metadata_link)
    return metadata_links_list

def get_filename_for_webcontent(link_dict):
    """Generate a filename for web content based on the link dictionary."""
    prefix = link_dict.get("url").split("//")[-1].split(".at")[0].replace("www.", "").replace(".", "_")
    # remove all special characters from the prefix
    prefix = re.sub(r"\s+", " ", prefix)
    prefix = re.sub(r"[^a-zA-Z0-9]", "_", prefix)
    
    suffic = link_dict.get("url name")
    # remove all special characters from the suffix
    suffic = re.sub(r"\s+", " ", suffic)
    suffic = re.sub(r"[^a-zA-Z0-9]", "_", suffic)
    
    name = f"{prefix}_{suffic}"[:50]
    return f"{name}.json"

def save_webcontent_as_json(link_dict, path="data/raw/web/"):
    """Save web content as a JSON file."""
    filename = path + get_filename_for_webcontent(link_dict)
    # if filename already exists, append a number to the filename
    if os.path.exists(filename):
        base, ext = os.path.splitext(filename)
        i = 1
        while os.path.exists(filename):
            filename = f"{base}_{i}{ext}"
            i += 1
    with open(filename, 'w') as f:
        json.dump(link_dict, f, ensure_ascii=False, indent=4)
    print(f"Web content saved to {filename}.")

def scrape_links_from_list(links_with_metadata:list):
    """Scrape links from a list of dicts incl. URLs."""
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
            # Save the web content as JSON
            save_webcontent_as_json(link_dict)
            success += 1
    print(f"Successfully scraped {success} out of {len(links_with_metadata)} links.")

def combine_all_json_in_path_to_one(path="data/raw/web/"):
    """Combine all JSON files in a directory into one."""
    combined_data = []
    for filename in os.listdir(path):
        if filename.endswith(".json"):
            with open(os.path.join(path, filename), 'r') as f:
                data = json.load(f)
                combined_data.append(data)
    with open(os.path.join(path, 'combined.json'), 'w') as f:
        json.dump(combined_data, f, ensure_ascii=False, indent=4)
    print(f"Combined {len(combined_data)} JSON files into one.")