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

    # Save the documentation as JSON
    save_internal_documentation_as_json(data_list)

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

def combine_all_json_in_path_to_one(path="data/raw/web/", output_filename="webcontent.json"):
    """Combine all JSON files in a directory into one, then delete the originals including old output."""

    output_path = os.path.join(path, output_filename)

    # Delete old combined file if it exists
    if os.path.exists(output_path):
        os.remove(output_path)
        print(f"Deleted existing file: {output_filename}")

    combined_data = []

    for filename in os.listdir(path):
        file_path = os.path.join(path, filename)

        # Skip the output file (e.g., if generated between runs)
        if filename.endswith(".json") and filename != output_filename:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                combined_data.append(data)

            # Remove the original file
            os.remove(file_path)

    # Save the combined output
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(combined_data, f, ensure_ascii=False, indent=4)

    print(f"Combined and removed {len(combined_data)} JSON files into {output_filename}.")

def save_internal_documentation_as_json(documentation_list, path="data/raw/internal_documentation/"):
    """Save documentation as a JSON file."""
    filename = path + "internal_documentation.json"
    with open(filename, 'w') as f:
        json.dump(documentation_list, f, ensure_ascii=False, indent=4)
    print(f"Documentation saved to {filename}.")