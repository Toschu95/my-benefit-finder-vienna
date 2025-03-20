import pandas as pd
import json
import google.auth
from googleapiclient.discovery import build

def load_param_from_config(param_name):
  with open('config.json') as f:
    data = json.load(f)
  return data[param_name]

def add_unique_link(links_list, label, url):
    """Add a unique link to the list if it does not already exist."""
    if not any(entry["url"] == url for entry in links_list):
        links_list.append({"url-name": label, "url": url})

def get_hyperlinks_from_google_spreadsheet(spreadsheet_id, range, api, credentials_path):
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
    links_list = []

    # Iterate over all rows in the API response
    for row in result.get("sheets", [])[0].get("data", [])[0].get("rowData", []):
        if "values" in row:
            for value in row["values"]:
                cell_text = value.get("formattedValue", "").strip()  # Get full cell text
                text_parts = cell_text.split("\n") if cell_text else []  # Split labels by newlines

                # If only one link in cell
                if "hyperlink" in value:
                    add_unique_link(links_list, text_parts[0], value["hyperlink"])

                # If multiple links in one cell
                if "textFormatRuns" in value:
                    for i, run in enumerate(value["textFormatRuns"]):
                        if "format" in run and "link" in run["format"]:
                            url = run["format"]["link"]["uri"]

                            # Ensuring existence of label
                            label = text_parts[i] if i < len(text_parts) else f"Link_{i+1}"

                            add_unique_link(links_list, label, url)

    return links_list   
    

