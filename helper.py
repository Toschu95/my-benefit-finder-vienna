import pandas as pd
import json
import google.auth
from googleapiclient.discovery import build

def load_param_from_config(param_name):
  """ Load a certain parameter from the config.json file."""
  with open('config.json') as f:
    data = json.load(f)
  return data[param_name]

def add_unique_link(metadata_links_list, metadata):
    """Add a unique link to the list if it does not already exist."""
    if not any(entry["url"] == metadata['url'] for entry in metadata_links_list):
        metadata_links_list.append(metadata)

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

                # get matadata
                if col_idx == 0:
                    metadata_link["kategorie intern"] = cell_text
                elif col_idx == 1:
                    metadata_link["thema intern"] = cell_text
                elif col_idx == 2:
                    metadata_link["name intern"] = cell_text
                elif col_idx == 3:
                    metadata_link["notiz intern"] = cell_text

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
    

