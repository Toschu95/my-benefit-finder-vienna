import helper

def main():
    config = {
        "id": helper.load_param_from_config("google_spreadsheet_id"),
        "range": helper.load_param_from_config("google_spreadsheet_range"),
        "api": helper.load_param_from_config("google_sheets_api"),
    }

    metadata_links_list = helper.get_hyperlinks_from_google_spreadsheet(
        config["id"],
        config["range"],
        config["api"],
        'credentials_google_service_acc.json'
    )
    
    helper.scrape_links_from_list([link['url'] for link in metadata_links_list])

if __name__ == "__main__":
    main()