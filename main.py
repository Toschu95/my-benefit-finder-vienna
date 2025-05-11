import helper

def main():
    config = {
        "id": helper.load_param_from_config("google_spreadsheet_id_doku"),
        "range": helper.load_param_from_config("google_spreadsheet_range_doku"),
        "api": helper.load_param_from_config("google_sheets_api"),
    }

    """
    links_list = helper.get_hyperlinks_from_google_spreadsheet(
        config["id"],
        config["range"],
        config["api"],
        'credentials_google_service_acc.json'
    )
    
    helper.scrape_links_from_list(links_list)"""

    helper.combine_all_json_in_path_to_one()

    """ helper.load_internal_documentation_from_google_spreadsheet(
        config["id"],
        config["range"],
        config["api"],
        'credentials_google_service_acc.json'
    ) """

if __name__ == "__main__":
    main()