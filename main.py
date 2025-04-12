import helper

def main():
    config = {
        "id": helper.load_param_from_config("google_spreadsheet_id"),
        "range": helper.load_param_from_config("google_spreadsheet_range"),
        "api": helper.load_param_from_config("google_sheets_api"),
    }

    result = helper.get_hyperlinks_from_google_spreadsheet(
        config["id"],
        config["range"],
        config["api"],
        'credentials_google_service_acc.json'
    )
    print(result)

if __name__ == "__main__":
    main()