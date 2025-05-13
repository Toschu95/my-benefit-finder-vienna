import helper

def main():
    helper.init_database_public_information(
        helper.load_param_from_config("google_spreadsheets", "links", "spreadsheet_id"),
        helper.load_param_from_config("google_spreadsheets", "links", "range"),
        helper.load_param_from_config("google_sheets_api"),
        'credentials_google_service_acc.json',
        helper.load_param_from_config("google_spreadsheets", "links", "update")
    )
    
    helper.init_database_internal_documentation(
        helper.load_param_from_config("google_spreadsheets", "doku", "spreadsheet_id"),
        helper.load_param_from_config("google_spreadsheets", "doku", "range"),
        helper.load_param_from_config("google_sheets_api"),
        'credentials_google_service_acc.json',
        helper.load_param_from_config("google_spreadsheets", "doku", "update")
    )

if __name__ == "__main__":
    main()