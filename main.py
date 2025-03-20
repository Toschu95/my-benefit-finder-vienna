import helper

result = helper.get_hyperlinks_from_google_spreadsheet(
    helper.load_param_from_config("google_spreadsheet_id"),
    helper.load_param_from_config("google_spreadsheet_range"),
    helper.load_param_from_config("google_sheets_api"),
    'credentials_google_service_acc.json'
    )
print(result)