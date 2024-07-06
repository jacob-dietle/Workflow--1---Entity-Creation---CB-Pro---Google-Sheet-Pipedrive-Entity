import requests

def get_uploaded_sheet(pd, sheet_id):
    token = f'{pd.inputs["google_sheets"]["$auth"]["oauth_access_token"]}'
    authorization = f'Bearer {token}'
    headers = {"Authorization": authorization}

    # Get sheet metadata
    metadata_url = f'https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}?includeGridData=false&access_token={token}'
    response = requests.get(metadata_url, headers=headers)
    metadata = response.json()

    # Get the sheet data range
    data_range = metadata["sheets"][0]["properties"]["title"]
    data_range_url = f'https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{data_range}?access_token={token}'
    data_range_response = requests.get(data_range_url, headers=headers)
    data_range_json = data_range_response.json()

    row_count_with_data = len(data_range_json['values'])

    # Get the grid properties
    sheet_properties = metadata["sheets"][0]["properties"]
    col_count = sheet_properties["gridProperties"]["columnCount"] + 1  # Adding 1 for the "Imported" column

    return row_count_with_data, col_count, headers

def handler(pd: "pipedream"):
    # Get the data from the initial workflow trigger
    print(pd.steps["trigger"]["event"])

    # Call the get_uploaded_sheet function with the retrieved sheet_id and export the relevant data
    sheet_id = pd.steps["trigger"]["event"]["id"]  # Replace this with the actual sheet_id from the event data
    row_count_with_data, col_count, headers = get_uploaded_sheet(pd, sheet_id)

    # Export the data for use in future steps
    pd.export("row_count_with_data", row_count_with_data)
    pd.export("col_count", col_count)
    pd.export("headers", headers)
