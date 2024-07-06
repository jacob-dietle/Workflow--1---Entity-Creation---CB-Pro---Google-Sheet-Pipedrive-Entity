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

def get_sheet_row(i, headers, sheet_id, token):
    range = f'A{i+1}:Z{i+1}' # Assuming that the spreadsheet has columns A through Z.
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{range}?access_token={token}'
    response = requests.get(url, headers=headers)
    json_data = response.json()
    row_data = json_data['values'][0] if 'values' in json_data else [None] * 26
    return row_data

def handler(pd: "pipedream"):
    # Get the export data from the previous code block
    row_count_with_data = pd.steps['get_uploaded_sheet_and_row_and_col_count']['row_count_with_data']
    col_count = pd.steps['get_uploaded_sheet_and_row_and_col_count']['col_count']
    headers = pd.steps['get_uploaded_sheet_and_row_and_col_count']['headers']
    sheet_id = pd.steps["trigger"]["event"]["id"]
    token = f'{pd.inputs["google_sheets"]["$auth"]["oauth_access_token"]}'

    # Extract the Google Sheet data for each row and process them
    for i in range(1, row_count_with_data): # Start from 1 to skip the header row
        new_row = get_sheet_row(i, headers, sheet_id, token)
        person_org_data = extract_person_org_data(new_row)
        if person_org_data:
            pd.export(f"person_data_{i}", person_org_data["person"])
            pd.export(f"org_data_{i}", person_org_data["org"])

def extract_person_org_data(new_row):
    input_sheet_columns = {
        "source_data": [4, 5],
        "name": 0,
        "job_title": 8,
        "industry": 9,
        "org_name": 10,
        "size": 12
    }

    # Split source_data using ","
    source_data = []
    for col in input_sheet_columns["source_data"]:
        if new_row[col]:
            source_data.extend([s.strip() for s in new_row[col].split(",")])

    email = next((e for e in source_data if e), None)

    name = new_row[input_sheet_columns["name"]]
    job_title = new_row[input_sheet_columns["job_title"]]
    industry = new_row[input_sheet_columns["industry"]]
    org_name = new_row[input_sheet_columns["org_name"]]
    size = new_row[input_sheet_columns["size"]]
    
    if size == "18568" or size == "Nov-50":
        size = "11-50"

    if not email:
        return None

    industry = industry.split(",")[0].strip() if industry else None

    person = {
            "name": name,
            "title": job_title,
            "email": email
        }

    org = {
            "name": org_name,
            "size": size,
            "industry": industry
        }

    return {
        "person": person,
        "org": org
    }