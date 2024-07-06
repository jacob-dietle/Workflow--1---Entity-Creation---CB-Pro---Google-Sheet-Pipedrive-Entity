import requests
import os
import json

def get_existing_orgs(api_token):
    existing_orgs = {}

    url = f'https://api-proxy.pipedrive.com/v1/organizations?api_token={api_token}&start=0&limit=500'
    response = requests.get(url)
    orgs = response.json()["data"]

    for org in orgs:
        existing_orgs[org["name"].lower()] = org["id"]

    return existing_orgs

def create_organization(api_token, org_data):
    headers = {'Content-Type': 'application/json'}
    create_org_url = f'https://api-proxy.pipedrive.com/v1/organizations?api_token={api_token}'

    payload = {
        "name": org_data["name"]
    }

    try:
        response = requests.post(create_org_url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()

        org_id = response.json()["data"]["id"]
        return org_id
    except requests.exceptions.HTTPError as e:
        print(f"Error in creating organization: {e}")
        print(response.text)
        return None

def handler(pd: "pipedream"):
    person_org_data = pd.steps["extract_person_org_data_from_sheet"]
    api_token = os.environ['PIPEDRIVE_API_KEY']

    orgs_to_create = {}

    for key, org_data in person_org_data.items():
        if key.startswith("org_data"):
            org_name_lower = org_data["name"].lower()

            existing_orgs = get_existing_orgs(api_token)

            if org_name_lower not in existing_orgs:
                org_id = create_organization(api_token, org_data)
                if org_id:
                    print(f"Organization created: ID - {org_id}, Name - {org_data['name']}")
                    org_data["id"] = org_id
                    orgs_to_create[key] = org_data
            else:
                org_id = existing_orgs[org_name_lower]
                print(f"Organization already exists: ID - {org_id}, Name - {org_data['name']}")

    pd.export("orgs_to_create", orgs_to_create)