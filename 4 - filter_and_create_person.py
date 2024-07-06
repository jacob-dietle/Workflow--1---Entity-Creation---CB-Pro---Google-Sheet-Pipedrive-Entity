import requests
import os
import json

def get_existing_person_emails(api_token):
    existing_emails = set()

    start = 0
    limit = 500
    
    while True:
        url = f'https://api-proxy.pipedrive.com/v1/persons?api_token={api_token}&start={start}&limit={limit}'
        response = requests.get(url)
        persons = response.json()["data"]

        if not persons:
            break

        for person in persons:
            email = person["email"][0]["value"] if person["email"] else None
            if email:
                existing_emails.add(email.lower())

        start += limit

    return existing_emails

def create_person(api_token, person_data, org_id):
    headers = {'Content-Type': 'application/json'}
    create_person_url = f'https://api-proxy.pipedrive.com/v1/persons?api_token={api_token}'
    
    field_key_source = "e26474dad27469cbfc53d1fb5785b9822edd74a8"
    source_value_id = 210  # ID for the "Crunchbase Pro" option

    payload = {
        "name": person_data["name"],
        "email": person_data["email"],
        "org_id": org_id,
        field_key_source: source_value_id
    }

    try:
        response = requests.post(create_person_url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()

        person_id = response.json()["data"]["id"]
        return person_id
    except requests.exceptions.HTTPError as e:
        print(f"Error in creating person: {e}")
        print(response.text)
        return None

def handler(pd: "pipedream"):
    person_org_data = pd.steps["extract_person_org_data_from_sheet"]
    api_token = os.environ['PIPEDRIVE_API_KEY']

    existing_person_emails = get_existing_person_emails(api_token)

    persons_to_create = {}

    for key, person_data in person_org_data.items():
        if key.startswith("person_data"):
            person_email_lower = person_data["email"].lower()
            org_key = f"org_data_{key.split('_')[-1]}"

            if org_key in pd.steps["filter_and_create_organization"]["orgs_to_create"]:
                org_id = pd.steps["filter_and_create_organization"]["orgs_to_create"][org_key]["id"]

                if person_email_lower not in existing_person_emails:
                    person_id = create_person(api_token, person_data, org_id)
                    if person_id:
                        print(f"Person created: ID - {person_id}, Name - {person_data['name']}, Email - {person_data['email']}")
                        persons_to_create[key] = {**person_data, "id": person_id, "org_id": org_id}
                else:
                    print(f"Person email already exists: Email - {person_data['email']}")
    
    pd.export("persons_to_create", persons_to_create)