import requests
import json
import os

def add_lead(pd: "pipedream", person_id, organization_id, person_name):
    api_token = os.environ['PIPEDRIVE_API_KEY']
    lead_data = {
        "title": person_name,
        "owner_id": 16325261,
        "person_id": person_id,
        "organization_id": organization_id,
    }

    create_lead_url = f'https://api-proxy.pipedrive.com/v1/leads?api_token={api_token}'

    # Log details for debugging
    print(f"Creating lead for Person ID: {person_id} ({person_name}), Organization ID: {organization_id}")
    print(f"Lead Data: {lead_data}")

    try:
        response = requests.post(create_lead_url, data=json.dumps(lead_data), headers={"Content-Type": "application/json"})
        response.raise_for_status()
        print(f"Lead was added successfully! {response.json()}")
    except requests.exceptions.HTTPError as e:
        print(f"Adding a lead failed: {e}")

def handler(pd: "pipedream"):
    persons_to_create = pd.steps["filter_and_create_person"]["persons_to_create"]

    for person_data_key, person_data in persons_to_create.items():
        person_id = person_data["id"]
        organization_id = person_data["org_id"]
        person_name = person_data["name"]

        add_lead(pd, person_id, organization_id, person_name)