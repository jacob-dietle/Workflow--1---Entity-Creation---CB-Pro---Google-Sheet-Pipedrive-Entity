import http.client
import json

def handler(pd: "pipedream"):

    org_data = pd.steps["filter_and_create_organization"]["orgs_to_create"]
    person_data = pd.steps["filter_and_create_person"]["persons_to_create"]

    # Setup the connection
    conn = http.client.HTTPSConnection("eo6yszyiys6x00y.m.pipedream.net")

    for org_key, org_value in org_data.items():
        # Find the corresponding person_data with the same key suffix
        person_key = f"person_data_{org_key.split('_')[-1]}"
        person_value = person_data[person_key]

        # Combine person and org data into a single JSON object
        combined_data = {
            "source": "workflow_1/cb pro",
            "org_data": org_value,
            "person_data": person_value
        }

        # Convert the JSON object into its string representation
        combined_data_str = json.dumps(combined_data)

        # Make the HTTP request with the required headers and send the combined data as the body
        conn.request("POST", "/", combined_data_str, {"Content-Type": "application/json"})

        response = conn.getresponse()
        if response.status == 200:
            print(f"Request for pair {person_key} and {org_key} was successful!")
        else:
            print(f"Request for pair {person_key} and {org_key} failed with status code {response.status}.")

        # Close connection to reset connection for the next request
        conn.close()
        conn = http.client.HTTPSConnection("eo6yszyiys6x00y.m.pipedream.net")

    conn.close()