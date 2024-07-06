def handler(pd: "pipedream"):
    org_data = pd.steps["filter_and_create_organization"]["orgs_to_create"]
    person_data = pd.steps["filter_and_create_person"]["persons_to_create"]

    if not org_data and not person_data:
        print("No new organizations or persons were created. Exiting workflow early.")
        return pd.flow.exit("No new entities.")