import yaml
import csv

# Load the YAML data from a file
with open("qfailsend.yml", "r") as f:
    data = yaml.safe_load(f)

# Determine the maximum number of links across all clients
max_links = max(len(client.get("questionnaire_links_generated", [])) for client in data.values())

# Create dynamic link column names
link_columns = []
for i in range(1, max_links + 1):
    link_columns.extend([f"link_{i}_type", f"link_{i}_url"])

# Base fields
base_fields = [
    'client_id', 'asdAdhd', 'daEval',
    'error', 'failedDate', 'fullName',
    'questionnaires_needed'
]

# Final fieldnames list
fieldnames = base_fields + link_columns

# Open CSV for writing
with open("clients.csv", "a", newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for client_id, client_data in data.items():
        row = {
            'client_id': client_id,
            'asdAdhd': client_data.get('asdAdhd', ''),
            'daEval': client_data.get('daEval', ''),
            'error': client_data.get('error', ''),
            'failedDate': client_data.get('failedDate', ''),
            'fullName': client_data.get('fullName', ''),
            'questionnaires_needed': ", ".join(client_data.get('questionnaires_needed', []))
        }

        # Add each link to its corresponding column
        links = client_data.get("questionnaire_links_generated", [])
        for i, link in enumerate(links):
            row[f"link_{i+1}_type"] = link.get('type', '')
            row[f"link_{i+1}_url"] = link.get('link', '')

        writer.writerow(row)
