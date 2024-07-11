import os
import json
import requests
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

load_dotenv()


def fetch_and_store_data():

    # response = requests.get("https://api.example.com/data")

    # if response.status_code == 200:
    #     # Getting the JSON data from the response
    #     data = response.json()
    # else:
    #     print(f"Error accessing API: {response.status_code} - {response.reason}")

    data = """
    [
        {"ip_address": "192.168.1.1", "updated_at": "2024-07-11 14:20:00"},
        {"ip_address": "10.0.0.1", "updated_at": "2024-07-11 14:23:00"},
        {"ip_address": "172.16.0.1", "updated_at": "2024-07-11 14:24:00"}
    ]
    """

    # Convert JSON string
    try:
        data = json.loads(data)
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON string: {e}")
        return

    # Filter data to only include entries from the last 30 minutes
    thirty_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=30)
    filtered_data = []
    for entry in data:
        entry_time = datetime.strptime(
            entry["updated_at"], "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc)
        if entry_time >= thirty_minutes_ago:
            filtered_data.append(entry)

    if not filtered_data:
        print("No data from the last 30 minutes")
        return

    # Define the BigQuery client
    client = bigquery.Client()

    rows_to_insert = []
    for entry in filtered_data:
        ip_info = get_ip_info(entry["ip_address"])
        entry.update(ip_info)
        rows_to_insert.append(entry)

    dataset_id = "ip_time_dataset"
    table_id = "ip_time_table"
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    errors = client.insert_rows(table, rows_to_insert)

    # Handle errors and prepare email content
    if not errors:
        email_content = (
            f"Data: {len(rows_to_insert)} rows inserted.\n\nDetails:\n{rows_to_insert}"
        )
    else:
        error_content = "\n".join(f"Error: {err}" for err in errors)
        email_content = f"Encountered errors:\n{error_content}"

    send_email(email_content)


def get_ip_info(ip):
    api_key = os.getenv("IP_API_KEY")
    url = f"https://ipinfo.io/{ip}/json?token={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": "Failed to fetch IP info"}


def send_email(content):
    sender_email = os.getenv("SENDGRID_SENDER_EMAIL")
    recipient_email = os.getenv("RECEIVER_EMAIL")
    sendgrid_api_key = os.getenv("SENDGRID_API_KEY")

    message = Mail(
        from_email=sender_email,
        to_emails=recipient_email,
        subject="Data Update",
        plain_text_content=content,
    )

    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print(f"Email sent (Status code: {response.status_code})")
    except Exception as e:
        print(f"Error sending email: {e}")


fetch_and_store_data()
