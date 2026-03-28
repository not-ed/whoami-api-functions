import logging
import azure.functions as func
import os
import requests
import mssql_python
import json

def IngestNewGithubEvents():
    github_username = os.getenv("GitHubUsername", None)
    database_server_name = os.getenv("DatabaseServerName", None)
    database_name = os.getenv("DatabaseName", None)
    database_username = os.getenv("DatabaseUsername", None)
    database_password = os.getenv("DatabasePassword", None)
    if None in [github_username, database_server_name, database_name, database_username, database_password]:
        print("Missing required configuration. Skipping run...")

    print(f"Fething public GitHub events for {github_username}...")
    events_response = requests.get(f"https://api.github.com/users/{github_username}/events", headers={"X-GitHub-Api-Version": "2026-03-10"})
    if not events_response.ok:
        return

    events = events_response.json()
    if len(events) == 0:
        print(f"No public events found for {github_username}. Skipping run...")

    pending_event_ids = []    
    for event in events:
        pending_event_ids.append(int(event["id"]))
    print(f"{len(pending_event_ids)} events retrieved:")
    print(f"Pending Event IDs: {pending_event_ids}")

    SQL_CONNECTION_STRING=f"Server={database_server_name};Database={database_name};Encrypt=yes;TrustServerCertificate=no;Authentication=SqlPassword;UID={database_username};PWD={database_password}"
    print(f"Connecting to {database_name} ({database_server_name}) as {database_username}...")
    connection = mssql_python.connect(SQL_CONNECTION_STRING)

    print(f"Checking if Pending Event IDs already exist in {database_name}...")
    fetch_existing_ids_query = connection.execute(f"SELECT id FROM github_events WHERE id IN {"("+",".join(str(i) for i in pending_event_ids)+")"};");
    for row in fetch_existing_ids_query.fetchall():
        if row[0] in pending_event_ids:
            print(f"Event ID {row[0]} already exists. Removing from pending list...")
            pending_event_ids.remove(row[0])
    
    print(f"Remaining Pending Event IDs: {pending_event_ids}")
    if len(pending_event_ids) > 0:
        insert_all_pending_events_query = "INSERT INTO github_events (id, body, created_at) VALUES "

        for event in events:
            if int(event["id"]) in pending_event_ids:
                insert_all_pending_events_query = insert_all_pending_events_query + f"({int(event["id"])}, '{json.dumps(event)}', '{event["created_at"]}'),"
        insert_all_pending_events_query = (insert_all_pending_events_query + ";").replace(",;",";")

        connection.execute(insert_all_pending_events_query)
        connection.commit()
    else:
        print("No new Events to save. Skipping writes to database...")

    connection.close()

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def github_events_import(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('GitHub Events Import Initiated.')
    IngestNewGithubEvents()