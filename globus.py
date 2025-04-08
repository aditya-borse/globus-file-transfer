import globus_sdk
import os
import sys
import json
import time

from pathlib import Path
from globus_sdk.tokenstorage import JSONTokenStorage

CLIENT_ID = "306cbb68-08a0-448c-a871-b7327a00f758"
TOKEN_FILE = os.path.expanduser("~/.globus-mvp-tokens.json")
SOURCE_ENDPOINT_ID = "18f263e1-1088-11f0-9184-02f84ea2e2ad"
DESTINATION_ENDPOINT_ID = "63f57589-ed4f-49ca-acd2-a4cfee6a73c9"

SOURCE_PATH = "/home/aditya/globus-demo/data/source.txt"
DESTINATION_PATH = "/~/destination_mvp.txt"

TRANSFER_SCOPES = "urn:globus:auth:scope:transfer.api.globus.org:all"


def get_transfer_client(client_id, token_file_path, requested_scopes):
    client = globus_sdk.NativeAppAuthClient(client_id)
    token_storage = JSONTokenStorage(token_file_path)

    if not token_storage.file_exists():
        client.oauth2_start_flow(requested_scopes=requested_scopes, refresh_tokens=True)
        authorize_url = client.oauth2_get_authorize_url()
        print(f"First time login or tokens expired.")
        print(f"Please go to this URL and login:\n\n{authorize_url}\n")
        auth_code = input("Please enter the code here: ").strip()

        try:
            token_response = client.oauth2_exchange_code_for_tokens(auth_code)
        except globus_sdk.AuthAPIError as e:
            print(f"ERROR: Authentication failed: {e.message}", file=sys.stderr)
            sys.exit(1)

        token_storage.store_token_response(token_response)
        print("Tokens stored successfully.")
    else:
        print("Using stored tokens.")

    token_data = token_storage.get_token_data(globus_sdk.TransferClient.resource_server)

    if not token_data:
        print("ERROR: Could not load valid transfer tokens from storage. Try deleting the token file and re running.", file=sys.stderr)
        print(f"Token file location: {token_file_path}", file=sys.stderr)
        sys.exit(1)

    authorizer = globus_sdk.RefreshTokenAuthorizer(
        token_data.refresh_token, 
        client, 
        access_token=token_data.access_token,
        expires_at=token_data.expires_at_seconds,
        on_refresh=token_storage.store_token_response,
    )

    transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
    return transfer_client


print("Starting Globus MVP Script...")

print("\nStep 1: Get Authenticated Client")
tc = get_transfer_client(CLIENT_ID, TOKEN_FILE, TRANSFER_SCOPES)
print("TransferClient obtained.")

print("\nStep 2: Prepare Source File")
source_file_path = Path(SOURCE_PATH)
try:
    source_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(source_file_path, "w") as f:
        f.write(f"This is a test file for Gen3 GSoC MVP.\n")
        f.write(f"Timestamp: {time.time()}\n")
    print(f"Created dummy source file at: {source_file_path}")
except Exception as e:
    print(f"ERROR: Could not create source file at {source_file_path}. Check path and permissions.", file=sys.stderr)
    print(f"Details: {e}", file=sys.stderr)
    sys.exit(1)

print("\nStep 3: Initiate Globus Transfer")
tdata = globus_sdk.TransferData(
    tc,
    SOURCE_ENDPOINT_ID,
    DESTINATION_ENDPOINT_ID,
    label="Gen3_GSoC_MVP_Transfer",
    sync_level="checksum",
)

tdata.add_item(str(source_file_path), DESTINATION_PATH)

print(f"Submitting transfer from {SOURCE_ENDPOINT_ID}:{SOURCE_PATH} to {DESTINATION_ENDPOINT_ID}:{DESTINATION_PATH}")

try:
    transfer_result = tc.submit_transfer(tdata)
    task_id = transfer_result["task_id"]
    print(f"Transfer submitted successfully. Task ID: {task_id}")
    print(f"Monitor transfer status at: https://app.globus.org/activity/{task_id}")
except globus_sdk.TransferAPIError as e:
    print(f"ERROR: Globus Transfer failed: {e.message}", file=sys.stderr)
    print(f"HTTP status: {e.http_status}", file=sys.stderr)
    print(f"Code: {e.code}", file=sys.stderr)
    try:
        os.remove(source_file_path)
        print(f"(Cleaned up dummy source file: {source_file_path})")
    except OSError:
        pass
    sys.exit(1)

print("\nStep 4: Monitor Transfer Status")
while True:
    try:
        task = tc.get_task(task_id)
        status = task["status"]
        print(f"Current task status: {status}")
        if status in ("SUCCEEDED", "FAILED"):
            break
        time.sleep(10)
    except globus_sdk.TransferAPIError as e:
         print(f"ERROR: Could not get task status: {e.message}", file=sys.stderr)
         status = "MONITOR_FAILED"
         break


if status == "SUCCEEDED":
    print("\nTransfer completed successfully.")
elif status == "FAILED":
     print(f"\nTransfer failed with status: {status}")
else:
     print(f"\nCould not confirm final transfer status.")

try:
    os.remove(source_file_path)
    print(f"\nCleaned up dummy source file: {source_file_path}")
except OSError as e:
     print(f"\nWarning: Could not clean up source file {source_file_path}: {e}")

print("\nGlobus MVP Script Finished.")