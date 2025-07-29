import csv
import requests
import os
import time
import logging
from collections import Counter
from dotenv import load_dotenv
from datetime import datetime, timedelta
import glob

# --- Configure Logging ---

# Create logs directory if it doesn't exist
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Define log file path with date only for daily overwrite
log_filename = datetime.now().strftime('sync_log_%Y%m%d.txt')
log_filepath = os.path.join(log_dir, log_filename)

file_logger = logging.getLogger('file_logger')
console_logger = logging.getLogger('console_logger')

def cleanup_old_logs(log_directory, max_logs=30):
    """
    Cleans up old log files in the specified directory, keeping only the most recent 'max_logs' files.
    """
    all_log_files = sorted(glob.glob(os.path.join(log_directory, 'sync_log_*.txt')))
    
    file_logger.debug(f"Current log files found: {len(all_log_files)}")

    if len(all_log_files) >= max_logs:
        num_to_delete = len(all_log_files) - max_logs + 1 # +1 to make space for the new log
        file_logger.info(f"More than {max_logs-1} log files found. Deleting {num_to_delete} oldest log(s).")
        for i in range(num_to_delete):
            try:
                os.remove(all_log_files[i])
                file_logger.info(f"Deleted old log file: {all_log_files[i]}")
            except OSError as e:
                file_logger.error(f"Error deleting old log file {all_log_files[i]}: {e}")


load_dotenv()
SNIPEIT_API_BASE_URL = os.environ.get('SNIPEIT_API_BASE_URL', '')
SNIPEIT_API_TOKEN = os.environ.get('SNIPEIT_API_TOKEN', '')
SNIPEIT_USER_PASSWORD = os.environ.get('SNIPEIT_USER_PASSWORD', '')

if not SNIPEIT_API_TOKEN:
    pass 

BIGFIX_CSV_FILE = 'bigfix_export.csv'
DIRECTORY_CSV_FILE = 'directory_export.csv'
MULTI_USER_ADMINS_CSV_FILE = 'multi_user_admins.csv' # New CSV for Priority 4

# Removed: USER_ADDITION_LOG_CSV constant

MODEL_COLUMN = 'Model'
MANUFACTURER_COLUMN = 'Manufacturer'
CATEGORY_COLUMN = 'Device Type'
SERIAL_COLUMN = 'Serial'
COMPUTER_NAME_COLUMN = 'Computer Name'
LAST_REPORT_TIME_COLUMN = 'Last Report Time'
BIGFIX_USERNAME_COLUMN = 'User Name' # Primary user source

# New columns for fallback user lookup
FIREFOX_USERS_COLUMN = 'Firefox Users'
CHROME_USERS = 'Chrome Users'
NYU_WIFI_USERS_COLUMN = 'nyu Wi-Fi Users'

USER_EMPLOYEE_NET_ID_COLUMN = 'EmployeeNetId'
USER_EMPLOYEE_ID_COLUMN = 'EmployeeID'
USER_FIRST_NAME_COLUMN = 'FirstName'
USER_MIDDLE_NAME_COLUMN = 'MiddleName'
USER_LAST_NAME_COLUMN = 'LastName'
USER_EMAIL_COLUMN = 'EmployeeEmailAddress'

DEFAULT_CATEGORY_NAME = 'Desktop'
DEFAULT_STATUS_LABEL = 'Ready to Deploy'
CHECKED_OUT_STATUS_LABEL = 'Deployed'

DEFAULT_LOCATION_NAME = 'Office'
TARGET_COMPANY_NAME = 'NYU - Tandon School of Engineering'

REQUEST_DELAY_SECONDS = 0.2
MAX_API_LIMIT_PER_REQUEST = 500

SERIAL_SKIP_LIST = ["0123456789", "To be filled by O.E.M.", "System Serial Number"]

HEADERS = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {SNIPEIT_API_TOKEN}',
    'Content-Type': 'application/json',
}

SNIPEIT_STATUS_NAMES_BY_ID = {}


def get_snipeit_data_paginated(endpoint):
    """Fetches all data from a Snipe-IT API endpoint with pagination."""
    all_items = []
    offset = 0
    total_fetched = 0

    file_logger.info(f"  Fetching from {endpoint}...")
    while True:
        params = {'limit': MAX_API_LIMIT_PER_REQUEST, 'offset': offset}
        url = f"{SNIPEIT_API_BASE_URL}/{endpoint}"
        try:
            response = requests.get(url, headers=HEADERS, params=params)
            response.raise_for_status()
            time.sleep(REQUEST_DELAY_SECONDS)

            data = response.json()
            rows = data.get('rows', [])
            total_count = data.get('total', 0)

            all_items.extend(rows)
            total_fetched += len(rows)

            file_logger.info(f"    Fetched {total_fetched}/{total_count} items from {endpoint} (offset: {offset})")

            if not rows or len(rows) < MAX_API_LIMIT_PER_REQUEST:
                break

            offset += MAX_API_LIMIT_PER_REQUEST

        except requests.exceptions.RequestException as e:
            file_logger.error(f"Error fetching data from {endpoint} (offset: {offset}): {e}")
            break
    return all_items

def get_asset_details_from_snipeit(asset_id):
    """Fetches the current details of an asset from Snipe-IT."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware/{asset_id}"
    response = None
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        asset_data = response.json()
        
        if asset_data and 'id' in asset_data:
            current_status_id = asset_data.get('status_label', {}).get('id')
            current_assigned_to_id = None
            if asset_data.get('assigned_to') and asset_data['assigned_to']['type'] == 'user':
                current_assigned_to_id = asset_data['assigned_to'].get('id')
            elif asset_data.get('assigned_to') and asset_data['assigned_to'].get('type') == 'location':
                current_assigned_to_id = asset_data['assigned_to'].get('id')
            elif asset_data.get('assigned_to') and asset_data['assigned_to'].get('type') == 'asset':
                current_assigned_to_id = asset_data['assigned_to'].get('id')
            return current_status_id, current_assigned_to_id
        else:
            return None, None
    except requests.exceptions.RequestException as e:
        file_logger.error(f"Error fetching asset details for ID {asset_id}: {e}")
        if response is not None and hasattr(response, 'status_code'):
            file_logger.error(f"Snipe-IT API ERROR response (Fetch Details - HTTP Status {response.status_code}): {response.text}")
        return None, None
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def get_asset_details_from_snipeit_raw_notes(asset_id):
    """Fetches the current details of an asset, specifically for its raw notes content."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware/{asset_id}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        asset_data = response.json()
        return asset_data.get('notes', '')
    except requests.exceptions.RequestException as e:
        file_logger.error(f"Error fetching raw notes for asset ID {asset_id}: {e}")
        return ''
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)


def create_snipeit_manufacturer(manu_name):
    """Creates a new manufacturer in Snipe-IT."""
    url = f"{SNIPEIT_API_BASE_URL}/manufacturers"
    payload = {'name': manu_name}
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        file_logger.info(f"Successfully created manufacturer: {manu_name}")
        return response.json()['payload']['id']
    except requests.exceptions.RequestException as e:
        if response is not None and response.json():
            response_json = response.json()
            if 'messages' in response_json and 'name' in response_json['messages'] and 'already been taken' in response_json['messages']['name'][0]:
                file_logger.info(f"Manufacturer '{manu_name}' already exists in Snipe-IT. Skipping creation.")
                return None
            file_logger.error(f"Error creating manufacturer {manu_name}: {e}")
            file_logger.error(f"Snipe-IT API response: {response_json}")
        else:
            file_logger.error(f"Error creating manufacturer {manu_name}: {e} (No JSON response body)")
        return None
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def create_snipeit_model(model_data):
    """Creates a new model in Snipe-IT."""
    url = f"{SNIPEIT_API_BASE_URL}/models"
    try:
        response = requests.post(url, headers=HEADERS, json=model_data)
        response.raise_for_status()
        file_logger.info(f"Successfully created model: {model_data['name']}")
        return response.json()['payload']['id']
    except requests.exceptions.RequestException as e:
        if response is not None and response.json():
            response_json = response.json()
            if 'messages' in response_json and 'name' in response_json['messages'] and 'already been taken' in response_json['messages']['name'][0]:
                file_logger.info(f"Model '{model_data['name']}' with this Manufacturer and Category already exists in Snipe-IT. Skipping creation.")
                return None
            file_logger.error(f"Error creating model {model_data.get('name', 'N/A')}: {e}")
            file_logger.error(f"Snipe-IT API response: {response_json}")
        else:
            file_logger.error(f"Error creating model {model_data.get('name', 'N/A')}: {e} (No JSON response body)")
        return None
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def create_snipeit_asset(asset_data):
    """Creates a new asset in Snipe-IT."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware"
    try:
        response = requests.post(url, headers=HEADERS, json=asset_data)
        response.raise_for_status()
        file_logger.info(f"Successfully created asset: {asset_data['asset_tag']} - {asset_data.get('name', 'N/A')}")
        return response.json()
    except requests.exceptions.RequestException as e:
        if response is not None and response.json():
            response_json = response.json()
            if 'messages' in response_json and 'asset_tag' in response_json['messages'] and 'already been taken' in response_json['messages']['asset_tag'][0]:
                file_logger.info(f"Asset with tag '{asset_data['asset_tag']}' already exists in Snipe-IT. Skipping creation.")
                return None
            file_logger.error(f"Error creating asset {asset_data.get('name', 'N/A')}: {e}")
            file_logger.error(f"Snipe-IT API response: {response_json}")
        else:
            file_logger.error(f"Error creating asset {asset_data.get('name', 'N/A')}: {e} (No JSON response body)")
        return None
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def delete_snipeit_asset(asset_id):
    """Deletes an asset from Snipe-IT."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware/{asset_id}"
    try:
        response = requests.delete(url, headers=HEADERS)
        response.raise_for_status()
        file_logger.info(f"Successfully deleted asset with ID: {asset_id}")
        return True
    except requests.exceptions.RequestException as e:
        file_logger.error(f"Error deleting asset with ID {asset_id}: {e}")
        if response is not None and response.json():
            file_logger.error(f"Snipe-IT API response: {response.json()}")
        return False
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def create_snipeit_user(user_data):
    """
    Creates a new user in Snipe-IT.
    Returns the API response JSON on success, or a structured error dictionary on failure.
    """
    url = f"{SNIPEIT_API_BASE_URL}/users"
    response = None
    try:
        response = requests.post(url, headers=HEADERS, json=user_data)
        response.raise_for_status()
        file_logger.info(f"Successfully created user: {user_data.get('username', 'N/A')} (HTTP Status: {response.status_code})")
        return {'status': 'success', 'payload': response.json().get('payload')}
    except requests.exceptions.RequestException as e:
        status_code = response.status_code if response is not None else 'N/A'
        response_json = {}
        if response is not None and response.json():
            try:
                response_json = response.json()
            except ValueError: # Not a JSON response
                response_json = {'messages': f"Non-JSON API response: {response.text}"}
        
        # Capture specific duplicate messages for structured logging
        duplicate_fields = []
        if 'messages' in response_json:
            if 'username' in response_json['messages'] and 'already been taken' in response_json['messages']['username'][0]:
                duplicate_fields.append('username')
            if 'employee_num' in response_json['messages'] and 'already been taken' in response_json['messages']['employee_num'][0]:
                duplicate_fields.append('employee_num')
            if 'email' in response_json['messages'] and 'already been taken' in response_json['messages']['email'][0]:
                duplicate_fields.append('email')
        
        if duplicate_fields:
            # This case is largely handled by pre-checks now, but remains as a robust fallback
            file_logger.info(f"User with username '{user_data.get('username')}' (Employee ID: {user_data.get('employee_num')}, Email: {user_data.get('email')}) already exists based on API fields: {', '.join(duplicate_fields)}. Skipping creation (HTTP Status: {status_code}).")
            return {'status': 'error', 'message': 'Duplicate user detected by API', 'duplicate_fields': duplicate_fields, 'http_status': status_code}
        
        if 'messages' in response_json and 'password' in response_json['messages']:
            file_logger.error(f"Password validation error for user '{user_data.get('username')}': {response_json['messages']['password']} (HTTP Status: {status_code}).")
            return {'status': 'error', 'message': f"Password validation error: {response_json['messages']['password'][0]}", 'http_status': status_code}
        
        # General API error
        error_message = response_json.get('messages', str(e))
        file_logger.error(f"Error creating user {user_data.get('username', 'N/A')}: {e} (HTTP Status: {status_code})")
        file_logger.error(f"Snipe-IT API response: {response_json}")
        return {'status': 'error', 'message': f"API Error: {error_message}", 'http_status': status_code}
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def checkout_asset_to_user(asset_id, user_id, notes=""):
    """Checks out an asset to a specified user. Snipe-IT usually sets status to 'Deployed' implicitly."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware/{asset_id}/checkout"
    payload = {
        "checkout_to_type": "user",
        "assigned_user": user_id,
        "note": notes,
    }

    file_logger.debug(f"\n--- Attempting Checkout ---")
    file_logger.debug(f"  Asset ID: {asset_id}")
    file_logger.debug(f"  User ID: {user_id}")
    file_logger.debug(f"  API Endpoint URL: {url}")
    file_logger.debug(f"  Payload being sent: {payload}")

    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response_json = response.json() # Get JSON response even on non-200 to check status key

        if response.status_code == 200 and response_json.get('status') == 'success':
            file_logger.info(f"Successfully checked out asset ID {asset_id} to user ID {user_id}. HTTP Status: {response.status_code}")
            return True
        else:
            file_logger.error(f"Error checking out asset ID {asset_id} to user ID {user_id}. HTTP Status: {response.status_code}")
            file_logger.error(f"Snipe-IT API ERROR response (Checkout - Application Error): {response_json.get('messages', response.text)}")
            return False
    except requests.exceptions.RequestException as e:
        file_logger.error(f"Error checking out asset ID {asset_id} to user ID {user_id}: {e}")
        if response is not None:
            file_logger.error(f"Snipe-IT API response (HTTP Status {response.status_code}): {response.text}")
        else:
            file_logger.error(f"No response from Snipe-IT API.")
        return False
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def checkin_asset(asset_id, status_id, notes=""):
    """Checks in an asset with more robust success/error checking."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware/{asset_id}/checkin"
    payload = {
        'status_id': status_id,
        'note': notes,
    }

    status_name = SNIPEIT_STATUS_NAMES_BY_ID.get(status_id, f"Unknown Status ID: {status_id}")

    file_logger.debug(f"\n--- Attempting Checkin ---")
    file_logger.debug(f"  Asset ID: {asset_id}")
    file_logger.debug(f"  Target Status Label: '{status_name}' (ID: {status_id})")
    file_logger.debug(f"  API Endpoint URL: {url}")
    file_logger.debug(f"  Payload being sent: {payload}")

    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response_json = response.json() # Get JSON response even on non-200 to check status key

        if response.status_code == 200 and response_json.get('status') == 'success':
            file_logger.info(f"Successfully checked in asset ID {asset_id}. HTTP Status: {response.status_code}")
            return True
        else:
            file_logger.error(f"Error checking in asset ID {asset_id}. HTTP Status: {response.status_code}")
            file_logger.error(f"Snipe-IT API ERROR response (Checkin - Application Error): {response_json.get('messages', response.text)}")
            return False
    except requests.exceptions.RequestException as e:
        file_logger.error(f"Error checking in asset ID {asset_id}: {e}")
        if response is not None:
            file_logger.error(f"Snipe-IT API response (HTTP Status {response.status_code}): {response.text}")
        else:
            file_logger.error(f"No response from Snipe-IT API.")
        return False
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def update_asset_status(asset_id, status_id, notes=""):
    """Updates an asset's status."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware/{asset_id}"
    payload = {}
    if status_id is not None: # Allow updating notes without changing status
        payload["status_id"] = status_id
    if notes is not None:
        payload["notes"] = notes

    status_name = SNIPEIT_STATUS_NAMES_BY_ID.get(status_id, f"Unknown Status ID: {status_id}")

    file_logger.debug(f"\n--- Attempting to UPDATE Asset ID: {asset_id} Status ---")
    file_logger.debug(f"  Target Status Label: '{status_name}' (ID: {status_id})")
    file_logger.debug(f"  API Endpoint URL: {url}")
    file_logger.debug(f"  Payload being sent: {payload}")

    if not payload:
        file_logger.info(f"  No status or notes provided for asset ID {asset_id}. Skipping update.")
        return False

    try:
        response = requests.put(url, headers=HEADERS, json=payload)
        response.raise_for_status() # HTTP errors are still caught here
        file_logger.info(f"Successfully updated asset ID {asset_id} status to {status_id}. HTTP Status: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        file_logger.error(f"Error updating asset ID {asset_id} status: {e}")
        if response is not None and hasattr(response, 'status_code'):
            file_logger.error(f"Snipe-IT API ERROR response (Update Status - HTTP Status {response.status_code}): {response.text}")
        else:
            file_logger.error(f"No response from Snipe-IT API or response object malformed.")
        return False
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)


def parse_last_report_time(time_str):
    """
    Parses BigFix 'Last Report Time' string into a datetime object.
    Example format: "July 15, 2025 10:55 AM"
    """
    try:
        return datetime.strptime(time_str.strip(), '%B %d, %Y %I:%M %p')
    except ValueError:
        file_logger.warning(f"Warning: Could not parse Last Report Time '{time_str}'. Using minimum date.")
        return datetime.min

def update_bigfix_last_report_in_notes(current_notes, new_report_time):
    """
    Updates or adds the BigFix Last Report Time in the asset's notes string.
    Preserves other notes content.
    """
    lines = current_notes.split('\n')
    new_report_line = f"BigFix Last Report: {new_report_time.strftime('%Y-%m-%d %H:%M:%S')}"
    
    updated_lines = []
    found_bigfix_line = False
    for line in lines:
        if 'BigFix Last Report:' in line:
            updated_lines.append(new_report_line)
            found_bigfix_line = True
        else:
            updated_lines.append(line)
    
    if not found_bigfix_line:
        updated_lines.append(new_report_line)
    
    return '\n'.join(updated_lines).strip()

def extract_netid(text):
    """
    Extracts a NetID from various string formats (username or part before '@' in email).
    Converts to lowercase and removes '@nyu.edu' or other domains if present.
    Handles formats like "tjb38", "todernst@gmail.com", "( Person 1, todernst@gmail.com )",
    "( Work, tjb387@nyu.edu )", "( nyu, tjb387 )".
    """
    if not text:
        return None

    text = text.strip().lower()

    # Split by common delimiters and take the first non-empty part
    # Then split by '@' and take the first part
    parts_by_comma = [p.strip() for p in text.split(',')]
    for part in parts_by_comma:
        if part.startswith('(') and part.endswith(')'):
            # If it's something like "( Work, tjb387 )", extract tjb387
            inner_content = part[1:-1].strip()
            if ',' in inner_content:
                parts_inner = [p.strip() for p in inner_content.split(',')]
                for p_inner in parts_inner:
                    if '@' in p_inner:
                        return p_inner.split('@')[0]
                    elif p_inner:
                        return p_inner
            elif '@' in inner_content:
                return inner_content.split('@')[0]
            elif inner_content:
                return inner_content
        else:
            if '@' in part:
                return part.split('@')[0]
            elif part:
                return part
    
    return None # If no suitable NetID found after all attempts

def find_best_netid(row, directory_data_for_user_lookup, snipeit_users):
    """
    Finds the best NetID from fallback columns based on occurrence count
    and validation against directory and Snipe-IT users.
    Returns the best NetID (string) or None if no suitable NetID is found.
    """
    netid_candidates = Counter()
    
    # Columns to check for NetIDs
    columns_to_check = [FIREFOX_USERS_COLUMN, CHROME_USERS, NYU_WIFI_USERS_COLUMN]

    for col in columns_to_check:
        column_data = row.get(col, '')
        if column_data:
            # Split by comma and process each part
            parts = [p.strip() for p in column_data.split(',')]
            for part in parts:
                netid = extract_netid(part)
                if netid:
                    netid_candidates[netid] += 1

    if not netid_candidates:
        return None

    # Sort candidates by count (descending) and then alphabetically (ascending) for tie-breaking
    sorted_candidates = sorted(netid_candidates.items(), key=lambda item: (-item[1], item[0]))

    best_qualifying_netid = None
    max_count_found = -1 
    qualifying_netids_at_max_count = [] 

    # Iterate through candidates, prioritizing by count
    for netid, count in sorted_candidates:
        if max_count_found != -1 and count < max_count_found:
            break

        # Check against DIRECTORY_CSV_FILE
        dir_match = None
        for dir_row in directory_data_for_user_lookup:
            if dir_row.get(USER_EMPLOYEE_NET_ID_COLUMN, '').strip().lower() == netid:
                dir_match = dir_row
                break
        
        if dir_match:
            # Check if user exists in Snipe-IT (checking both by employee_num and username keys)
            employee_id_from_dir = dir_match.get(USER_EMPLOYEE_ID_COLUMN, '').strip()
            snipeit_user_exists = False
            if employee_id_from_dir and employee_id_from_dir in snipeit_users:
                snipeit_user_exists = True
            elif netid in snipeit_users: # Check by username (netid) if employee_id didn't match or was missing
                snipeit_user_exists = True

            if snipeit_user_exists:
                if count > max_count_found:
                    max_count_found = count
                    best_qualifying_netid = netid
                    qualifying_netids_at_max_count = [netid] 
                elif count == max_count_found:
                    qualifying_netids_at_max_count.append(netid) 

    if len(qualifying_netids_at_max_count) == 1:
        return qualifying_netids_at_max_count[0]
    elif len(qualifying_netids_at_max_count) > 1:
        file_logger.info(f"  Multiple NetIDs ({', '.join(qualifying_netids_at_max_count)}) tied with max count {max_count_found} and qualified. No assignment for this device.")
        return None
    
    return None 

def sync_directory_users_to_snipeit(directory_data_for_user_lookup, snipeit_users): # Removed user_addition_results parameter
    """
    Reads the directory export CSV, checks for user existence in Snipe-IT,
    and attempts to create new users if they don't exist.
    Logs outcomes to the main log file.
    """
    users_added_count = 0
    users_skipped_count = 0

    file_logger.info(f"\n--- Processing Directory Export CSV: {DIRECTORY_CSV_FILE} for Users ---")
    try:
        # Changed encoding to 'latin-1' to handle potential character issues
        with open(DIRECTORY_CSV_FILE, mode='r', encoding='latin-1') as csvfile:
            reader = csv.DictReader(csvfile)

            user_required_cols = [
                USER_EMPLOYEE_NET_ID_COLUMN,
                USER_EMPLOYEE_ID_COLUMN,
                USER_FIRST_NAME_COLUMN,
                USER_LAST_NAME_COLUMN,
                USER_EMAIL_COLUMN
            ]
            user_missing_cols = [col for col in user_required_cols if col not in reader.fieldnames]

            if user_missing_cols:
                reason = f"Missing required user columns in '{DIRECTORY_CSV_FILE}': {', '.join(user_missing_cols)}. Available: {', '.join(reader.fieldnames)}"
                file_logger.error(f"Error: {reason}")
                # No longer appending to user_addition_results
                return users_added_count, users_skipped_count # Exit early if critical CSV error

            for row_num, row in enumerate(reader, start=2):
                employee_id = row.get(USER_EMPLOYEE_ID_COLUMN)
                employee_net_id = row.get(USER_EMPLOYEE_NET_ID_COLUMN)
                first_name = row.get(USER_FIRST_NAME_COLUMN)
                last_name = row.get(USER_LAST_NAME_COLUMN)
                email = row.get(USER_EMAIL_COLUMN)

                # Removed: user_add_result dictionary initialization

                # --- Basic validation for user data ---
                if not all([employee_id, employee_net_id, first_name, last_name, email]):
                    missing_fields = []
                    if not employee_id: missing_fields.append('EmployeeID')
                    if not employee_net_id: missing_fields.append('EmployeeNetId')
                    if not first_name: missing_fields.append('FirstName')
                    if not last_name: missing_fields.append('LastName')
                    if not email: missing_fields.append('EmployeeEmailAddress')

                    reason = f"Missing required user data: {', '.join(missing_fields)}"
                    file_logger.warning(f"Skipping user in row {row_num}: {reason} for '{employee_net_id}' (ID: {employee_id}).")
                    users_skipped_count += 1
                    # No longer appending to user_addition_results
                    continue
                
                # Strip whitespace from relevant fields
                employee_id = employee_id.strip()
                employee_net_id = employee_net_id.strip()
                first_name = first_name.strip()
                last_name = last_name.strip()
                email = email.strip()

                directory_data_for_user_lookup.append(row) # Add to lookup list for asset assignment later

                duplicate_found = False
                duplicate_reason = ""
                duplicate_user_info = {}
                matched_fields = []

                # --- Duplicate Check Logic ---
                # Check 1: By Employee ID
                if employee_id in snipeit_users:
                    duplicate_user_info = snipeit_users[employee_id]
                    duplicate_reason = f"User with EmployeeID '{employee_id}' already exists in Snipe-IT."
                    matched_fields.append('EmployeeID')
                    duplicate_found = True
                
                # Check 2: By Username (NetID) - only if not already found by Employee ID
                if not duplicate_found:
                    # Iterate through values to find match regardless of key type (employee_num or username)
                    for existing_user_key, existing_user_data in snipeit_users.items():
                        if existing_user_data.get('username') and existing_user_data['username'].lower() == employee_net_id.lower():
                            duplicate_user_info = existing_user_data
                            duplicate_reason = f"User with username '{employee_net_id}' already exists in Snipe-IT."
                            matched_fields.append('Username')
                            duplicate_found = True
                            break # Found a duplicate by username, no need to check others

                # Check 3: By Email - only if not already found by Employee ID or Username
                if not duplicate_found:
                    for existing_user_key, existing_user_data in snipeit_users.items():
                        if existing_user_data.get('email') and existing_user_data['email'].lower() == email.lower():
                            duplicate_user_info = existing_user_data
                            duplicate_reason = f"User with email '{email}' already exists in Snipe-IT."
                            matched_fields.append('Email')
                            duplicate_found = True
                            break # Found a duplicate by email, no need to check others

                if duplicate_found:
                    file_logger.info(f"{duplicate_reason} (Attempted NetID: {employee_net_id}, EmployeeID: {employee_id}). Matched fields: {', '.join(matched_fields)}. Skipping creation.")
                    file_logger.info(f"  Attempted: NetID='{employee_net_id}', EmployeeID='{employee_id}', Email='{email}'")
                    file_logger.info(f"  Existing: NetID='{duplicate_user_info.get('username', 'N/A')}', EmployeeID='{duplicate_user_info.get('employee_num', 'N/A')}', Email='{duplicate_user_info.get('email', 'N/A')}'")
                    users_skipped_count += 1
                    # No longer appending to user_addition_results
                    continue

                # If no duplicates found by any criteria, proceed to create user
                user_payload = {
                    'first_name': first_name,
                    'last_name': last_name,
                    'username': employee_net_id,
                    'employee_num': employee_id,
                    'email': email,
                    'password': SNIPEIT_USER_PASSWORD,
                    'password_confirmation': SNIPEIT_USER_PASSWORD,
                    'activated': True,
                    'can_login': False,
                    'ldap_import': True
                }
                
                file_logger.debug(f"DEBUG: Attempting to create user: {employee_net_id} (ID: {employee_id})")
                created_user_response = create_snipeit_user(user_payload)
                
                if created_user_response and created_user_response.get('status') == 'success':
                    users_added_count += 1
                    new_user_id = created_user_response['payload']['id']
                    # Add newly created user to our local cache for immediate de-duplication within the same run
                    # Store by both employee_id and username (lowercase) for robust lookup
                    snipeit_users[employee_id] = {
                        'id': new_user_id,
                        'username': employee_net_id.lower(),
                        'email': email.lower(),
                        'first_name': first_name.lower(),
                        'last_name': last_name.lower(),
                        'employee_num': employee_id # Store the employee_num for easier lookup later
                    }
                    if employee_id.lower() != employee_net_id.lower(): # Avoid redundant key if employee_id and netid are the same
                        snipeit_users[employee_net_id.lower()] = snipeit_users[employee_id]

                    # No longer appending to user_addition_results
                else:
                    users_skipped_count += 1
                    reason = created_user_response.get('message', 'Unknown API error') if created_user_response else 'Unknown error during API call or no response'
                    file_logger.error(f"Failed to create user {employee_net_id} (ID: {employee_id}): {reason}")
                    # No longer appending to user_addition_results

    except FileNotFoundError:
        file_logger.error(f"Error: Directory export CSV file '{DIRECTORY_CSV_FILE}' not found. Skipping user import.")
        # No longer appending to user_addition_results
    except Exception as e:
        file_logger.exception(f"An error occurred while reading the directory CSV file: {e}")
        # No longer appending to user_addition_results
    
    return users_added_count, users_skipped_count


def main():
    global SNIPEIT_STATUS_NAMES_BY_ID
    global file_logger, console_logger 

    # --- Configure Loggers at the start of main() ---
    file_logger.setLevel(logging.DEBUG)
    if file_logger.handlers:
        for handler in file_logger.handlers[:]:
            file_logger.removeHandler(handler)
    file_handler = logging.FileHandler(log_filepath, mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    file_logger.addHandler(file_handler)

    console_logger.setLevel(logging.INFO)
    if console_logger.handlers:
        for handler in console_logger.handlers[:]:
            console_logger.removeHandler(handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    console_logger.addHandler(console_handler)
    console_logger.propagate = False
    # --- End Logger Configuration ---

    cleanup_old_logs(log_dir, max_logs=30)

    # Removed: Clean up old user addition log CSVs logic
    # Removed: console_logger.info(f"User addition results will be saved to: {USER_ADDITION_LOG_CSV}")

    console_logger.info(f"") 
    console_logger.info(f"=====================================================================")
    console_logger.info(f"                 Snipe-IT Sync Script Started!")
    console_logger.info(f"=====================================================================")
    console_logger.info(f"Detailed logs are being saved to: {log_filepath}")
    console_logger.info(f"") 

    if not SNIPEIT_API_TOKEN:
        file_logger.warning("SNIPEIT_API_TOKEN environment variable not set. Please set it in your .env file.")
    elif not SNIPEIT_USER_PASSWORD:
        file_logger.warning("SNIPEIT_USER_PASSWORD environment variable not set. Users might fail to create due to missing password.")
    else:
        file_logger.info("SNIPEIT_API_TOKEN and SNIPEIT_USER_PASSWORD successfully loaded.")


    snipeit_manufacturers = {}
    snipeit_categories = {}
    snipeit_models = {}
    snipeit_statuses = {}
    snipeit_locations = {}
    snipeit_companies = {}
    snipeit_assets = {}
    snipeit_users = {} # Dictionary to store existing users
    directory_data_for_user_lookup = [] # List to store directory data for user lookup during asset assignment
    # Removed: user_addition_results = [] # New: List to store data for user addition CSV

    eng_format_devices_found = 0
    assigned_via_priority_3 = 0
    assigned_via_priority_4 = 0 # New: Counter for Priority 4 assignments
    eng_format_device_names = [] 

    file_logger.info("--- Collecting Existing Snipe-IT Data (Paginated) ---")

    status_list = get_snipeit_data_paginated('statuslabels')
    if not status_list:
        file_logger.error("ERROR: No status labels found in Snipe-IT. Cannot proceed with asset management.")
        return
    for status in status_list:
        snipeit_statuses[status['name'].lower()] = status['id']
        SNIPEIT_STATUS_NAMES_BY_ID[status['id']] = status['name']
    file_logger.info(f"Total {len(snipeit_statuses)} status labels collected from Snipe-IT API.")

    default_status_id = snipeit_statuses.get(DEFAULT_STATUS_LABEL.lower())
    checked_out_status_id = snipeit_statuses.get(CHECKED_OUT_STATUS_LABEL.lower())
    checkin_status_id = snipeit_statuses.get(DEFAULT_STATUS_LABEL.lower())

    if not default_status_id:
        file_logger.error(f"Error: Default Status Label '{DEFAULT_STATUS_LABEL}' not found in Snipe-IT. Cannot proceed.")
        return
    if not checked_out_status_id:
        file_logger.error(f"Error: Checked Out Status Label '{CHECKED_OUT_STATUS_LABEL}' not found in Snipe-IT. Cannot proceed.")
        return
    if not checkin_status_id:
        file_logger.error(f"Error: Check-in Status Label '{DEFAULT_STATUS_LABEL}' not found in Snipe-IT. Cannot proceed.")
        return

    file_logger.info(f"Identified Default Status ID: {default_status_id} ('{DEFAULT_STATUS_LABEL}')")
    file_logger.info(f"Identified Checked Out Status ID: {checked_out_status_id} ('{CHECKED_OUT_STATUS_LABEL}')")
    file_logger.info(f"Identified Check-in Status ID: {checkin_status_id} ('{DEFAULT_STATUS_LABEL}')")

    manu_list = get_snipeit_data_paginated('manufacturers')
    for manu in manu_list:
        snipeit_manufacturers[manu['name'].lower()] = manu['id']
    file_logger.info(f"Total {len(snipeit_manufacturers)} manufacturers collected from Snipe-IT.")

    cat_list = get_snipeit_data_paginated('categories')
    for cat in cat_list:
        snipeit_categories[cat['name'].lower()] = cat['id']
    file_logger.info(f"Total {len(snipeit_categories)} categories collected from Snipe-IT.")

    location_list = get_snipeit_data_paginated('locations')
    for loc in location_list:
        snipeit_locations[loc['name'].lower()] = loc['id']
    file_logger.info(f"Total {len(snipeit_locations)} locations collected from Snipe-IT.")

    company_list = get_snipeit_data_paginated('companies')
    for company in company_list:
        snipeit_companies[company['name'].lower()] = company['id']
    file_logger.info(f"Total {len(snipeit_companies)} companies collected from Snipe-IT.")

    file_logger.info("Collecting existing models from Snipe-IT (paginated)...")
    existing_models_raw = get_snipeit_data_paginated('models')
    for model in existing_models_raw:
        model_name_clean = model['name'].strip().lower()
        manufacturer_id = model['manufacturer']['id'] if model.get('manufacturer') else None
        category_id = model['category']['id'] if model.get('category') else None
        if manufacturer_id and category_id:
            snipeit_models[(model_name_clean, manufacturer_id, category_id)] = model['id']
    file_logger.info(f"Total {len(snipeit_models)} existing models collected from Snipe-IT.")

    file_logger.info("Collecting existing assets from Snipe-IT (paginated)...")
    existing_assets_raw = get_snipeit_data_paginated('hardware')
    for asset in existing_assets_raw:
        asset_tag = asset.get('asset_tag')
        asset_id = asset.get('id')
        asset_name = asset.get('name')
        notes = asset.get('notes', '')
        checked_out_to_id = None
        if asset.get('assigned_to') and asset['assigned_to']['type'] == 'user':
            checked_out_to_id = asset['assigned_to'].get('id')

        if asset_tag and asset_id:
            last_report_time_from_notes = datetime.min
            notes_match = [line for line in notes.split('\n') if 'BigFix Last Report:' in line]
            if notes_match:
                try:
                    time_str = notes_match[0].split('BigFix Last Report:')[1].strip()
                    last_report_time_from_notes = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                except ValueError as e:
                    file_logger.warning(f"Warning: Could not parse Last Report Time from notes for asset {asset_tag}: '{notes_match[0]}'. Error: {e}")

            snipeit_assets[asset_tag.upper()] = {
                'id': asset_id,
                'name': asset_name.strip().lower() if asset_name else '',
                'last_report_time': last_report_time_from_notes,
                'checked_out_to_id': checked_out_to_id
            }
    file_logger.info(f"Total {len(snipeit_assets)} existing assets collected from Snipe-IT.")

    file_logger.info("Collecting existing users from Snipe-IT (paginated)...")
    for user in get_snipeit_data_paginated('users'):
        employee_num = user.get('employee_num')
        username = user.get('username')
        user_id = user.get('id')

        email = user.get('email', '') or ''
        first_name = user.get('first_name', '') or ''
        last_name = user.get('last_name', '') or ''

        # Store users by employee_num if available, and also by username for robust lookup
        user_data_for_cache = {
            'id': user_id,
            'username': (username.strip().lower() if username else None),
            'email': email.strip().lower(),
            'first_name': first_name.strip().lower(),
            'last_name': last_name.lower(),
            'employee_num': employee_num.strip() if employee_num else None # Ensure employee_num is always in the cached data
        }

        if employee_num:
            snipeit_users[employee_num.strip()] = user_data_for_cache
        if username: # Ensure username is also a lookup key
            snipeit_users[username.strip().lower()] = user_data_for_cache
    file_logger.info(f"Total {len(snipeit_users)} existing users collected from Snipe-IT.")

    # --- Phase 1: Synchronize Users from Directory Export CSV ---
    users_added_this_run, users_skipped_this_run = sync_directory_users_to_snipeit(
        directory_data_for_user_lookup, snipeit_users
    )
    # Update main counters
    users_added_count = users_added_this_run
    users_skipped_count = users_skipped_this_run


    file_logger.info(f"\n--- Processing BigFix CSV: {BIGFIX_CSV_FILE} for Assets ---")
    bigfix_data = {}
    skipped_serial_count = 0
    try:
        # Changed encoding to 'latin-1' to handle potential character issues
        with open(BIGFIX_CSV_FILE, mode='r', encoding='latin-1') as csvfile:
            reader = csv.DictReader(csvfile)

            required_cols = [MODEL_COLUMN, MANUFACTURER_COLUMN, CATEGORY_COLUMN, SERIAL_COLUMN, COMPUTER_NAME_COLUMN, LAST_REPORT_TIME_COLUMN, BIGFIX_USERNAME_COLUMN]
            required_cols.extend([FIREFOX_USERS_COLUMN, CHROME_USERS, NYU_WIFI_USERS_COLUMN])

            missing_cols = [col for col in required_cols if col not in reader.fieldnames]

            if missing_cols:
                file_logger.error(f"Error: Missing required columns in CSV: {', '.join(missing_cols)}")
                file_logger.error(f"Available columns in CSV: {', '.join(reader.fieldnames)}")
                return

            for row_num, row in enumerate(reader, start=2):
                serial = row.get(SERIAL_COLUMN)
                last_report_time_str = row.get(LAST_REPORT_TIME_COLUMN)
                computer_name_from_csv = row.get(COMPUTER_NAME_COLUMN, '')

                if not serial or not serial.strip():
                    file_logger.warning(f"Skipping row {row_num}: Missing serial number for asset '{computer_name_from_csv}'.")
                    skipped_serial_count += 1
                    continue

                serial_upper = serial.strip().upper()

                if serial_upper in [s.upper() for s in SERIAL_SKIP_LIST]:
                    file_logger.info(f"Skipping row {row_num}: Serial '{serial_upper}' is in the skip list.")
                    skipped_serial_count += 1
                    continue

                if not last_report_time_str:
                    file_logger.warning(f"Skipping row {row_num}: Missing 'Last Report Time' for asset '{computer_name_from_csv}' (Serial: {serial}).")
                    skipped_serial_count += 1
                    continue

                parsed_time = parse_last_report_time(last_report_time_str)

                if serial_upper not in bigfix_data or parsed_time > bigfix_data[serial_upper]['parsed_last_report_time']:
                    bigfix_data[serial_upper] = {
                        'row': row,
                        'parsed_last_report_time': parsed_time
                    }
        file_logger.info(f"Found {len(bigfix_data)} unique (by serial, latest report time) devices in the BigFix CSV after filtering.")
        if skipped_serial_count > 0:
            file_logger.info(f"Skipped {skipped_serial_count} BigFix entries due to missing or invalid serials/times, or serials in the skip list.")

    except FileNotFoundError:
        file_logger.error(f"Error: CSV file '{BIGFIX_CSV_FILE}' not found. Skipping asset import.")
        bigfix_data = {}
    except Exception as e:
        file_logger.exception(f"An error occurred while reading the BigFix CSV file: {e}")
        return

    file_logger.info("\n--- Populating Manufacturers in Snipe-IT (Asset-related) ---")
    manufacturers_added_count = 0
    bigfix_unique_manufacturers_from_csv = set(d['row'].get(MANUFACTURER_COLUMN).strip() for d in bigfix_data.values() if d['row'].get(MANUFACTURER_COLUMN))

    for manu_name in sorted(list(bigfix_unique_manufacturers_from_csv)):
        if manu_name.lower() not in snipeit_manufacturers:
            file_logger.info(f"Manufacturer '{manu_name}' not found in Snipe-IT. Attempting to create...")
            new_id = create_snipeit_manufacturer(manu_name)
            if new_id:
                manufacturers_added_count += 1
                snipeit_manufacturers[manu_name.lower()] = new_id
    file_logger.info(f"Successfully added {manufacturers_added_count} new manufacturers to Snipe-IT.")

    file_logger.info("\n--- Populating Models in Snipe-IT (Asset-related) ---")
    models_added_count = 0
    bigfix_models_to_process = {}
    for serial_upper, device_data in bigfix_data.items():
        row = device_data['row']
        model_name = row.get(MODEL_COLUMN).strip()
        manufacturer_name = row.get(MANUFACTURER_COLUMN).strip()
        category_name = row.get(CATEGORY_COLUMN, DEFAULT_CATEGORY_NAME).strip()

        model_csv_key = (model_name.lower(), manufacturer_name.lower(), category_name.lower())

        if model_csv_key not in bigfix_models_to_process:
            manufacturer_id_for_model = snipeit_manufacturers.get(manufacturer_name.lower())
            category_id_for_model = snipeit_categories.get(category_name.lower())

            if not manufacturer_id_for_model:
                file_logger.warning(f"Warning: Manufacturer '{manufacturer_name}' for model '{model_name}' not found/created. Skipping model collection.")
                continue
            if not category_id_for_model:
                file_logger.warning(f"Warning: Category '{category_name}' for model '{model_name}' not found. Using default '{DEFAULT_CATEGORY_NAME}'.")
                category_id_for_model = snipeit_categories.get(DEFAULT_CATEGORY_NAME.lower())
                if not category_id_for_model:
                    file_logger.warning(f"Warning: Default category '{DEFAULT_CATEGORY_NAME}' not found. Skipping model collection.")
                    continue

            bigfix_models_to_process[model_csv_key] = {
                'name': model_name,
                'manufacturer_id': manufacturer_id_for_model,
                'category_id': category_id_for_model,
                'model_number': model_name
            }

    for model_csv_key in sorted(bigfix_models_to_process.keys()):
        model_data = bigfix_models_to_process[model_csv_key]

        model_name_clean_for_lookup = model_data['name'].lower()
        manufacturer_id = model_data['manufacturer_id']
        category_id = model_data['category_id']

        model_snipeit_lookup_key = (model_name_clean_for_lookup, manufacturer_id, category_id)
        if model_snipeit_lookup_key in snipeit_models:
            continue

        payload = {
            'name': model_data['name'],
            'category_id': category_id,
            'manufacturer_id': manufacturer_id,
            'model_number': model_data['model_number']
        }
        new_model_id = create_snipeit_model(payload)
        if new_model_id:
            models_added_count += 1
            snipeit_models[model_snipeit_lookup_key] = new_model_id

    file_logger.info(f"Successfully added {models_added_count} new models to Snipe-IT.")

    # New: Read multi_user_admins.csv
    file_logger.info(f"\n--- Processing Multi-User Admins CSV: {MULTI_USER_ADMINS_CSV_FILE} ---")
    try:
        # Changed encoding to 'latin-1' to handle potential character issues
        with open(MULTI_USER_ADMINS_CSV_FILE, mode='r', encoding='latin-1') as csvfile:
            reader = csv.DictReader(csvfile)
            admin_required_cols = ["Name Schema", "Admin", "netid"]
            admin_missing_cols = [col for col in admin_required_cols if col not in reader.fieldnames]

            if admin_missing_cols:
                file_logger.error(f"Error: Missing required columns in '{MULTI_USER_ADMINS_CSV_FILE}': {', '.join(admin_missing_cols)}")
                file_logger.error(f"Available columns in CSV: {', '.join(reader.fieldnames)}")
                # Decide if to exit or continue without this data
            else:
                for row_num, row in enumerate(reader, start=2):
                    name_schema = row.get("Name Schema", "").strip().lower()
                    admin_netid = row.get("netid", "").strip().lower()
                    if name_schema and admin_netid:
                        multi_user_admins_data.append({'name_schema': name_schema, 'netid': admin_netid})
                    else:
                        file_logger.warning(f"Skipping row {row_num} in '{MULTI_USER_ADMINS_CSV_FILE}': Missing 'Name Schema' or 'netid'.")
        file_logger.info(f"Loaded {len(multi_user_admins_data)} entries from '{MULTI_USER_ADMINS_CSV_FILE}'.")
    except FileNotFoundError:
        file_logger.warning(f"Warning: Multi-User Admins CSV file '{MULTI_USER_ADMINS_CSV_FILE}' not found. Priority 4 assignment will be skipped.")
        multi_user_admins_data = [] # Ensure it's empty if file not found
    except Exception as e:
        file_logger.exception(f"An error occurred while reading the Multi-User Admins CSV file: {e}")
        multi_user_admins_data = []

    file_logger.info(f"\n--- Populating Assets in Snipe-IT ---")
    assets_added_count = 0
    assets_deleted_count = 0 
    assets_skipped_final_count = 0

    default_location_id = snipeit_locations.get(DEFAULT_LOCATION_NAME.lower())
    target_company_id = snipeit_companies.get(TARGET_COMPANY_NAME.lower())

    if not default_location_id:
        file_logger.error(f"Error: Default Location '{DEFAULT_LOCATION_NAME}' not found in Snipe-IT. Cannot create assets. Please create it.")
        return
    if not target_company_id:
        file_logger.error(f"Error: Target Company '{TARGET_COMPANY_NAME}' not found in Snipe-IT. Cannot create assets. Please ensure it exists.")
        return

    for serial_upper, device_data in bigfix_data.items():
        row = device_data['row']
        bigfix_last_report_time = device_data['parsed_last_report_time']

        serial = row.get(SERIAL_COLUMN).strip()
        computer_name_from_csv = row.get(COMPUTER_NAME_COLUMN)
        computer_name_for_asset = computer_name_from_csv.strip() if computer_name_from_csv and computer_name_from_csv.strip() else serial

        computer_name_lower = computer_name_for_asset.lower()
        if computer_name_lower.startswith('eng-'):
            eng_format_devices_found += 1
            eng_format_device_names.append(computer_name_for_asset)
            file_logger.debug(f"  Computer name '{computer_name_for_asset}' matched broad 'ENG-' prefix. (Total {eng_format_devices_found})")

        model_name = row.get(MODEL_COLUMN).strip()
        manufacturer_name = row.get(MANUFACTURER_COLUMN).strip()
        category_name = row.get(CATEGORY_COLUMN, DEFAULT_CATEGORY_NAME).strip()

        notes = f"BigFix Last Report: {bigfix_last_report_time.strftime('%Y-%m-%d %H:%M:%S')}"

        manufacturer_id = snipeit_manufacturers.get(manufacturer_name.lower())
        category_id = snipeit_categories.get(category_name.lower())

        model_lookup_key = (model_name.lower(), manufacturer_id, category_id)
        model_id = snipeit_models.get(model_lookup_key)

        if not manufacturer_id or not category_id or not model_id:
            file_logger.warning(f"Skipping asset '{computer_name_for_asset}' (Serial: {serial}): Required Manufacturer ID, Category ID, or Model ID not found/created. Skipping asset and user association.")
            assets_skipped_final_count += 1
            continue

        existing_snipeit_asset = snipeit_assets.get(serial_upper)

        asset_id_for_workflow = None

        if existing_snipeit_asset:
            file_logger.info(f"Asset '{computer_name_for_asset}' (Serial: {serial}) already exists in Snipe-IT with ID {existing_snipeit_asset['id']}. Using existing asset.")
            asset_id_for_workflow = existing_snipeit_asset['id']
            if bigfix_last_report_time > existing_snipeit_asset['last_report_time']:
                file_logger.info(f"Updating last report time for existing asset {serial_upper} (ID: {asset_id_for_workflow}).")
                current_notes_on_snipeit = get_asset_details_from_snipeit_raw_notes(asset_id_for_workflow)
                updated_notes = update_bigfix_last_report_in_notes(current_notes_on_snipeit, bigfix_last_report_time)
                if updated_notes != current_notes_on_snipeit:
                    update_asset_status(asset_id_for_workflow, None, notes=updated_notes)
                snipeit_assets[serial_upper]['last_report_time'] = bigfix_last_report_time

        else:
            asset_payload = {
                'asset_tag': serial,
                'name': computer_name_for_asset,
                'model_id': model_id,
                'status_id': default_status_id,
                'location_id': default_location_id,
                'company_id': target_company_id,
                'serial': serial,
                'notes': notes
            }
            file_logger.debug(f"DEBUG: Attempting to create asset with Tag: '{asset_payload['asset_tag']}', Name: '{asset_payload['name']}', Company ID: '{asset_payload['company_id']}'")

            created_asset_response = create_snipeit_asset(asset_payload)
            if created_asset_response:
                assets_added_count += 1
                asset_id_for_workflow = created_asset_response['payload']['id']
                snipeit_assets[serial_upper] = {
                    'id': asset_id_for_workflow,
                    'name': computer_name_for_asset.lower(),
                    'last_report_time': bigfix_last_report_time,
                    'checked_out_to_id': None
                }
            else:
                file_logger.info(f"Skipping user association for asset {serial} as it was not created successfully.")
                assets_skipped_final_count += 1
                continue

        # --- User Lookup and Checkout Logic (Prioritized) ---
        # This logic now ONLY looks up users in the already synchronized snipeit_users cache.
        # It does NOT attempt to create users here.
        
        user_name_for_assignment = None
        snipeit_target_user_info = None

        # Priority 1: Primary "User Name" column
        bigfix_primary_user_name = row.get(BIGFIX_USERNAME_COLUMN, '').strip()
        if bigfix_primary_user_name:
            file_logger.debug(f"  Attempting Priority 1 lookup for primary user: '{bigfix_primary_user_name}'")
            found_user_in_directory_csv_primary = None
            for dir_row in directory_data_for_user_lookup:
                if dir_row.get(USER_EMPLOYEE_NET_ID_COLUMN, '').strip().lower() == bigfix_primary_user_name.lower():
                    found_user_in_directory_csv_primary = dir_row
                    break
            
            if found_user_in_directory_csv_primary:
                target_employee_id_primary = found_user_in_directory_csv_primary.get(USER_EMPLOYEE_ID_COLUMN, '').strip()
                # Check snipeit_users by Employee ID first, then by username (NetID)
                if target_employee_id_primary and target_employee_id_primary in snipeit_users:
                    snipeit_target_user_info = snipeit_users[target_employee_id_primary]
                    user_name_for_assignment = bigfix_primary_user_name
                    file_logger.info(f"  User found via Priority 1 (Primary 'User Name') using Employee ID: '{user_name_for_assignment}'")
                elif bigfix_primary_user_name.lower() in snipeit_users: # Check by username key if employee_id not found
                    snipeit_target_user_info = snipeit_users[bigfix_primary_user_name.lower()]
                    user_name_for_assignment = bigfix_primary_user_name
                    file_logger.info(f"  User found via Priority 1 (Primary 'User Name') using NetID key: '{user_name_for_assignment}'")
                else:
                    file_logger.info(f"  Priority 1: Primary BigFix user '{bigfix_primary_user_name}' found in directory data but NOT in Snipe-IT. Moving to Priority 2. (User must be synced first)")
            else:
                file_logger.info(f"  Priority 1: Primary BigFix user '{bigfix_primary_user_name}' not found in directory data. Moving to Priority 2.")
        else:
            file_logger.info(f"  Priority 1: Primary 'User Name' column is empty. Moving to Priority 2.")

        # Priority 2: Fallback columns (Firefox Users, Chrome Users, nyu Wi-Fi Users)
        if not snipeit_target_user_info:
            file_logger.debug(f"  Initiating Priority 2 (Fallback columns) lookup for asset {serial}.")
            best_fallback_netid = find_best_netid(row, directory_data_for_user_lookup, snipeit_users)
            
            if best_fallback_netid:
                file_logger.info(f"  Priority 2: Fallback lookup identified '{best_fallback_netid}' as the best candidate.")
                user_name_for_assignment = best_fallback_netid
                
                # Check snipeit_users by Employee ID first, then by username (NetID)
                found_in_snipeit_cache = False
                for dir_row in directory_data_for_user_lookup: # Iterate dir_data to get employee_id for lookup
                    if dir_row.get(USER_EMPLOYEE_NET_ID_COLUMN, '').strip().lower() == best_fallback_netid:
                        employee_id_from_dir_p2 = dir_row.get(USER_EMPLOYEE_ID_COLUMN, '').strip()
                        if employee_id_from_dir_p2 and employee_id_from_dir_p2 in snipeit_users:
                            snipeit_target_user_info = snipeit_users[employee_id_from_dir_p2]
                            found_in_snipeit_cache = True
                            break
                
                if not found_in_snipeit_cache: # Also check by username key if employee_id didn't match
                    if user_name_for_assignment.lower() in snipeit_users:
                        snipeit_target_user_info = snipeit_users[user_name_for_assignment.lower()]
                        found_in_snipeit_cache = True

                if not found_in_snipeit_cache:
                    file_logger.info(f"  Warning: Priority 2 NetID '{user_name_for_assignment}' found in directory data but NOT in Snipe-IT. Moving to Priority 3. (User must be synced first)")
            else:
                file_logger.info(f"  Priority 2: No suitable user found after fallback lookup for asset {serial}. Moving to Priority 3.")
        
        # Priority 3: Computer Name (ENG-*-*)
        if not snipeit_target_user_info:
            file_logger.debug(f"  Initiating Priority 3 (Computer Name) lookup for asset {serial} with name '{computer_name_for_asset}'.")
            
            if computer_name_lower.startswith('eng-') and '-' in computer_name_lower[4:]:
                parts = computer_name_lower.split('-')
                if len(parts) >= 2 and parts[0] == 'eng':
                    extracted_netid = parts[1]
                    file_logger.info(f"  Priority 3: Extracted potential NetID '{extracted_netid}' from Computer Name '{computer_name_for_asset}' for assignment attempt.")

                    dir_match_third_priority = None
                    for dir_row in directory_data_for_user_lookup:
                        if dir_row.get(USER_EMPLOYEE_NET_ID_COLUMN, '').strip().lower() == extracted_netid:
                            dir_match_third_priority = dir_row
                            break
                    
                    if dir_match_third_priority:
                        target_employee_id_third_priority = dir_row.get(USER_EMPLOYEE_ID_COLUMN, '').strip()
                        # Check snipeit_users by Employee ID first, then by username (NetID)
                        if target_employee_id_third_priority and target_employee_id_third_priority in snipeit_users:
                            snipeit_target_user_info = snipeit_users[target_employee_id_third_priority]
                            user_name_for_assignment = extracted_netid
                            assigned_via_priority_3 += 1 
                            file_logger.info(f"  User found via Priority 3 (Computer Name) using Employee ID: '{user_name_for_assignment}'")
                        elif extracted_netid.lower() in snipeit_users: # Check by username key if employee_id not found
                            snipeit_target_user_info = snipeit_users[extracted_netid.lower()]
                            user_name_for_assignment = extracted_netid
                            assigned_via_priority_3 += 1
                            file_logger.info(f"  User found via Priority 3 (Computer Name) using NetID key: '{user_name_for_assignment}'")
                        else:
                            file_logger.info(f"  Priority 3: Computer Name NetID '{extracted_netid}' found in directory data but NOT in Snipe-IT. Moving to Priority 4. (User must be synced first)")
                    else:
                        file_logger.info(f"  Priority 3: Computer Name NetID '{extracted_netid}' not found in directory data. Moving to Priority 4.")
                else:
                    file_logger.debug(f"  Priority 3: Computer Name '{computer_name_for_asset}' does not have a valid 'ENG-NetID' structure for extraction using split logic. Moving to Priority 4.")
            else:
                file_logger.debug(f"  Priority 3: Computer Name '{computer_name_for_asset}' does not match 'ENG-*-*' format for NetID extraction. Moving to Priority 4.")

        # Priority 4: multi_user_admins.csv lookup (NEW)
        if not snipeit_target_user_info and multi_user_admins_data:
            file_logger.debug(f"  Initiating Priority 4 (Multi-User Admins) lookup for asset {serial} with name '{computer_name_for_asset}'.")
            
            found_p4_match = False
            for admin_entry in multi_user_admins_data:
                name_schema = admin_entry['name_schema']
                admin_netid = admin_entry['netid']
                
                if computer_name_lower.startswith(name_schema):
                    file_logger.info(f"  Priority 4: Computer Name '{computer_name_for_asset}' matches schema '{name_schema}'. Checking for admin '{admin_netid}'.")
                    
                    # Check if the admin_netid exists in the directory data
                    dir_match_p4 = None
                    for dir_row in directory_data_for_user_lookup:
                        if dir_row.get(USER_EMPLOYEE_NET_ID_COLUMN, '').strip().lower() == admin_netid:
                            dir_match_p4 = dir_row
                            break
                    
                    if dir_match_p4:
                        # Check if the user (by employee ID or username) exists in Snipe-IT
                        employee_id_from_dir_p4 = dir_match_p4.get(USER_EMPLOYEE_ID_COLUMN, '').strip()
                        snipeit_user_exists_p4 = False
                        
                        if employee_id_from_dir_p4 and employee_id_from_dir_p4 in snipeit_users:
                            snipeit_target_user_info = snipeit_users[employee_id_from_dir_p4]
                            user_name_for_assignment = admin_netid
                            snipeit_user_exists_p4 = True
                        elif admin_netid in snipeit_users: # Fallback to username if EmployeeID didn't match or was missing
                            snipeit_target_user_info = snipeit_users[admin_netid]
                            user_name_for_assignment = admin_netid
                            snipeit_user_exists_p4 = True
                        
                        if snipeit_user_exists_p4:
                            assigned_via_priority_4 += 1
                            file_logger.info(f"  User found via Priority 4 (Multi-User Admin) and validated: '{user_name_for_assignment}'. Assigning device.")
                            found_p4_match = True
                            break # Found a match and assigned, stop checking other schemas for this device
                        else:
                            file_logger.info(f"  Priority 4: Admin NetID '{admin_netid}' found in directory data but NOT in Snipe-IT. Skipping assignment for this schema. (User must be synced first)")
                    else:
                        file_logger.info(f"  Priority 4: Admin NetID '{admin_netid}' from schema '{name_schema}' not found in directory data. Skipping assignment for this schema.")
            
            if not found_p4_match:
                file_logger.info(f"  Priority 4: No suitable Multi-User Admin schema match found for asset {serial}.")

        if not snipeit_target_user_info:
            file_logger.info(f"  No suitable user found after all lookup priorities for asset {serial}. Skipping checkout.")
            assets_skipped_final_count += 1
            continue

        if asset_id_for_workflow and snipeit_target_user_info:
            snipeit_user_id = snipeit_target_user_info['id']
            
            current_snipeit_status, current_snipeit_assigned_to_id = get_asset_details_from_snipeit(asset_id_for_workflow)

            file_logger.debug(f"\nDEBUG: Asset {serial} (ID: {asset_id_for_workflow}) Initial Snipe-IT State Check:")
            file_logger.debug(f"  Current Status ID: {current_snipeit_status} ('{SNIPEIT_STATUS_NAMES_BY_ID.get(current_snipeit_status, 'Unknown')}')")
            file_logger.debug(f"  Current Assigned To ID: {current_snipeit_assigned_to_id if current_snipeit_assigned_to_id else 'None'}")
            file_logger.debug(f"  Target User ID for Checkout: {snipeit_user_id} (NetID: {user_name_for_assignment})")
            file_logger.debug(f"  Expected Checkin Status ID: {checkin_status_id} ('{DEFAULT_STATUS_LABEL}')")
            file_logger.debug(f"  Expected Checked Out Status ID: {checked_out_status_id} ('{CHECKED_OUT_STATUS_LABEL}')")

            if current_snipeit_assigned_to_id == snipeit_user_id and current_snipeit_status == checked_out_status_id:
                file_logger.info(f"Asset ID {asset_id_for_workflow} (Serial: {serial}) is already checked out to the correct user ID {snipeit_user_id} and status {SNIPEIT_STATUS_NAMES_BY_ID.get(checked_out_status_id)}. Skipping checkout.")
            else:
                file_logger.info(f"\n--- Workflow for Asset ID {asset_id_for_workflow} (Serial: {serial}) - Preparing for Checkout ---")
                file_logger.info(f"  Current Snipe-IT State: Status ID {current_snipeit_status}, Assigned To: {current_snipeit_assigned_to_id}")

                if current_snipeit_status != checkin_status_id:
                    file_logger.info(f"  Asset is NOT in '{DEFAULT_STATUS_LABEL}' state. Attempting to set status to '{DEFAULT_STATUS_LABEL}' ({checkin_status_id}).")
                    status_update_notes = f"Automatic status update to '{DEFAULT_STATUS_LABEL}' before check-in/checkout workflow. Last BigFix Report: {bigfix_last_report_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    if update_asset_status(asset_id_for_workflow, checkin_status_id, notes=status_update_notes):
                        file_logger.info(f"  Successfully set asset ID {asset_id_for_workflow} status to '{DEFAULT_STATUS_LABEL}'.")
                        current_snipeit_status, current_snipeit_assigned_to_id = get_asset_details_from_snipeit(asset_id_for_workflow)
                        if current_snipeit_status != checkin_status_id:
                            file_logger.error(f"  ❌ Status update verification failed. Asset is still not in '{DEFAULT_STATUS_LABEL}' state after update attempt.")
                            file_logger.error(f"     Actual: Status {current_snipeit_status} ('{SNIPEIT_STATUS_NAMES_BY_ID.get(current_snipeit_status, 'Unknown')}').")
                            file_logger.info(f"  Skipping checkout for asset {serial} due to inconsistent state after status update.")
                            assets_skipped_final_count += 1
                            continue
                    else:
                        file_logger.error(f"  Failed to set asset {serial} status to '{DEFAULT_STATUS_LABEL}'. Skipping checkout.")
                        assets_skipped_final_count += 1
                        continue
                else:
                    file_logger.info(f"  Asset is already in '{DEFAULT_STATUS_LABEL}' state. Proceeding to check assignment.")

                if current_snipeit_assigned_to_id is not None:
                    file_logger.info(f"  Asset is currently assigned to user ID {current_snipeit_assigned_to_id}. Attempting to check in asset.")
                    
                    checkin_notes = (
                        f"Automatic check-in based on BigFix data before re-assignment. "
                        f"Previous assignment: {current_snipeit_assigned_to_id if current_snipeit_assigned_to_id else 'None'}. "
                        f"Last BigFix Report: {bigfix_last_report_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )

                    if checkin_asset(asset_id_for_workflow, checkin_status_id, notes=checkin_notes):
                        file_logger.info(f"  Successfully attempted check-in for asset ID {asset_id_for_workflow}.")
                        current_snipeit_status, current_snipeit_assigned_to_id = get_asset_details_from_snipeit(asset_id_for_workflow)
                        if current_snipeit_status == checkin_status_id and current_snipeit_assigned_to_id is None:
                            file_logger.info(f"  ✅ Post-check-in state confirmed: Asset is now '{SNIPEIT_STATUS_NAMES_BY_ID.get(checkin_status_id)}' and unassigned.")
                        else:
                            file_logger.error(f"  ❌ Post-check-in state verification failed. Asset is not fully in '{SNIPEIT_STATUS_NAMES_BY_ID.get(checkin_status_id)}' state or is still assigned.")
                            file_logger.error(f"     Actual: Status {current_snipeit_status} ('{SNIPEIT_STATUS_NAMES_BY_ID.get(current_snipeit_status, 'Unknown')}'), Assigned {current_snipeit_assigned_to_id}.")
                            file_logger.info(f"  Skipping checkout for asset {serial} due to inconsistent state after check-in.")
                            assets_skipped_final_count += 1
                            continue
                    else:
                        file_logger.error(f"  Failed to check in asset {serial}. Skipping checkout.")
                        assets_skipped_final_count += 1
                        continue
                else:
                    file_logger.info(f"  Asset is already unassigned. Proceeding to checkout.")

                file_logger.info(f"  Attempting to check out asset ID {asset_id_for_workflow} to user ID {snipeit_user_id}.")
                checkout_notes = f"Automatically checked out based on BigFix 'User Name': {user_name_for_assignment}. Last BigFix Report: {bigfix_last_report_time.strftime('%Y-%m-%d %H:%M:%S')}"
                if checkout_asset_to_user(asset_id_for_workflow, snipeit_user_id, checkout_notes):
                    file_logger.info(f"  Successfully initiated checkout for asset {asset_id_for_workflow}.")
                    snipeit_assets[serial_upper]['checked_out_to_id'] = snipeit_user_id
                    current_snipeit_status, current_snipeit_assigned_to_id = get_asset_details_from_snipeit(asset_id_for_workflow)
                    if current_snipeit_status == checked_out_status_id and current_snipeit_assigned_to_id == snipeit_user_id:
                        file_logger.info(f"  ✅ Checkout confirmed: Asset is now {SNIPEIT_STATUS_NAMES_BY_ID.get(checked_out_status_id)} and assigned to {snipeit_user_id}.")
                    else:
                        file_logger.warning(f"  ⚠️ Checkout verification failed. Expected Status {SNIPEIT_STATUS_NAMES_BY_ID.get(checked_out_status_id)} (ID {checked_out_status_id}) and assigned to User ID {snipeit_user_id}.")
                        file_logger.warning(f"     Actual: Status {SNIPEIT_STATUS_NAMES_BY_ID.get(current_snipeit_status)} (ID {current_snipeit_status}), Assigned To: {current_snipeit_assigned_to_id if current_snipeit_assigned_to_id else 'Nobody'}")
                        file_logger.info(f"     Attempting to force status update to '{CHECKED_OUT_STATUS_LABEL}'.")
                        update_notes = f"Forced status update after BigFix-driven checkout. Last BigFix Report: {bigfix_last_report_time.strftime('%Y-%m-%d %H:%M:%S')}"
                        if update_asset_status(asset_id_for_workflow, checked_out_status_id, update_notes):
                            file_logger.info(f"  Successfully forced status to '{CHECKED_OUT_STATUS_LABEL}'.") # Corrected variable name
                        else:
                            file_logger.error(f"  Failed to force status to '{CHECKED_OUT_STATUS_LABEL}'. Manual intervention may be needed.")
                else:
                    file_logger.error(f"  Failed to check out asset {asset_id_for_workflow} to user {snipeit_user_id}.")
                    snipeit_assets[serial_upper]['checked_out_to_id'] = current_snipeit_assigned_to_id
        else:
            if asset_id_for_workflow:
                file_logger.info(f"No BigFix username or suitable fallback user found for asset {serial}. Skipping user association/checkout.")
                assets_skipped_final_count += 1

    # Removed: Logic to write user addition results to CSV

    console_logger.info(f"\n--- Sync Summary ---")
    console_logger.info(f"Manufacturers Added: {manufacturers_added_count}")
    console_logger.info(f"Models Added: {models_added_count}")
    console_logger.info(f"Users Added: {users_added_count}")
    console_logger.info(f"Assets Added: {assets_added_count}")
    console_logger.info(f"BigFix Entries Skipped (initial filter): {skipped_serial_count}")
    console_logger.info(f"Devices Matching 'ENG-' Prefix (Total Count): {eng_format_devices_found}")
    console_logger.info(f"Devices Assigned via Priority 3: {assigned_via_priority_3}")
    console_logger.info(f"Devices Assigned via Priority 4: {assigned_via_priority_4}") # New summary line
    console_logger.info(f"Assets Skipped (creation/checkout issues or no user found): {assets_skipped_final_count}")

    if eng_format_device_names:
        file_logger.info(f"Names of devices matching 'ENG-' prefix: {', '.join(eng_format_device_names)}")
    else:
        file_logger.info(f"No devices found matching the 'ENG-' prefix.")
        
    console_logger.info(f"\nScript execution finished.")
    file_logger.info(f"\nScript execution finished. Detailed logs saved to: {log_filepath}")

if __name__ == '__main__':
    main()
