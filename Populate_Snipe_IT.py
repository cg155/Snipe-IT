import csv
import requests
import os
import time
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
SNIPEIT_API_BASE_URL = os.environ.get('SNIPEIT_API_BASE_URL', '')
SNIPEIT_API_TOKEN = os.environ.get('SNIPEIT_API_TOKEN', '')
SNIPEIT_USER_PASSWORD = os.environ.get('SNIPEIT_USER_PASSWORD', '')

if not SNIPEIT_API_TOKEN:
    print("Warning: SNIPEIT_API_TOKEN environment variable not set. Please set it in your .env file.")
elif not SNIPEIT_USER_PASSWORD:
    print("Warning: SNIPEIT_USER_PASSWORD environment variable not set. Users might fail to create due to missing password.")
else:
    print("SNIPEIT_API_TOKEN and SNIPEIT_USER_PASSWORD successfully loaded.")

BIGFIX_CSV_FILE = 'bigfix_export.csv'
DIRECTORY_CSV_FILE = 'directory_export.csv'

MODEL_COLUMN = 'Model'
MANUFACTURER_COLUMN = 'Manufacturer'
CATEGORY_COLUMN = 'Device Type'
SERIAL_COLUMN = 'Serial'
COMPUTER_NAME_COLUMN = 'Computer Name'
LAST_REPORT_TIME_COLUMN = 'Last Report Time'
BIGFIX_USERNAME_COLUMN = 'User Name'

USER_EMPLOYEE_NET_ID_COLUMN = 'EmployeeNetId'
USER_EMPLOYEE_ID_COLUMN = 'EmployeeID'
USER_FIRST_NAME_COLUMN = 'FirstName'
USER_MIDDLE_NAME_COLUMN = 'MiddleName'
USER_LAST_NAME_COLUMN = 'LastName'
USER_EMAIL_COLUMN = 'EmployeeEmailAddress'

DEFAULT_CATEGORY_NAME = 'Desktop'
DEFAULT_STATUS_LABEL = 'Ready to Deploy' # This is usually 'Deployable'
CHECKED_OUT_STATUS_LABEL = 'Deployed' # This is usually 'Undeployable'

DEFAULT_LOCATION_NAME = 'Office'
TARGET_COMPANY_NAME = 'NYU - Tandon School of Engineering'

REQUEST_DELAY_SECONDS = 0.6
MAX_API_LIMIT_PER_REQUEST = 500

SERIAL_SKIP_LIST = ["0123456789", "To be filled by O.E.M.", "System Serial Number"]

HEADERS = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {SNIPEIT_API_TOKEN}',
    'Content-Type': 'application/json',
}

SNIPEIT_STATUS_NAMES_BY_ID = {} # Global dict to map status IDs back to names

def get_snipeit_data_paginated(endpoint):
    """Fetches all data from a Snipe-IT API endpoint with pagination."""
    all_items = []
    offset = 0
    total_fetched = 0

    print(f"  Fetching from {endpoint}...")
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

            print(f"    Fetched {total_fetched}/{total_count} items from {endpoint} (offset: {offset})")

            if not rows or len(rows) < MAX_API_LIMIT_PER_REQUEST:
                break

            offset += MAX_API_LIMIT_PER_REQUEST

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {endpoint} (offset: {offset}): {e}")
            break
    return all_items

def get_asset_details_from_snipeit(asset_id):
    """Fetches the current details of an asset from Snipe-IT, similar to the test script."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware/{asset_id}"
    response = None
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        asset_data = response.json() # Direct access, no 'payload' key
        
        if asset_data and 'id' in asset_data:
            current_status_id = asset_data.get('status_label', {}).get('id')
            current_assigned_to_id = None
            if asset_data.get('assigned_to') and asset_data['assigned_to']['type'] == 'user': # Corrected line
                current_assigned_to_id = asset_data['assigned_to'].get('id')
            elif asset_data.get('assigned_to') and asset_data['assigned_to'].get('type') == 'location':
                current_assigned_to_id = asset_data['assigned_to'].get('id')
            elif asset_data.get('assigned_to') and asset_data['assigned_to'].get('type') == 'asset':
                current_assigned_to_id = asset_data['assigned_to'].get('id')
            return current_status_id, current_assigned_to_id
        else:
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching asset details for ID {asset_id}: {e}")
        if response is not None and hasattr(response, 'status_code'):
            print(f"Snipe-IT API ERROR response (Fetch Details - HTTP Status {response.status_code}): {response.text}")
        return None, None
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)


def create_snipeit_manufacturer(manu_name):
    """Creates a new manufacturer in Snipe-IT."""
    url = f"{SNIPEIT_API_BASE_URL}/manufacturers"
    payload = {'name': manu_name}
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        print(f"Successfully created manufacturer: {manu_name}")
        return response.json()['payload']['id']
    except requests.exceptions.RequestException as e:
        if response is not None and response.json():
            response_json = response.json()
            if 'messages' in response_json and 'name' in response_json['messages'] and 'already been taken' in response_json['messages']['name'][0]:
                print(f"Manufacturer '{manu_name}' already exists in Snipe-IT. Skipping creation.")
                return None
            print(f"Error creating manufacturer {manu_name}: {e}")
            print(f"Snipe-IT API response: {response_json}")
        else:
            print(f"Error creating manufacturer {manu_name}: {e} (No JSON response body)")
        return None
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def create_snipeit_model(model_data):
    """Creates a new model in Snipe-IT."""
    url = f"{SNIPEIT_API_BASE_URL}/models"
    try:
        response = requests.post(url, headers=HEADERS, json=model_data)
        response.raise_for_status()
        print(f"Successfully created model: {model_data['name']}")
        return response.json()['payload']['id']
    except requests.exceptions.RequestException as e:
        if response is not None and response.json():
            response_json = response.json()
            if 'messages' in response_json and 'name' in response_json['messages'] and 'already been taken' in response_json['messages']['name'][0]:
                print(f"Model '{model_data['name']}' with this Manufacturer and Category already exists in Snipe-IT. Skipping creation.")
                return None
            print(f"Error creating model {model_data.get('name', 'N/A')}: {e}")
            print(f"Snipe-IT API response: {response_json}")
        else:
            print(f"Error creating model {model_data.get('name', 'N/A')}: {e} (No JSON response body)")
        return None
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def create_snipeit_asset(asset_data):
    """Creates a new asset in Snipe-IT."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware"
    try:
        response = requests.post(url, headers=HEADERS, json=asset_data)
        response.raise_for_status()
        print(f"Successfully created asset: {asset_data['asset_tag']} - {asset_data.get('name', 'N/A')}")
        return response.json()
    except requests.exceptions.RequestException as e:
        if response is not None and response.json():
            response_json = response.json()
            if 'messages' in response_json and 'asset_tag' in response_json['messages'] and 'already been taken' in response_json['messages']['asset_tag'][0]:
                print(f"Asset with tag '{asset_data['asset_tag']}' already exists in Snipe-IT. Skipping creation.")
                return None
            print(f"Error creating asset {asset_data.get('name', 'N/A')}: {e}")
            print(f"Snipe-IT API response: {response_json}")
        else:
            print(f"Error creating asset {asset_data.get('name', 'N/A')}: {e} (No JSON response body)")
        return None
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def delete_snipeit_asset(asset_id):
    """Deletes an asset from Snipe-IT."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware/{asset_id}"
    try:
        response = requests.delete(url, headers=HEADERS)
        response.raise_for_status()
        print(f"Successfully deleted asset with ID: {asset_id}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error deleting asset with ID {asset_id}: {e}")
        if response is not None and response.json():
            print(f"Snipe-IT API response: {response.json()}")
        return False
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def create_snipeit_user(user_data):
    """Creates a new user in Snipe-IT."""
    url = f"{SNIPEIT_API_BASE_URL}/users"
    response = None
    try:
        response = requests.post(url, headers=HEADERS, json=user_data)
        response.raise_for_status()
        print(f"Successfully created user: {user_data.get('username', 'N/A')} (HTTP Status: {response.status_code})")
        return response.json()
    except requests.exceptions.RequestException as e:
        status_code = response.status_code if response is not None else 'N/A'
        if response is not None and response.json():
            response_json = response.json()
            if 'messages' in response_json:
                if 'username' in response_json['messages'] and 'already been taken' in response_json['messages']['username'][0]:
                    print(f"User with username '{user_data.get('username')}' already exists. Skipping creation (HTTP Status: {status_code}).")
                    return None
                if 'employee_num' in response_json['messages'] and 'already been taken' in response_json['messages']['employee_num'][0]:
                    print(f"User with employee number '{user_data.get('employee_num')}' already exists. Skipping creation (HTTP Status: {status_code}).")
                    return None
                if 'email' in response_json['messages'] and 'already been taken' in response_json['messages']['email'][0]:
                    print(f"User with email '{user_data.get('email')}' already exists (different EmployeeID perhaps). Skipping creation (HTTP Status: {status_code}).")
                    return None
                if 'password' in response_json['messages']:
                    print(f"Password validation error for user '{user_data.get('username')}': {response_json['messages']['password']} (HTTP Status: {status_code}).")
                    return None
            print(f"Error creating user {user_data.get('username', 'N/A')}: {e} (HTTP Status: {status_code})")
            print(f"Snipe-IT API response: {response_json}")
        else:
            print(f"Error creating user {user_data.get('username', 'N/A')}: {e} (No JSON response body / HTTP Status: {status_code})")
        return None
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

    print(f"\n--- Attempting Checkout ---")
    print(f"  Asset ID: {asset_id}")
    print(f"  User ID: {user_id}")
    print(f"  API Endpoint URL: {url}")
    print(f"  Payload being sent: {payload}")

    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response_json = response.json() # Get JSON response even on non-200 to check status key

        if response.status_code == 200 and response_json.get('status') == 'success':
            print(f"Successfully checked out asset ID {asset_id} to user ID {user_id}. HTTP Status: {response.status_code}")
            return True
        else:
            print(f"Error checking out asset ID {asset_id} to user ID {user_id}. HTTP Status: {response.status_code}")
            print(f"Snipe-IT API ERROR response (Checkout - Application Error): {response_json.get('messages', response.text)}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error checking out asset ID {asset_id} to user ID {user_id}: {e}")
        if response is not None:
            print(f"Snipe-IT API response (HTTP Status {response.status_code}): {response.text}")
        else:
            print(f"No response from Snipe-IT API.")
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

    print(f"\n--- Attempting Checkin ---")
    print(f"  Asset ID: {asset_id}")
    print(f"  Target Status Label: '{status_name}' (ID: {status_id})")
    print(f"  API Endpoint URL: {url}")
    print(f"  Payload being sent: {payload}")

    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response_json = response.json() # Get JSON response even on non-200 to check status key

        if response.status_code == 200 and response_json.get('status') == 'success':
            print(f"Successfully checked in asset ID {asset_id}. HTTP Status: {response.status_code}")
            return True
        else:
            print(f"Error checking in asset ID {asset_id}. HTTP Status: {response.status_code}")
            print(f"Snipe-IT API ERROR response (Checkin - Application Error): {response_json.get('messages', response.text)}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error checking in asset ID {asset_id}: {e}")
        if response is not None:
            print(f"Snipe-IT API response (HTTP Status {response.status_code}): {response.text}")
        else:
            print(f"No response from Snipe-IT API.")
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

    print(f"\n--- Attempting to UPDATE Asset ID: {asset_id} Status ---")
    print(f"  Target Status Label: '{status_name}' (ID: {status_id})")
    print(f"  API Endpoint URL: {url}")
    print(f"  Payload being sent: {payload}")

    if not payload:
        print(f"  No status or notes provided for asset ID {asset_id}. Skipping update.")
        return False

    try:
        response = requests.put(url, headers=HEADERS, json=payload)
        response.raise_for_status() # HTTP errors are still caught here
        print(f"Successfully updated asset ID {asset_id} status to {status_id}. HTTP Status: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error updating asset ID {asset_id} status: {e}")
        if response is not None and hasattr(response, 'status_code'):
            print(f"Snipe-IT API ERROR response (Update Status - HTTP Status {response.status_code}): {response.text}")
        else:
            print(f"No response from Snipe-IT API or response object malformed.")
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
        print(f"Warning: Could not parse Last Report Time '{time_str}'. Using minimum date.")
        return datetime.min

def main():
    global SNIPEIT_STATUS_NAMES_BY_ID

    snipeit_manufacturers = {}
    snipeit_categories = {}
    snipeit_models = {}
    snipeit_statuses = {}
    snipeit_locations = {}
    snipeit_companies = {}
    snipeit_assets = {}
    snipeit_users = {}
    directory_data_for_user_lookup = []

    print("--- Collecting Existing Snipe-IT Data (Paginated) ---")

    status_list = get_snipeit_data_paginated('statuslabels')
    if not status_list:
        print("ERROR: No status labels found in Snipe-IT. Cannot proceed with asset management.")
        return
    for status in status_list:
        snipeit_statuses[status['name'].lower()] = status['id']
        SNIPEIT_STATUS_NAMES_BY_ID[status['id']] = status['name']
    print(f"Total {len(snipeit_statuses)} status labels collected from Snipe-IT API.")

    default_status_id = snipeit_statuses.get(DEFAULT_STATUS_LABEL.lower())
    checked_out_status_id = snipeit_statuses.get(CHECKED_OUT_STATUS_LABEL.lower())
    checkin_status_id = snipeit_statuses.get(DEFAULT_STATUS_LABEL.lower()) # This will be the ID for 'Ready to Deploy'

    if not default_status_id:
        print(f"Error: Default Status Label '{DEFAULT_STATUS_LABEL}' not found in Snipe-IT. Cannot proceed.")
        return
    if not checked_out_status_id:
        print(f"Error: Checked Out Status Label '{CHECKED_OUT_STATUS_LABEL}' not found in Snipe-IT. Cannot proceed.")
        return
    if not checkin_status_id:
        print(f"Error: Check-in Status Label '{DEFAULT_STATUS_LABEL}' not found in Snipe-IT. Cannot proceed.")
        return

    print(f"Identified Default Status ID: {default_status_id} ('{DEFAULT_STATUS_LABEL}')")
    print(f"Identified Checked Out Status ID: {checked_out_status_id} ('{CHECKED_OUT_STATUS_LABEL}')")
    print(f"Identified Check-in Status ID: {checkin_status_id} ('{DEFAULT_STATUS_LABEL}')")

    manu_list = get_snipeit_data_paginated('manufacturers')
    for manu in manu_list:
        snipeit_manufacturers[manu['name'].lower()] = manu['id']
    print(f"Total {len(snipeit_manufacturers)} manufacturers collected from Snipe-IT.")

    cat_list = get_snipeit_data_paginated('categories')
    for cat in cat_list:
        snipeit_categories[cat['name'].lower()] = cat['id']
    print(f"Total {len(snipeit_categories)} categories collected from Snipe-IT.")

    location_list = get_snipeit_data_paginated('locations')
    for loc in location_list:
        snipeit_locations[loc['name'].lower()] = loc['id']
    print(f"Total {len(snipeit_locations)} locations collected from Snipe-IT.")

    company_list = get_snipeit_data_paginated('companies')
    for company in company_list:
        snipeit_companies[company['name'].lower()] = company['id']
    print(f"Total {len(snipeit_companies)} companies collected from Snipe-IT.")

    print("Collecting existing models from Snipe-IT (paginated)...")
    existing_models_raw = get_snipeit_data_paginated('models')
    for model in existing_models_raw:
        model_name_clean = model['name'].strip().lower()
        manufacturer_id = model['manufacturer']['id'] if model.get('manufacturer') else None
        category_id = model['category']['id'] if model.get('category') else None
        if manufacturer_id and category_id:
            snipeit_models[(model_name_clean, manufacturer_id, category_id)] = model['id']
    print(f"Total {len(snipeit_models)} existing models collected from Snipe-IT.")

    print("Collecting existing assets from Snipe-IT (paginated)...")
    existing_assets_raw = get_snipeit_data_paginated('hardware')
    for asset in existing_assets_raw:
        asset_tag = asset.get('asset_tag')
        asset_id = asset.get('id')
        asset_name = asset.get('name')
        notes = asset.get('notes', '')
        checked_out_to_id = None
        if asset.get('assigned_to') and asset['assigned_to']['type'] == 'user':
            checked_out_to_id = asset['assigned_to']['id']

        if asset_tag and asset_id:
            last_report_time_from_notes = datetime.min
            notes_match = [line for line in notes.split('\n') if 'BigFix Last Report:' in line]
            if notes_match:
                try:
                    time_str = notes_match[0].split('BigFix Last Report:')[1].strip()
                    last_report_time_from_notes = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                except ValueError as e:
                    print(f"Warning: Could not parse Last Report Time from notes for asset {asset_tag}: '{notes_match[0]}'. Error: {e}")

            snipeit_assets[asset_tag.upper()] = {
                'id': asset_id,
                'name': asset_name.strip().lower() if asset_name else '',
                'last_report_time': last_report_time_from_notes,
                'checked_out_to_id': checked_out_to_id
            }
    print(f"Total {len(snipeit_assets)} existing assets collected from Snipe-IT.")

    print("Collecting existing users from Snipe-IT (paginated)...")
    for user in get_snipeit_data_paginated('users'):
        employee_num = user.get('employee_num')
        username = user.get('username')
        user_id = user.get('id')

        email = user.get('email', '') or ''
        first_name = user.get('first_name', '') or ''
        last_name = user.get('last_name', '') or ''

        if employee_num:
            snipeit_users[employee_num.strip()] = {
                'id': user_id,
                'username': (username.strip().lower() if username else None),
                'email': email.strip().lower(),
                'first_name': first_name.strip().lower(),
                'last_name': last_name.lower(),
            }
        elif username:
             snipeit_users[username.strip().lower()] = {
                'id': user_id,
                'username': username.strip().lower(),
                'email': email.strip().lower(),
                'first_name': first_name.strip().lower(),
                'last_name': last_name.lower(),
            }
    print(f"Total {len(snipeit_users)} existing users collected from Snipe-IT.")

    print(f"\n--- Processing BigFix CSV: {BIGFIX_CSV_FILE} for Assets ---")
    bigfix_data = {}
    skipped_serial_count = 0
    try:
        with open(BIGFIX_CSV_FILE, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            required_cols = [MODEL_COLUMN, MANUFACTURER_COLUMN, CATEGORY_COLUMN, SERIAL_COLUMN, COMPUTER_NAME_COLUMN, LAST_REPORT_TIME_COLUMN, BIGFIX_USERNAME_COLUMN]
            missing_cols = [col for col in required_cols if col not in reader.fieldnames]

            if missing_cols:
                print(f"Error: Missing required columns in CSV: {', '.join(missing_cols)}")
                print(f"Available columns in CSV: {', '.join(reader.fieldnames)}")
                return

            for row_num, row in enumerate(reader, start=2):
                serial = row.get(SERIAL_COLUMN)
                last_report_time_str = row.get(LAST_REPORT_TIME_COLUMN)
                computer_name_from_csv = row.get(COMPUTER_NAME_COLUMN, '')

                if not serial or not serial.strip():
                    print(f"Skipping row {row_num}: Missing serial number for asset '{computer_name_from_csv}'.")
                    skipped_serial_count += 1
                    continue

                serial_upper = serial.strip().upper()

                if serial_upper in [s.upper() for s in SERIAL_SKIP_LIST]:
                    print(f"Skipping row {row_num}: Serial '{serial_upper}' is in the skip list.")
                    skipped_serial_count += 1
                    continue

                if not last_report_time_str:
                    print(f"Skipping row {row_num}: Missing 'Last Report Time' for asset '{computer_name_from_csv}' (Serial: {serial}).")
                    skipped_serial_count += 1
                    continue

                parsed_time = parse_last_report_time(last_report_time_str)

                if serial_upper not in bigfix_data or parsed_time > bigfix_data[serial_upper]['parsed_last_report_time']:
                    bigfix_data[serial_upper] = {
                        'row': row,
                        'parsed_last_report_time': parsed_time
                    }
        print(f"Found {len(bigfix_data)} unique (by serial, latest report time) devices in the BigFix CSV after filtering.")
        if skipped_serial_count > 0:
            print(f"Skipped {skipped_serial_count} BigFix entries due to missing or invalid serials/times, or serials in the skip list.")

    except FileNotFoundError:
        print(f"Error: CSV file '{BIGFIX_CSV_FILE}' not found. Skipping asset import.")
        bigfix_data = {}
    except Exception as e:
        print(f"An error occurred while reading the BigFix CSV file: {e}")
        return

    print("\n--- Populating Manufacturers in Snipe-IT (Asset-related) ---")
    manufacturers_added_count = 0
    bigfix_unique_manufacturers_from_csv = set(d['row'].get(MANUFACTURER_COLUMN).strip() for d in bigfix_data.values() if d['row'].get(MANUFACTURER_COLUMN))

    for manu_name in sorted(list(bigfix_unique_manufacturers_from_csv)):
        if manu_name.lower() not in snipeit_manufacturers:
            print(f"Manufacturer '{manu_name}' not found in Snipe-IT. Attempting to create...")
            new_id = create_snipeit_manufacturer(manu_name)
            if new_id:
                manufacturers_added_count += 1
                snipeit_manufacturers[manu_name.lower()] = new_id
    print(f"Successfully added {manufacturers_added_count} new manufacturers to Snipe-IT.")

    print("\n--- Populating Models in Snipe-IT (Asset-related) ---")
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
                print(f"Warning: Manufacturer '{manufacturer_name}' for model '{model_name}' not found/created. Skipping model collection.")
                continue
            if not category_id_for_model:
                print(f"Warning: Category '{category_name}' for model '{model_name}' not found. Using default '{DEFAULT_CATEGORY_NAME}'.")
                category_id_for_model = snipeit_categories.get(DEFAULT_CATEGORY_NAME.lower())
                if not category_id_for_model:
                    print(f"Warning: Default category '{DEFAULT_CATEGORY_NAME}' not found. Skipping model collection.")
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

    print(f"Successfully added {models_added_count} new models to Snipe-IT.")

    print(f"\n--- Processing Directory Export CSV: {DIRECTORY_CSV_FILE} for Users ---")
    users_added_count = 0
    users_skipped_count = 0

    try:
        with open(DIRECTORY_CSV_FILE, mode='r', encoding='utf-8') as csvfile:
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
                print(f"Error: Missing required user columns in '{DIRECTORY_CSV_FILE}': {', '.join(user_missing_cols)}")
                print(f"Available columns in CSV: {', '.join(reader.fieldnames)}")
                return

            for row_num, row in enumerate(reader, start=2):
                employee_id = row.get(USER_EMPLOYEE_ID_COLUMN)
                employee_net_id = row.get(USER_EMPLOYEE_NET_ID_COLUMN)
                first_name = row.get(USER_FIRST_NAME_COLUMN)
                last_name = row.get(USER_LAST_NAME_COLUMN)
                email = row.get(USER_EMAIL_COLUMN)

                if not all([employee_id, employee_net_id, first_name, last_name, email]):
                    print(f"Skipping user in row {row_num}: Missing required user data (EmployeeID, EmployeeNetId, FirstName, LastName, EmployeeEmailAddress).")
                    users_skipped_count += 1
                    continue
                
                employee_id = employee_id.strip()
                employee_net_id = employee_net_id.strip()
                first_name = first_name.strip()
                last_name = last_name.strip()
                email = email.strip()

                directory_data_for_user_lookup.append(row)

                if employee_id in snipeit_users:
                    print(f"User with EmployeeID '{employee_id}' (NetID: {employee_net_id}) already exists in Snipe-IT. Skipping creation.")
                    users_skipped_count += 1
                    continue
                
                is_duplicate_by_username = False
                is_duplicate_by_email = False
                for existing_user_data in snipeit_users.values():
                    if existing_user_data['username'] and existing_user_data['username'] == employee_net_id.lower():
                        is_duplicate_by_username = True
                        break
                    if existing_user_data['email'] and existing_user_data['email'] == email.lower():
                        is_duplicate_by_email = True
                        break
                
                if is_duplicate_by_username:
                    print(f"User with username '{employee_net_id}' already exists (different EmployeeID perhaps). Skipping creation.")
                    users_skipped_count += 1
                    continue
                if is_duplicate_by_email:
                    print(f"User with email '{email}' already exists (different EmployeeID perhaps). Skipping creation.")
                    users_skipped_count += 1
                    continue

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
                
                print(f"DEBUG: Attempting to create user: {employee_net_id} (ID: {employee_id})")
                created_user_response = create_snipeit_user(user_payload)
                if created_user_response:
                    users_added_count += 1
                    new_user_id = created_user_response['payload']['id']
                    snipeit_users[employee_id] = {
                        'id': new_user_id,
                        'username': employee_net_id.lower(),
                        'email': email.lower(),
                        'first_name': first_name.lower(),
                        'last_name': last_name.lower(),
                    }

    except FileNotFoundError:
        print(f"Error: Directory export CSV file '{DIRECTORY_CSV_FILE}' not found. Skipping user import.")
    except Exception as e:
        print(f"An error occurred while reading the directory CSV file: {e}")

    print(f"\n--- Populating Assets in Snipe-IT ---")
    assets_added_count = 0
    assets_deleted_count = 0 # This counter is not used in the current script logic
    assets_skipped_final_count = 0

    default_location_id = snipeit_locations.get(DEFAULT_LOCATION_NAME.lower())
    target_company_id = snipeit_companies.get(TARGET_COMPANY_NAME.lower())

    if not default_location_id:
        print(f"Error: Default Location '{DEFAULT_LOCATION_NAME}' not found in Snipe-IT. Cannot create assets. Please create it.")
        return
    if not target_company_id:
        print(f"Error: Target Company '{TARGET_COMPANY_NAME}' not found in Snipe-IT. Cannot create assets. Please ensure it exists.")
        return

    for serial_upper, device_data in bigfix_data.items():
        row = device_data['row']
        bigfix_last_report_time = device_data['parsed_last_report_time']

        serial = row.get(SERIAL_COLUMN).strip()
        computer_name_from_csv = row.get(COMPUTER_NAME_COLUMN)
        computer_name_for_asset = computer_name_from_csv.strip() if computer_name_from_csv and computer_name_from_csv.strip() else serial

        model_name = row.get(MODEL_COLUMN).strip()
        manufacturer_name = row.get(MANUFACTURER_COLUMN).strip()
        category_name = row.get(CATEGORY_COLUMN, DEFAULT_CATEGORY_NAME).strip()

        notes = f"BigFix Last Report: {bigfix_last_report_time.strftime('%Y-%m-%d %H:%M:%S')}"

        manufacturer_id = snipeit_manufacturers.get(manufacturer_name.lower())
        category_id = snipeit_categories.get(category_name.lower())

        model_lookup_key = (model_name.lower(), manufacturer_id, category_id)
        model_id = snipeit_models.get(model_lookup_key)

        if not manufacturer_id or not category_id or not model_id:
            print(f"Skipping asset '{computer_name_for_asset}' (Serial: {serial}): Required Manufacturer ID, Category ID, or Model ID not found/created.")
            assets_skipped_final_count += 1
            continue

        existing_snipeit_asset = snipeit_assets.get(serial_upper)

        asset_id_for_workflow = None

        if existing_snipeit_asset:
            print(f"Asset '{computer_name_for_asset}' (Serial: {serial}) already exists in Snipe-IT with ID {existing_snipeit_asset['id']}. Using existing asset.")
            asset_id_for_workflow = existing_snipeit_asset['id']
            # Update last report time if newer from BigFix
            if bigfix_last_report_time > existing_snipeit_asset['last_report_time']:
                print(f"Updating last report time for existing asset {serial_upper} (ID: {asset_id_for_workflow}).")
                # Need to explicitly update notes to reflect this if it's the only change
                current_notes_on_snipeit = get_asset_details_from_snipeit_raw_notes(asset_id_for_workflow)
                updated_notes = update_bigfix_last_report_in_notes(current_notes_on_snipeit, bigfix_last_report_time)
                if updated_notes != current_notes_on_snipeit:
                    update_asset_status(asset_id_for_workflow, None, notes=updated_notes) # Pass None for status_id if only updating notes
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
            print(f"DEBUG: Attempting to create asset with Tag: '{asset_payload['asset_tag']}', Name: '{asset_payload['name']}', Company ID: '{asset_payload['company_id']}'")

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
                print(f"Skipping user association for asset {serial} as it was not created successfully.")
                assets_skipped_final_count += 1
                continue # Skip to next asset if asset creation failed

        # --- Scenario One: Checkout Logic Only ---
        bigfix_user_name = row.get(BIGFIX_USERNAME_COLUMN, '').strip()

        if asset_id_for_workflow and bigfix_user_name: # Only proceed if asset exists AND BigFix provides a user name
            current_snipeit_status, current_snipeit_assigned_to_id = get_asset_details_from_snipeit(asset_id_for_workflow)

            # Debugging prints as discussed
            print(f"\nDEBUG: Asset {serial} (ID: {asset_id_for_workflow}) Initial Snipe-IT State Check:")
            print(f"  Current Status ID: {current_snipeit_status} ('{SNIPEIT_STATUS_NAMES_BY_ID.get(current_snipeit_status, 'Unknown')}')")
            print(f"  Current Assigned To ID: {current_snipeit_assigned_to_id if current_snipeit_assigned_to_id else 'None'}")
            print(f"  Expected Checkin Status ID: {checkin_status_id} ('{DEFAULT_STATUS_LABEL}')")
            print(f"  Expected Checked Out Status ID: {checked_out_status_id} ('{CHECKED_OUT_STATUS_LABEL}')")

            found_user_in_directory_csv = None
            for dir_row in directory_data_for_user_lookup:
                if dir_row.get(USER_EMPLOYEE_NET_ID_COLUMN, '').strip().lower() == bigfix_user_name.lower():
                    found_user_in_directory_csv = dir_row
                    break
            
            if found_user_in_directory_csv:
                target_employee_id = found_user_in_directory_csv.get(USER_EMPLOYEE_ID_COLUMN, '').strip()
                snipeit_target_user_info = None
                
                if target_employee_id in snipeit_users:
                    snipeit_target_user_info = snipeit_users[target_employee_id]
                else: 
                    for user_details in snipeit_users.values():
                        if user_details['username'] and user_details['username'] == bigfix_user_name.lower():
                            snipeit_target_user_info = user_details
                            break

                if snipeit_target_user_info:
                    snipeit_user_id = snipeit_target_user_info['id']

                    if current_snipeit_assigned_to_id == snipeit_user_id and current_snipeit_status == checked_out_status_id:
                        print(f"Asset ID {asset_id_for_workflow} (Serial: {serial}) is already checked out to the correct user ID {snipeit_user_id} and status {SNIPEIT_STATUS_NAMES_BY_ID.get(checked_out_status_id)}. Skipping checkout.")
                    else:
                        print(f"\n--- Workflow for Asset ID {asset_id_for_workflow} (Serial: {serial}) - Preparing for Checkout ---")
                        print(f"  Current Snipe-IT State: Status ID {current_snipeit_status}, Assigned To: {current_snipeit_assigned_to_id}")
                        
                        # If the asset is NOT in the "Ready to Deploy" status, attempt to directly update its status.
                        if current_snipeit_status != checkin_status_id: # checkin_status_id is 'Ready to Deploy' (ID 2)
                            print(f"  Asset is NOT in '{SNIPEIT_STATUS_NAMES_BY_ID.get(checkin_status_id)}' state. Attempting to directly update status to '{DEFAULT_STATUS_LABEL}'.")
                            
                            update_notes_for_status_change = (
                                f"Automatic pre-checkout status update based on BigFix data. "
                                f"Current assignment: {current_snipeit_assigned_to_id if current_snipeit_assigned_to_id else 'None'}. "
                                f"Current status: {SNIPEIT_STATUS_NAMES_BY_ID.get(current_snipeit_status, current_snipeit_status)}. "
                                f"Last BigFix Report: {bigfix_last_report_time.strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                            
                            # Use update_asset_status to directly set the status_id
                            if update_asset_status(asset_id_for_workflow, checkin_status_id, notes=update_notes_for_status_change):
                                print(f"  Successfully attempted direct status update for asset ID {asset_id_for_workflow} to {SNIPEIT_STATUS_NAMES_BY_ID.get(checkin_status_id)}.")
                                # Re-fetch to confirm state after update
                                current_snipeit_status, current_snipeit_assigned_to_id = get_asset_details_from_snipeit(asset_id_for_workflow)
                                if current_snipeit_status != checkin_status_id or current_snipeit_assigned_to_id is not None:
                                    print(f"  ❌ Post-status-update state verification failed. Asset is not fully in '{SNIPEIT_STATUS_NAMES_BY_ID.get(checkin_status_id)}' state.")
                                    print(f"     Actual: Status {current_snipeit_status}, Assigned {current_snipeit_assigned_to_id}.")
                                    print(f"  Skipping checkout for asset {serial} due to inconsistent state after status update.")
                                    continue # Skip to next asset if status update didn't result in expected state
                            else:
                                print(f"  Failed to get asset {serial} into '{DEFAULT_STATUS_LABEL}' state via direct update. Skipping checkout.")
                                continue # Skip to next asset if update API call failed
                        else:
                            print(f"  Asset is already in '{SNIPEIT_STATUS_NAMES_BY_ID.get(checkin_status_id)}' state and ready for checkout.")

                        # Step 2: Perform the checkout (This part remains the same)
                        print(f"  Attempting to check out asset ID {asset_id_for_workflow} to user ID {snipeit_user_id}.")
                        checkout_notes = f"Automatically checked out based on BigFix 'User Name': {bigfix_user_name}. Last BigFix Report: {bigfix_last_report_time.strftime('%Y-%m-%d %H:%M:%S')}"
                        if checkout_asset_to_user(asset_id_for_workflow, snipeit_user_id, checkout_notes):
                            print(f"  Successfully initiated checkout for asset {asset_id_for_workflow}.")
                            # Update local cache immediately
                            snipeit_assets[serial_upper]['checked_out_to_id'] = snipeit_user_id
                            # Re-fetch to confirm Snipe-IT's new status and assignment
                            current_snipeit_status, current_snipeit_assigned_to_id = get_asset_details_from_snipeit(asset_id_for_workflow)
                            if current_snipeit_status == checked_out_status_id and current_snipeit_assigned_to_id == snipeit_user_id:
                                print(f"  ✅ Checkout confirmed: Asset is now {SNIPEIT_STATUS_NAMES_BY_ID.get(checked_out_status_id)} and assigned to {snipeit_user_id}.")
                            else:
                                print(f"  ⚠️ Checkout verification failed. Expected Status {SNIPEIT_STATUS_NAMES_BY_ID.get(checked_out_status_id)} (ID {checked_out_status_id}) and assigned to User ID {snipeit_user_id}.")
                                print(f"     Actual: Status {SNIPEIT_STATUS_NAMES_BY_ID.get(current_snipeit_status)} (ID {current_snipeit_status}), Assigned To: {current_snipeit_assigned_to_id if current_snipeit_assigned_to_id else 'Nobody'}")
                                print(f"     Attempting to force status update to '{CHECKED_OUT_STATUS_LABEL}'.")
                                update_notes = f"Forced status update after BigFix-driven checkout. Last BigFix Report: {bigfix_last_report_time.strftime('%Y-%m-%d %H:%M:%S')}"
                                if update_asset_status(asset_id_for_workflow, checked_out_status_id, update_notes):
                                    print(f"  Successfully forced status to '{CHECKED_OUT_STATUS_LABEL}'.")
                                else:
                                    print(f"  Failed to force status to '{CHECKED_OUT_STATUS_LABEL}'. Manual intervention may be needed.")
                        else:
                            print(f"  Failed to check out asset {asset_id_for_workflow} to user {snipeit_user_id}.")
                            snipeit_assets[serial_upper]['checked_out_to_id'] = current_snipeit_assigned_to_id
                else:
                    print(f"Could not find Snipe-IT user for BigFix 'User Name' '{bigfix_user_name}' (EmployeeID: {target_employee_id}). Skipping checkout.")
            else:
                print(f"BigFix 'User Name' '{bigfix_user_name}' not found in Directory CSV 'EmployeeNetId'. Skipping checkout.")
        elif not bigfix_user_name:
            print(f"BigFix 'User Name' column is empty for asset '{serial}'. Skipping user association (Scenario One only).")
        else:
            print(f"Asset ID for workflow is missing for {serial}. Skipping user association.")


    print(f"\n--- Overall Summary ---")
    print(f"Asset Import:")
    print(f"  New Manufacturers Added: {manufacturers_added_count}")
    print(f"  New Models Added: {models_added_count}")
    print(f"  New Assets Added: {assets_added_count}")
    print(f"  Old Assets Deleted (Repurposed): {assets_deleted_count}")
    print(f"  Assets Skipped (Initial CSV Filter/Later Checks): {skipped_serial_count + assets_skipped_final_count}")
    print(f"\nUser Import:")
    print(f"  New Users Added: {users_added_count}")
    print(f"  Users Skipped: {users_skipped_count}")


def get_asset_details_from_snipeit_raw_notes(asset_id):
    """Helper to get asset details for notes field specifically."""
    url = f"{SNIPEIT_API_BASE_URL}/hardware/{asset_id}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        asset_data = response.json()
        return asset_data.get('notes', '')
    except requests.exceptions.RequestException as e:
        print(f"Error fetching raw notes for asset ID {asset_id}: {e}")
        return ''
    finally:
        time.sleep(REQUEST_DELAY_SECONDS)

def update_bigfix_last_report_in_notes(current_notes, new_report_time):
    """Updates or adds the BigFix Last Report line in the notes."""
    report_line_prefix = "BigFix Last Report:"
    new_report_line = f"{report_line_prefix} {new_report_time.strftime('%Y-%m-%d %H:%M:%S')}"
    
    lines = current_notes.split('\n')
    updated_lines = []
    found_and_updated = False

    for line in lines:
        if report_line_prefix in line:
            updated_lines.append(new_report_line)
            found_and_updated = True
        else:
            updated_lines.append(line)
    
    if not found_and_updated:
        updated_lines.append(new_report_line) # Add it if not found
    
    return "\n".join(filter(None, updated_lines)) # Filter out empty lines


if __name__ == "__main__":
    main()