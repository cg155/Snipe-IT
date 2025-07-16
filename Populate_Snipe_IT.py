import csv
import requests
import os
import time
from dotenv import load_dotenv
from datetime import datetime

# --- Configuration ---
load_dotenv() # This loads variables from .env into os.environ
SNIPEIT_API_BASE_URL = os.environ.get('SNIPEIT_API_BASE_URL', '')
SNIPEIT_API_TOKEN = os.environ.get('SNIPEIT_API_TOKEN', '')

if not SNIPEIT_API_TOKEN:
    print("Warning: SNIPEIT_API_TOKEN environment variable not set.")
    # You might want to exit or raise an error here if the token is critical
else:
    print("SNIPEIT_API_TOKEN successfully loaded.")
    # Proceed with using SNIPEIT_API_TOKEN

BIGFIX_CSV_FILE = 'test.csv'

# CSV Column Mappings (These must EXACTLY match the headers in your BigFix CSV)
MODEL_COLUMN = 'Model'
MANUFACTURER_COLUMN = 'Manufacturer'
CATEGORY_COLUMN = 'Device Type'
SERIAL_COLUMN = 'Serial' # Used as Asset Tag in Snipe-IT
COMPUTER_NAME_COLUMN = 'Computer Name' # Used as Asset Name in Snipe-IT
LAST_REPORT_TIME_COLUMN = 'Last Report Time' # Used for de-duplication and notes

# Default Snipe-IT Values (ensure these exist in your Snipe-IT instance)
DEFAULT_CATEGORY_NAME = 'Desktop' # Updated to "Desktop"
DEFAULT_STATUS_LABEL = 'Deployed' # e.g., 'Deployed', 'Ready to Deploy', 'In Repair'
DEFAULT_LOCATION_NAME = 'Office'
TARGET_COMPANY_NAME = 'NYU - Tandon School of Engineering' # Target company for all assets

# --- API Rate Limiting ---
REQUEST_DELAY_SECONDS = 0.6
MAX_API_LIMIT_PER_REQUEST = 500 # Correct variable name

# --- Serial Number Skip List ---
SERIAL_SKIP_LIST = ["0123456789", "To be filled by O.E.M.", "System Serial Number"]

# --- API Headers ---
HEADERS = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {SNIPEIT_API_TOKEN}',
    'Content-Type': 'application/json',
}

# --- API Helper Functions ---

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

            # Corrected variable name here
            if not rows or len(rows) < MAX_API_LIMIT_PER_REQUEST:
                break

            # Corrected variable name here
            offset += MAX_API_LIMIT_PER_REQUEST

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {endpoint} (offset: {offset}): {e}")
            break
    return all_items

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

# --- Main Script Logic ---

def main():
    # --- Step 1: Collect Current Snipe-IT Data (with pagination) ---
    snipeit_manufacturers = {}
    snipeit_categories = {}
    snipeit_models = {}
    snipeit_statuses = {}
    snipeit_locations = {}
    snipeit_companies = {}
    snipeit_assets = {}

    print("--- Collecting Existing Snipe-IT Data (Paginated) ---")
    manu_list = get_snipeit_data_paginated('manufacturers')
    for manu in manu_list:
        snipeit_manufacturers[manu['name'].lower()] = manu['id']
    print(f"Total {len(snipeit_manufacturers)} manufacturers collected from Snipe-IT.")

    cat_list = get_snipeit_data_paginated('categories')
    for cat in cat_list:
        snipeit_categories[cat['name'].lower()] = cat['id']
    print(f"Total {len(snipeit_categories)} categories collected from Snipe-IT.")

    status_list = get_snipeit_data_paginated('statuslabels')
    for status in status_list:
        snipeit_statuses[status['name'].lower()] = status['id']
    print(f"Total {len(snipeit_statuses)} status labels collected from Snipe-IT.")

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
                'last_report_time': last_report_time_from_notes
            }
    print(f"Total {len(snipeit_assets)} existing assets collected from Snipe-IT.")

    # --- Step 2: Parse BigFix CSV, Filter, and De-duplicate ---
    bigfix_data = {}
    skipped_serial_count = 0

    print(f"\n--- Parsing and De-duplicating BigFix CSV: {BIGFIX_CSV_FILE} ---")
    try:
        with open(BIGFIX_CSV_FILE, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            required_cols = [MODEL_COLUMN, MANUFACTURER_COLUMN, CATEGORY_COLUMN, SERIAL_COLUMN, COMPUTER_NAME_COLUMN, LAST_REPORT_TIME_COLUMN]
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
        print(f"Error: CSV file '{BIGFIX_CSV_FILE}' not found. Please ensure the file is in the same directory or provide the full path.")
        return
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return

    # --- Step 3: Populate Manufacturers in Snipe-IT ---
    print("\n--- Populating Manufacturers in Snipe-IT ---")
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

    # --- Step 4: Populate Models in Snipe-IT ---
    print("\n--- Populating Models in Snipe-IT ---")
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

    # --- Step 5: Populate Assets in Snipe-IT ---
    print("\n--- Populating Assets in Snipe-IT ---")
    assets_added_count = 0
    assets_deleted_count = 0
    assets_skipped_final_count = 0

    default_status_id = snipeit_statuses.get(DEFAULT_STATUS_LABEL.lower())
    default_location_id = snipeit_locations.get(DEFAULT_LOCATION_NAME.lower())
    target_company_id = snipeit_companies.get(TARGET_COMPANY_NAME.lower())

    if not default_status_id:
        print(f"Error: Default Status Label '{DEFAULT_STATUS_LABEL}' not found in Snipe-IT. Cannot create assets. Please create it.")
        return
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

        if existing_snipeit_asset:
            if existing_snipeit_asset['name'] != computer_name_for_asset.lower():
                print(f"Asset '{computer_name_for_asset}' (Serial: {serial}) matches existing Snipe-IT asset '{existing_snipeit_asset['name']}' by serial, but names differ.")
                if bigfix_last_report_time > existing_snipeit_asset['last_report_time']:
                    print(f"  BigFix L.R.T. ({bigfix_last_report_time}) is more recent than Snipe-IT L.R.T. ({existing_snipeit_asset['last_report_time']}). Deleting old asset.")
                    if delete_snipeit_asset(existing_snipeit_asset['id']):
                        assets_deleted_count += 1
                        print(f"  Proceeding to add new asset for {serial}.")
                        del snipeit_assets[serial_upper]
                    else:
                        print(f"  Failed to delete old asset {existing_snipeit_asset['id']}. Skipping new asset creation for {serial}.")
                        assets_skipped_final_count += 1
                        continue
                else:
                    print(f"  BigFix L.R.T. ({bigfix_last_report_time}) is NOT more recent than Snipe-IT L.R.T. ({existing_snipeit_asset['last_report_time']}). Skipping new asset creation.")
                    assets_skipped_final_count += 1
                    continue
            else:
                assets_skipped_final_count += 1
                continue

        asset_payload = {
            'asset_tag': serial,
            'name': computer_name_for_asset, # Corrected key for asset name
            'model_id': model_id,
            'status_id': default_status_id,
            'location_id': default_location_id,
            'company_id': target_company_id,
            'serial': serial,
            'notes': notes
        }
        print(f"DEBUG: Attempting to create asset with Tag: '{asset_payload['asset_tag']}', Name: '{asset_payload['name']}', Company ID: '{asset_payload['company_id']}'")

        if create_snipeit_asset(asset_payload):
            assets_added_count += 1
            snipeit_assets[serial_upper] = {
                'id': None,
                'name': computer_name_for_asset.lower(),
                'last_report_time': bigfix_last_report_time
            }

    print(f"\n--- Script Finished ---")
    print(f"Summary: ")
    print(f"  New Manufacturers Added: {manufacturers_added_count}")
    print(f"  New Models Added: {models_added_count}")
    print(f"  New Assets Added: {assets_added_count}")
    print(f"  Old Assets Deleted (Repurposed): {assets_deleted_count}")
    print(f"  Assets Skipped (Initial CSV Filter/Later Checks): {skipped_serial_count + assets_skipped_final_count}")


if __name__ == "__main__":
    main()