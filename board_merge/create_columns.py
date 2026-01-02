#!/usr/bin/env python3
"""
Create New Columns in Target Board

Creates the following columns in the Bewerberliste (target board):
- Familienstand (dropdown)
- Kinder (numbers)
- Geburtsland (dropdown)
"""

import os
import sys
import json
import time
from dotenv import load_dotenv
from export_boards import MondayAPIClient

# Load environment variables
load_dotenv()

TARGET_BOARD_ID = "3567618324"

# Familienstand options
FAMILIENSTAND_OPTIONS = [
    "ledig",
    "verheiratet",
    "getrennt lebend",
    "geschieden"
]

# Geburtsland options (same as Nationalität)
GEBURTSLAND_OPTIONS = [
    "Afghanistan",
    "Albanien",
    "Algerien",
    "Angola",
    "Argentinien",
    "Armenien",
    "Aserbaidschan",
    "Belgien",
    "Benin",
    "Bosnien und Herzegowina",
    "Brasilien",
    "Bulgarien",
    "China",
    "Deutschland",
    "Dominikanische Republik",
    "Ecuador",
    "Eritrea",
    "Estland",
    "Frankreich",
    "Georgien",
    "Ghana",
    "Griechenland",
    "Großbritannien",
    "Indien",
    "Irak",
    "Iran",
    "Irland",
    "Israel",
    "Italien",
    "Japan",
    "Jemen",
    "Jordanien",
    "Kamerun",
    "Kapverden",
    "Kasachstan",
    "Kolumbien",
    "Kongo",
    "Kosovo",
    "Kroatien",
    "Kuba",
    "Lettland",
    "Litauen",
    "Mazedonien",
    "Marokko",
    "Mexiko",
    "Mongolei",
    "Neuseeland",
    "Nicaragua",
    "Niederlande",
    "Nigeria",
    "Norwegen",
    "Pakistan",
    "Polen",
    "Portugal",
    "Rumänien",
    "Russland",
    "Schweden",
    "Schweiz",
    "Senegal",
    "Serbien",
    "Slowakei",
    "Slowenien",
    "Somalia",
    "Spanien",
    "Sudan",
    "Syrien",
    "Südafrika",
    "Taiwan",
    "Thailand",
    "Togo",
    "Tschechien",
    "Tunesien",
    "Türkei",
    "USA",
    "Ukraine",
    "Ungarn",
    "Usbekistan",
    "Weißrussland",
    "Zypern",
    "Ägypten",
    "Äthiopien",
    "Österreich"
]


def create_dropdown_column(client: MondayAPIClient, board_id: str, title: str, labels: list) -> dict:
    """Create a dropdown column with specified labels."""
    
    # First create the column
    mutation = """
    mutation CreateColumn($boardId: ID!, $title: String!, $columnType: ColumnType!) {
        create_column(board_id: $boardId, title: $title, column_type: $columnType) {
            id
            title
        }
    }
    """
    
    variables = {
        "boardId": board_id,
        "title": title,
        "columnType": "dropdown"
    }
    
    result = client.execute_query(mutation, variables)
    column_info = result.get("create_column", {})
    column_id = column_info.get("id")
    
    if not column_id:
        raise Exception(f"Failed to create column {title}")
    
    print(f"Created column '{title}' with ID: {column_id}")
    
    # Now add labels to the dropdown
    # We need to use change_column_metadata or update settings
    # Monday.com API requires setting labels via column settings
    
    # Build labels structure
    labels_list = [{"id": i+1, "name": label} for i, label in enumerate(labels)]
    settings = {"labels": labels_list}
    
    update_mutation = """
    mutation ChangeColumnMetadata($boardId: ID!, $columnId: String!, $columnProperty: ColumnProperty!, $value: String!) {
        change_column_metadata(board_id: $boardId, column_id: $columnId, column_property: $columnProperty, value: $value) {
            id
        }
    }
    """
    
    # Update column settings with labels
    settings_str = json.dumps(settings)
    
    try:
        client.execute_query(update_mutation, {
            "boardId": board_id,
            "columnId": column_id,
            "columnProperty": "settings_str",
            "value": settings_str
        })
        print(f"  Added {len(labels)} labels to dropdown")
    except Exception as e:
        print(f"  Warning: Could not add labels via API: {e}")
        print(f"  Labels may need to be added manually in Monday.com UI")
    
    time.sleep(0.5)  # Rate limit protection
    return column_info


def create_number_column(client: MondayAPIClient, board_id: str, title: str) -> dict:
    """Create a numbers column."""
    
    mutation = """
    mutation CreateColumn($boardId: ID!, $title: String!, $columnType: ColumnType!) {
        create_column(board_id: $boardId, title: $title, column_type: $columnType) {
            id
            title
        }
    }
    """
    
    variables = {
        "boardId": board_id,
        "title": title,
        "columnType": "numbers"
    }
    
    result = client.execute_query(mutation, variables)
    column_info = result.get("create_column", {})
    column_id = column_info.get("id")
    
    if not column_id:
        raise Exception(f"Failed to create column {title}")
    
    print(f"Created column '{title}' with ID: {column_id}")
    
    time.sleep(0.5)  # Rate limit protection
    return column_info


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Create new columns in target board")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created without creating")
    
    args = parser.parse_args()
    
    # Load API token
    api_token = os.getenv("MONDAY_API_TOKEN")
    if not api_token:
        print("Error: MONDAY_API_TOKEN not found in .env file")
        sys.exit(1)
    
    if args.dry_run:
        print("=== DRY RUN MODE ===\n")
        print("Would create the following columns:\n")
        print(f"1. Familienstand (dropdown)")
        print(f"   Labels: {', '.join(FAMILIENSTAND_OPTIONS)}\n")
        print(f"2. Kinder (numbers)\n")
        print(f"3. Geburtsland (dropdown)")
        print(f"   Labels: {len(GEBURTSLAND_OPTIONS)} countries")
        print(f"   First 10: {', '.join(GEBURTSLAND_OPTIONS[:10])}...")
        return
    
    client = MondayAPIClient(api_token)
    
    print(f"Creating columns in board {TARGET_BOARD_ID}...\n")
    
    created_columns = {}
    
    # Create Familienstand
    print("1. Creating Familienstand...")
    result = create_dropdown_column(client, TARGET_BOARD_ID, "Familienstand", FAMILIENSTAND_OPTIONS)
    created_columns["familienstand"] = result
    
    # Create Kinder
    print("\n2. Creating Kinder...")
    result = create_number_column(client, TARGET_BOARD_ID, "Kinder")
    created_columns["kinder"] = result
    
    # Create Geburtsland
    print("\n3. Creating Geburtsland...")
    result = create_dropdown_column(client, TARGET_BOARD_ID, "Geburtsland", GEBURTSLAND_OPTIONS)
    created_columns["geburtsland"] = result
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY - New Column IDs:")
    print("="*60)
    for name, info in created_columns.items():
        print(f"  {name}: {info.get('id')}")
    print("="*60)
    
    print("\nAdd these IDs to column_mapping.yaml:")
    print(f"""
  # Familienstand
  - source_column_id: "dropdown_mktvv9h8"
    target_column_id: "{created_columns['familienstand'].get('id')}"
    merge_strategy: "only_if_empty"

  # Kinder (Text zu Zahl)
  - source_column_id: "text_mkv6jasp"
    target_column_id: "{created_columns['kinder'].get('id')}"
    transform: "parse_number"
    merge_strategy: "only_if_empty"

  # Geburtsland (Text zu Dropdown)
  - source_column_id: "text_mktv4e5t"
    target_column_id: "{created_columns['geburtsland'].get('id')}"
    transform: "map_country"
    merge_strategy: "only_if_empty"
""")


if __name__ == "__main__":
    main()
