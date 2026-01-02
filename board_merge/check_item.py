#!/usr/bin/env python3
"""Check a specific item to see its column values."""

import os
import sys
import json
from dotenv import load_dotenv
from export_boards import MondayAPIClient

TARGET_BOARD_ID = "3567618324"
SOURCE_BOARD_ID = "9661290405"

def get_item_by_id(client: MondayAPIClient, board_id: str, item_id: str):
    """Get a specific item by ID."""
    query = """
    query GetItem($boardId: [ID!]!, $itemId: ID!) {
        items(ids: [$itemId]) {
            id
            name
            board {
                id
            }
            column_values {
                id
                text
                value
                type
            }
        }
    }
    """
    variables = {
        "boardId": [board_id],
        "itemId": item_id
    }
    result = client.execute_query(query, variables)
    items = result.get("items", [])
    if items:
        # Verify it's from the right board
        board = items[0].get("board", {})
        if board.get("id") == board_id:
            return items[0]
    return None

def main():
    api_token = os.getenv("MONDAY_API_TOKEN")
    if not api_token:
        print("Error: MONDAY_API_TOKEN not found in .env file")
        sys.exit(1)
    
    client = MondayAPIClient(api_token)
    
    item_id = "5901065412"
    
    print(f"Checking item {item_id} in TARGET board ({TARGET_BOARD_ID})...")
    print("="*60)
    
    item = get_item_by_id(client, TARGET_BOARD_ID, item_id)
    
    if not item:
        print(f"Item {item_id} not found in target board. Checking source board...")
        item = get_item_by_id(client, SOURCE_BOARD_ID, item_id)
        if not item:
            print(f"Item {item_id} not found in either board!")
            sys.exit(1)
        print(f"Found in SOURCE board ({SOURCE_BOARD_ID})")
    else:
        print(f"Found in TARGET board ({TARGET_BOARD_ID})")
    
    print(f"\nItem Name: {item.get('name', 'N/A')}")
    print(f"Item ID: {item.get('id', 'N/A')}")
    print("\n" + "="*60)
    print("Column Values:")
    print("="*60)
    
    for col_val in item.get("column_values", []):
        col_id = col_val.get("id", "")
        col_text = col_val.get("text", "")
        col_value = col_val.get("value", "")
        col_type = col_val.get("type", "")
        
        # Highlight the link column
        if col_id == "link":
            print(f"\n>>> LINK COLUMN (id: {col_id}) <<<")
            print(f"  Type: {col_type}")
            print(f"  Text: {col_text}")
            print(f"  Value (raw): {col_value}")
            
            # Try to parse value
            if col_value:
                try:
                    value_data = json.loads(col_value) if isinstance(col_value, str) else col_value
                    print(f"  Value (parsed): {json.dumps(value_data, indent=4, ensure_ascii=False)}")
                except:
                    print(f"  Value (could not parse as JSON)")
        else:
            print(f"\n{col_id}:")
            print(f"  Type: {col_type}")
            print(f"  Text: {col_text[:100]}")  # First 100 chars
            if col_value:
                try:
                    value_data = json.loads(col_value) if isinstance(col_value, str) else col_value
                    if isinstance(value_data, dict):
                        print(f"  Value keys: {list(value_data.keys())}")
                except:
                    pass

if __name__ == "__main__":
    main()
