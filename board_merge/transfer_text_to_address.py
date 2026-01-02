#!/usr/bin/env python3
"""
Transfer Text Column to Address Column

Transfers text data from a text column to an address column within the same board.
Converts plain text to Monday.com address format.
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from export_boards import MondayAPIClient

# Load environment variables
load_dotenv()

# Default configuration
BOARD_ID = "9661290405"
SOURCE_COLUMN_ID = "text_mkv3xsq2"
TARGET_COLUMN_ID = "standort"
# Monday.com country ID for Germany
DEFAULT_COUNTRY_ID = 82


# Common German cities with their coordinates
GERMAN_CITY_COORDS = {
    "münchen": (48.1351, 11.5820),
    "berlin": (52.5200, 13.4050),
    "hamburg": (53.5511, 9.9937),
    "frankfurt": (50.1109, 8.6821),
    "köln": (50.9375, 6.9603),
    "düsseldorf": (51.2277, 6.7735),
    "stuttgart": (48.7758, 9.1829),
    "nürnberg": (49.4521, 11.0767),
    "mannheim": (49.4875, 8.4660),
    "leipzig": (51.3397, 12.3731),
    "dresden": (51.0504, 13.7373),
    "hannover": (52.3759, 9.7320),
    "bremen": (53.0793, 8.8017),
    "dortmund": (51.5136, 7.4653),
    "essen": (51.4556, 7.0116),
    "augsburg": (48.3705, 10.8978),
    "regensburg": (49.0134, 12.1016),
    "würzburg": (49.7913, 9.9534),
    "bonn": (50.7374, 7.0982),
    "karlsruhe": (49.0069, 8.4037),
    "wiesbaden": (50.0782, 8.2398),
    "mainz": (49.9929, 8.2473),
    "elmshorn": (53.7533, 9.6533),
    "haar": (48.1089, 11.7281),
    "erlangen": (49.5897, 11.0078),
    "fürstenfeldbruck": (48.1789, 11.2550),
}

# Default coordinates for Germany (center)
DEFAULT_GERMANY_COORDS = (51.1657, 10.4515)


def extract_city_from_text(text: str) -> Optional[str]:
    """
    Extract city name from address text.
    
    Args:
        text: Address text like "80001 München" or "Straße 1, 12345 Berlin"
        
    Returns:
        City name if found, None otherwise
    """
    text_lower = text.lower()
    
    # Check each known city
    for city in GERMAN_CITY_COORDS.keys():
        if city in text_lower:
            return city
    
    return None


def get_coordinates_for_text(text: str) -> Tuple[float, float]:
    """
    Get coordinates for address text.
    Uses known city coordinates or falls back to Germany center.
    
    Args:
        text: Address text
        
    Returns:
        Tuple of (lat, lng)
    """
    city = extract_city_from_text(text)
    if city:
        return GERMAN_CITY_COORDS[city]
    return DEFAULT_GERMANY_COORDS


def text_to_address_json(text: str, country_id: int = 82) -> str:
    """
    Convert plain text to Monday.com location column JSON format.
    
    Monday.com location columns require lat/lng coordinates.
    Uses known city coordinates or falls back to Germany center.
    
    Args:
        text: Plain text location (e.g., "Berlin", "12345 München")
        country_id: Monday.com country ID (82 = Germany, unused but kept for compatibility)
        
    Returns:
        JSON string in location column format
    """
    lat, lng = get_coordinates_for_text(text)
    
    # Monday.com location columns require lat, lng, and address
    address_data = {
        "lat": lat,
        "lng": lng,
        "address": text.strip()
    }
    return json.dumps(address_data)


def get_column_value(item: Dict, column_id: str) -> Optional[Dict]:
    """Get column value from item by column ID."""
    for col_val in item.get("column_values", []):
        if col_val.get("id") == column_id:
            return col_val
    return None


def is_column_empty(col_value: Optional[Dict]) -> bool:
    """Check if column value is empty."""
    if not col_value:
        return True
    
    text = col_value.get("text", "").strip()
    value = col_value.get("value", "")
    
    if text:
        return False
    
    if value:
        try:
            value_data = json.loads(value) if isinstance(value, str) else value
            if isinstance(value_data, dict):
                # For address columns, check if address field has content
                if value_data.get("address"):
                    return False
                # Check if any meaningful data exists
                return not any(v for v in value_data.values() if v)
            return False
        except:
            return False
    
    return True


def update_column_value(client: MondayAPIClient, board_id: str, item_id: str, 
                        column_id: str, value: str) -> Tuple[bool, str]:
    """
    Update a column value for an item.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    mutation = """
    mutation ChangeColumnValue($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
        change_column_value(
            board_id: $boardId,
            item_id: $itemId,
            column_id: $columnId,
            value: $value
        ) {
            id
        }
    }
    """
    
    variables = {
        "boardId": board_id,
        "itemId": item_id,
        "columnId": column_id,
        "value": value
    }
    
    try:
        client.execute_query(mutation, variables)
        return True, "OK"
    except Exception as e:
        return False, str(e)


def transfer_text_to_address(
    client: MondayAPIClient,
    board_id: str,
    source_column_id: str,
    target_column_id: str,
    country_id: int = DEFAULT_COUNTRY_ID,
    dry_run: bool = False,
    limit: Optional[int] = None
) -> Dict:
    """
    Transfer text values to address column.
    
    Args:
        client: Monday.com API client
        board_id: Board ID to process
        source_column_id: Source text column ID
        target_column_id: Target address column ID
        dry_run: If True, don't make any changes
        limit: Optional limit on number of items to process
        
    Returns:
        Statistics dictionary
    """
    stats = {
        "total_items": 0,
        "transferred": 0,
        "skipped_no_source": 0,
        "skipped_target_filled": 0,
        "errors": 0
    }
    
    transferred_items = []
    skipped_items = []
    error_items = []
    
    print(f"\n{'='*60}")
    print("Text to Address Transfer")
    print(f"{'='*60}")
    print(f"Board ID: {board_id}")
    print(f"Source Column: {source_column_id}")
    print(f"Target Column: {target_column_id}")
    print(f"Country ID: {country_id}")
    print(f"Strategy: only_if_empty")
    print(f"Dry Run: {dry_run}")
    if limit:
        print(f"Limit: {limit} items")
    print(f"{'='*60}\n")
    
    if dry_run:
        print("[DRY RUN MODE - No changes will be made]\n")
    
    cursor = None
    page = 1
    processed = 0
    
    while True:
        print(f"Processing page {page}...", flush=True)
        result = client.get_all_items_paginated(board_id, cursor=cursor, include_updates=False)
        items = result.get("items", [])
        
        if not items:
            break
        
        for item in items:
            if limit and processed >= limit:
                break
            
            item_id = item.get("id")
            item_name = item.get("name", "")
            stats["total_items"] += 1
            processed += 1
            
            # Get source column value
            source_col = get_column_value(item, source_column_id)
            source_text = source_col.get("text", "").strip() if source_col else ""
            
            if not source_text:
                stats["skipped_no_source"] += 1
                skipped_items.append({
                    "id": item_id,
                    "name": item_name,
                    "reason": "no_source_data"
                })
                continue
            
            # Check target column
            target_col = get_column_value(item, target_column_id)
            if not is_column_empty(target_col):
                stats["skipped_target_filled"] += 1
                skipped_items.append({
                    "id": item_id,
                    "name": item_name,
                    "reason": "target_not_empty"
                })
                continue
            
            # Convert and transfer
            address_json = text_to_address_json(source_text, country_id)
            
            if dry_run:
                print(f"  [DRY RUN] Item {item_id} ({item_name}): '{source_text}' -> address column")
                stats["transferred"] += 1
                transferred_items.append({
                    "id": item_id,
                    "name": item_name,
                    "source_text": source_text
                })
            else:
                success, message = update_column_value(
                    client, board_id, item_id, target_column_id, address_json
                )
                
                if success:
                    print(f"  ✓ Item {item_id} ({item_name}): '{source_text}' -> address column")
                    stats["transferred"] += 1
                    transferred_items.append({
                        "id": item_id,
                        "name": item_name,
                        "source_text": source_text
                    })
                else:
                    print(f"  ✗ Item {item_id} ({item_name}): ERROR - {message}")
                    stats["errors"] += 1
                    error_items.append({
                        "id": item_id,
                        "name": item_name,
                        "source_text": source_text,
                        "error": message
                    })
                
                # Rate limit protection
                time.sleep(0.2)
        
        if limit and processed >= limit:
            break
        
        cursor = result.get("cursor")
        if not cursor:
            break
        
        page += 1
        time.sleep(0.5)
    
    # Print summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total items processed: {stats['total_items']}")
    print(f"Transferred: {stats['transferred']}")
    print(f"Skipped (no source data): {stats['skipped_no_source']}")
    print(f"Skipped (target not empty): {stats['skipped_target_filled']}")
    print(f"Errors: {stats['errors']}")
    print(f"{'='*60}")
    
    # Print transferred items
    if transferred_items:
        print(f"\nTransferred Items ({len(transferred_items)}):")
        for item in transferred_items:
            print(f"  - ID: {item['id']}, Name: {item['name']}, Value: {item['source_text']}")
    
    # Print error items
    if error_items:
        print(f"\nFailed Operations ({len(error_items)}):")
        for item in error_items:
            print(f"  - ID: {item['id']}, Name: {item['name']}, Error: {item['error']}")
    
    return {
        "stats": stats,
        "transferred": transferred_items,
        "errors": error_items,
        "skipped": skipped_items
    }


def main():
    parser = argparse.ArgumentParser(
        description="Transfer text column values to address column"
    )
    parser.add_argument(
        "--board-id", 
        default=BOARD_ID,
        help=f"Board ID (default: {BOARD_ID})"
    )
    parser.add_argument(
        "--source-column",
        default=SOURCE_COLUMN_ID,
        help=f"Source text column ID (default: {SOURCE_COLUMN_ID})"
    )
    parser.add_argument(
        "--target-column",
        default=TARGET_COLUMN_ID,
        help=f"Target address column ID (default: {TARGET_COLUMN_ID})"
    )
    parser.add_argument(
        "--country-id",
        type=int,
        default=DEFAULT_COUNTRY_ID,
        help=f"Monday.com country ID (default: {DEFAULT_COUNTRY_ID} = Germany)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode (no changes)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of items to process"
    )
    parser.add_argument(
        "--log",
        help="Save log to JSON file"
    )
    
    args = parser.parse_args()
    
    # Load API token
    api_token = os.getenv("MONDAY_API_TOKEN")
    if not api_token:
        print("Error: MONDAY_API_TOKEN not found in .env file")
        sys.exit(1)
    
    client = MondayAPIClient(api_token)
    
    # Run transfer
    result = transfer_text_to_address(
        client,
        args.board_id,
        args.source_column,
        args.target_column,
        country_id=args.country_id,
        dry_run=args.dry_run,
        limit=args.limit
    )
    
    # Save log if requested
    if args.log:
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "board_id": args.board_id,
            "source_column": args.source_column,
            "target_column": args.target_column,
            "country_id": args.country_id,
            "dry_run": args.dry_run,
            **result
        }
        os.makedirs(os.path.dirname(args.log) if os.path.dirname(args.log) else ".", exist_ok=True)
        with open(args.log, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)
        print(f"\nLog saved to: {args.log}")
    
    # Exit with error code if there were failures
    if result["stats"]["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
