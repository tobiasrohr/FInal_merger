#!/usr/bin/env python3
"""
Backup Target Board

Creates a complete backup of the target board before merge.
"""

import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv
from export_boards import MondayAPIClient

TARGET_BOARD_ID = "3567618324"


def main():
    # Load API token
    api_token = os.getenv("MONDAY_API_TOKEN")
    if not api_token:
        print("Error: MONDAY_API_TOKEN not found in .env file")
        sys.exit(1)
    
    client = MondayAPIClient(api_token)
    
    # Create backup directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"output/backup_{timestamp}"
    os.makedirs(backup_dir, exist_ok=True)
    
    print("="*60)
    print("Backing up target board...")
    print("="*60)
    
    # Get board info
    board_info = client.get_board_info(TARGET_BOARD_ID)
    board_name = board_info.get("name", "Unknown")
    
    print(f"\nBoard: {board_name} ({TARGET_BOARD_ID})")
    
    # Export structure
    columns = board_info.get("columns", [])
    print(f"Columns: {len(columns)}")
    
    # Export all items
    print("\nExporting items...")
    all_items = []
    cursor = None
    page = 1
    
    while True:
        print(f"  Page {page}...", end=" ", flush=True)
        result = client.get_all_items_paginated(TARGET_BOARD_ID, cursor=cursor)
        items = result.get("items", [])
        
        if not items:
            break
        
        all_items.extend(items)
        print(f"{len(items)} items (total: {len(all_items)})")
        
        cursor = result.get("cursor")
        if not cursor:
            break
        
        page += 1
    
    # Save backup
    backup_data = {
        "board_id": TARGET_BOARD_ID,
        "board_name": board_name,
        "backup_date": datetime.now().isoformat(),
        "item_count": len(all_items),
        "columns": columns,
        "items": all_items
    }
    
    backup_file = os.path.join(backup_dir, "backup.json")
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"Backup complete!")
    print(f"  Items backed up: {len(all_items)}")
    print(f"  Backup saved to: {backup_file}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
