#!/usr/bin/env python3
"""
Export Monday.com Board Structure and Items

Exports column metadata and item data from source and target boards
for analysis and mapping purposes.
"""

import os
import sys
import json
import csv
import time
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

# Monday.com API endpoint
MONDAY_API_URL = "https://api.monday.com/v2"

# Board IDs from user
SOURCE_BOARD_ID = "9661290405"
TARGET_BOARD_ID = "3567618324"


class MondayAPIClient:
    """Client for interacting with Monday.com GraphQL API."""
    
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.headers = {
            "Authorization": api_token,
            "Content-Type": "application/json"
        }
    
    def execute_query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """Execute a GraphQL query/mutation."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        
        response = requests.post(
            MONDAY_API_URL,
            json=payload,
            headers=self.headers
        )
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Rate limited. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            return self.execute_query(query, variables)
        
        response.raise_for_status()
        data = response.json()
        
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        
        return data.get("data", {})
    
    def get_board_info(self, board_id: str) -> Dict:
        """Fetch board name, columns, and groups."""
        query = """
        query GetBoardInfo($boardId: [ID!]!) {
            boards(ids: $boardId) {
                id
                name
                columns {
                    id
                    title
                    type
                    settings_str
                }
                groups {
                    id
                    title
                }
            }
        }
        """
        variables = {"boardId": [board_id]}
        result = self.execute_query(query, variables)
        boards = result.get("boards", [])
        return boards[0] if boards else {}
    
    def get_all_items_paginated(self, board_id: str, cursor: Optional[str] = None, limit: int = 500, include_updates: bool = True) -> Dict:
        """Fetch items from board with pagination."""
        if include_updates:
            query = """
            query GetItems($boardId: [ID!]!, $cursor: String, $limit: Int!) {
                boards(ids: $boardId) {
                    items_page(limit: $limit, cursor: $cursor) {
                        cursor
                        items {
                            id
                            name
                            column_values {
                                id
                                text
                                value
                                type
                            }
                            updates (limit: 2) {
                                body
                                created_at
                                creator {
                                    name
                                }
                            }
                        }
                    }
                }
            }
            """
        else:
            query = """
            query GetItems($boardId: [ID!]!, $cursor: String, $limit: Int!) {
                boards(ids: $boardId) {
                    items_page(limit: $limit, cursor: $cursor) {
                        cursor
                        items {
                            id
                            name
                            column_values {
                                id
                                text
                                value
                                type
                            }
                        }
                    }
                }
            }
            """
        variables = {
            "boardId": [board_id],
            "limit": limit
        }
        if cursor:
            variables["cursor"] = cursor
        
        result = self.execute_query(query, variables)
        boards = result.get("boards", [])
        if not boards:
            return {"cursor": None, "items": []}
        
        items_page = boards[0].get("items_page", {})
        return {
            "cursor": items_page.get("cursor"),
            "items": items_page.get("items", [])
        }


def export_board_structure(client: MondayAPIClient, board_id: str, board_name: str, output_dir: str):
    """Export board column structure to CSV."""
    print(f"\nExporting structure for board {board_id} ({board_name})...")
    
    board_info = client.get_board_info(board_id)
    columns = board_info.get("columns", [])
    
    csv_path = os.path.join(output_dir, f"board_{board_id}_columns.csv")
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["column_id", "title", "type", "settings_str"])
        
        for col in columns:
            writer.writerow([
                col.get("id", ""),
                col.get("title", ""),
                col.get("type", ""),
                col.get("settings_str", "")
            ])
    
    print(f"  ✓ Exported {len(columns)} columns to {csv_path}")
    
    # Also save as JSON for easier programmatic access
    json_path = os.path.join(output_dir, f"board_{board_id}_columns.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump({
            "board_id": board_id,
            "board_name": board_name,
            "columns": columns
        }, f, indent=2, ensure_ascii=False)
    
    return columns


def export_board_items(client: MondayAPIClient, board_id: str, board_name: str, output_dir: str, limit: Optional[int] = None):
    """Export all items from board with pagination."""
    print(f"\nExporting items from board {board_id} ({board_name})...")
    
    all_items = []
    cursor = None
    page = 1
    
    while True:
        print(f"  Fetching page {page}...", end=" ", flush=True)
        result = client.get_all_items_paginated(board_id, cursor=cursor)
        items = result.get("items", [])
        
        if not items:
            break
        
        all_items.extend(items)
        print(f"got {len(items)} items (total: {len(all_items)})")
        
        if limit and len(all_items) >= limit:
            all_items = all_items[:limit]
            break
        
        cursor = result.get("cursor")
        if not cursor:
            break
        
        page += 1
        time.sleep(0.5)  # Rate limit protection
    
    print(f"  ✓ Exported {len(all_items)} items total")
    
    # Save as JSON
    json_path = os.path.join(output_dir, f"board_{board_id}_items.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump({
            "board_id": board_id,
            "board_name": board_name,
            "export_date": datetime.now().isoformat(),
            "item_count": len(all_items),
            "items": all_items
        }, f, indent=2, ensure_ascii=False)
    
    return all_items


def create_column_comparison(source_cols: List[Dict], target_cols: List[Dict], output_dir: str):
    """Create side-by-side comparison of columns from both boards."""
    print("\nCreating column comparison...")
    
    csv_path = os.path.join(output_dir, "column_comparison.csv")
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "source_column_id", "source_title", "source_type",
            "target_column_id", "target_title", "target_type",
            "match_type"
        ])
        
        # Create lookup by title (case-insensitive)
        target_by_title = {col.get("title", "").lower(): col for col in target_cols}
        target_by_id = {col.get("id"): col for col in target_cols}
        
        matched_target_ids = set()
        
        # Match source columns to target columns
        for src_col in source_cols:
            src_id = src_col.get("id", "")
            src_title = src_col.get("title", "")
            src_type = src_col.get("type", "")
            
            # Try to find match
            tgt_col = None
            match_type = "no_match"
            
            # First try by exact ID match
            if src_id in target_by_id:
                tgt_col = target_by_id[src_id]
                match_type = "id_match"
            # Then try by title match
            elif src_title.lower() in target_by_title:
                tgt_col = target_by_title[src_title.lower()]
                match_type = "title_match"
            
            if tgt_col:
                matched_target_ids.add(tgt_col.get("id"))
                writer.writerow([
                    src_id, src_title, src_type,
                    tgt_col.get("id"), tgt_col.get("title"), tgt_col.get("type"),
                    match_type
                ])
            else:
                writer.writerow([
                    src_id, src_title, src_type,
                    "", "", "",
                    "no_match"
                ])
        
        # Add unmatched target columns
        for tgt_col in target_cols:
            if tgt_col.get("id") not in matched_target_ids:
                writer.writerow([
                    "", "", "",
                    tgt_col.get("id"), tgt_col.get("title"), tgt_col.get("type"),
                    "target_only"
                ])
    
    print(f"  ✓ Column comparison saved to {csv_path}")


def main():
    # Load API token
    api_token = os.getenv("MONDAY_API_TOKEN")
    if not api_token:
        print("Error: MONDAY_API_TOKEN not found in .env file")
        sys.exit(1)
    
    client = MondayAPIClient(api_token)
    
    # Create output directory with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"output/export_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    print("="*60)
    print("Monday.com Board Export")
    print("="*60)
    
    # Export source board
    source_info = client.get_board_info(SOURCE_BOARD_ID)
    source_name = source_info.get("name", "Unknown")
    source_cols = export_board_structure(client, SOURCE_BOARD_ID, source_name, output_dir)
    
    # Export target board
    target_info = client.get_board_info(TARGET_BOARD_ID)
    target_name = target_info.get("name", "Unknown")
    target_cols = export_board_structure(client, TARGET_BOARD_ID, target_name, output_dir)
    
    # Create comparison
    create_column_comparison(source_cols, target_cols, output_dir)
    
    # Export items (optional - can be large)
    print("\n" + "="*60)
    export_items = input("Export items? This may take a while for large boards (y/n): ").lower().strip()
    if export_items == 'y':
        export_board_items(client, SOURCE_BOARD_ID, source_name, output_dir)
        export_board_items(client, TARGET_BOARD_ID, target_name, output_dir)
    
    print("\n" + "="*60)
    print(f"Export complete! Files saved to: {output_dir}")
    print("="*60)


if __name__ == "__main__":
    main()
