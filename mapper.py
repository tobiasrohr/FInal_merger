#!/usr/bin/env python3
"""
Monday.com Gender to Salutation Mapper

Maps gender values (männlich/weiblich) from source column to salutations (Herr/Frau) 
in target column.
"""

import os
import sys
import time
import json
import argparse
from typing import Dict, Optional
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

# Monday.com API endpoint
MONDAY_API_URL = "https://api.monday.com/v2"

# Mapping: source value -> target value
GENDER_MAPPING = {
    "weiblich": "Frau",
    "männlich": "Herr"
}


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
    
    def get_board_columns(self, board_id: str) -> Dict:
        """Fetch board column metadata."""
        query = """
        query GetBoardColumns($boardId: [ID!]!) {
            boards(ids: $boardId) {
                columns {
                    id
                    title
                    type
                    settings_str
                }
            }
        }
        """
        variables = {"boardId": [board_id]}
        result = self.execute_query(query, variables)
        return result.get("boards", [{}])[0].get("columns", [])
    
    def get_column_option_ids(self, board_id: str, column_id: str) -> Dict[str, str]:
        """Extract option IDs for a dropdown column."""
        columns = self.get_board_columns(board_id)
        
        for col in columns:
            if col["id"] == column_id:
                if col["type"] != "dropdown":
                    raise ValueError(f"Column {column_id} is not a dropdown column")
                
                settings = json.loads(col.get("settings_str", "{}"))
                labels = settings.get("labels", {})
                
                # Build mapping: label -> option ID
                option_map = {}
                
                # Handle different label formats from Monday.com API
                if isinstance(labels, dict):
                    # Format: {"option_id": {"name": "Label"}}
                    for option_id, label_data in labels.items():
                        if isinstance(label_data, dict):
                            label = label_data.get("name", "")
                        else:
                            label = str(label_data)
                        option_map[label] = option_id
                elif isinstance(labels, list):
                    # Format: [{"id": "option_id", "name": "Label"}, ...]
                    for label_item in labels:
                        if isinstance(label_item, dict):
                            option_id = label_item.get("id", "")
                            label = label_item.get("name", "")
                            option_map[label] = option_id
                
                return option_map
        
        raise ValueError(f"Column {column_id} not found on board {board_id}")
    
    def get_all_items(self, board_id: str, source_column: str, target_column: str) -> list:
        """Fetch all items from a board."""
        # Try to fetch items using items_page with a reasonable limit
        query = """
        query GetItems($boardId: [ID!]!, $limit: Int!, $columnIds: [String!]!) {
            boards(ids: $boardId) {
                items_page(limit: $limit) {
                    items {
                        id
                        name
                        column_values(ids: $columnIds) {
                            id
                            text
                            value
                        }
                    }
                }
            }
        }
        """
        # Fetch up to 500 items (should be enough for most cases)
        variables = {
            "boardId": [board_id],
            "limit": 500,
            "columnIds": [source_column, target_column]
        }
        
        result = self.execute_query(query, variables)
        boards = result.get("boards", [])
        if not boards:
            return []
        
        items_page = boards[0].get("items_page", {})
        items = items_page.get("items", [])
        return items
    


def main():
    parser = argparse.ArgumentParser(
        description="Map gender values to salutations in Monday.com"
    )
    parser.add_argument(
        "--board",
        type=str,
        default="9661290405",
        help="Monday.com board ID (default: 9661290405)"
    )
    parser.add_argument(
        "--source-column",
        type=str,
        default="dropdown_mktvnt0e",
        help="Source column ID (default: dropdown_mktvnt0e)"
    )
    parser.add_argument(
        "--target-column",
        type=str,
        default="drop_down4",
        help="Target column ID (default: drop_down4)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without making changes"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit processing to first N items (for testing)"
    )
    
    args = parser.parse_args()
    
    # Load API token
    api_token = os.getenv("MONDAY_API_TOKEN")
    if not api_token:
        print("Error: MONDAY_API_TOKEN not found in .env file")
        sys.exit(1)
    
    client = MondayAPIClient(api_token)
    
    print(f"Fetching board metadata for board {args.board}...")
    
    # Get option IDs for target column
    try:
        option_map = client.get_column_option_ids(args.board, args.target_column)
        print(f"Found {len(option_map)} options in target column:")
        for label, opt_id in option_map.items():
            print(f"  - {label}: {opt_id}")
        
        # Verify we have the required options
        required_options = set(GENDER_MAPPING.values())
        available_options = set(option_map.keys())
        missing = required_options - available_options
        if missing:
            print(f"Warning: Missing required options: {missing}")
            print("Available options:", available_options)
    except Exception as e:
        print(f"Error fetching column metadata: {e}")
        sys.exit(1)
    
    # Fetch all items
    print("\nFetching items from board...")
    items = client.get_all_items(args.board, args.source_column, args.target_column)
    if args.limit:
        items = items[:args.limit]
        print(f"Limited to first {len(items)} items (--limit={args.limit})")
    else:
        print(f"Found {len(items)} items")
    
    # Process items
    updated_count = 0
    skipped_count = 0
    error_count = 0
    
    print("\nProcessing items...")
    for item in items:
        item_id = item["id"]
        item_name = item.get("name", "Unnamed")
        
        # Find source and target column values
        source_value = None
        target_value = None
        
        for col_val in item.get("column_values", []):
            if col_val["id"] == args.source_column:
                text = col_val.get("text") or ""
                source_value = text.strip() if text else ""
            elif col_val["id"] == args.target_column:
                text = col_val.get("text") or ""
                target_value = text.strip() if text else ""
        
        # Skip if source is empty
        if not source_value or source_value.lower() not in GENDER_MAPPING:
            skipped_count += 1
            continue
        
        # Determine target value
        target_salutation = GENDER_MAPPING[source_value.lower()]
        
        # Skip if already correct
        if target_value == target_salutation:
            skipped_count += 1
            continue
        
        # Get option ID for target value
        if target_salutation not in option_map:
            print(f"Warning: Option '{target_salutation}' not found for item {item_name}")
            error_count += 1
            continue
        
        option_id = option_map[target_salutation]
        
        if args.dry_run:
            print(f"[DRY RUN] Would update item '{item_name}' ({item_id}):")
            print(f"  Source: {source_value} -> Target: {target_salutation} (ID: {option_id})")
            updated_count += 1
        else:
            # Update the column value using option ID (more reliable than label)
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
                "boardId": args.board,
                "itemId": item_id,
                "columnId": args.target_column,
                "value": json.dumps({"ids": [option_id]})
            }
            
            try:
                result = client.execute_query(mutation, variables)
                if "change_column_value" in result:
                    print(f"Updated item '{item_name}' ({item_id}): {source_value} -> {target_salutation}")
                    updated_count += 1
                else:
                    print(f"Failed to update item '{item_name}' ({item_id})")
                    error_count += 1
            except Exception as e:
                print(f"Error updating item '{item_name}' ({item_id}): {e}")
                error_count += 1
            
            # Rate limit protection
            time.sleep(0.3)
    
    # Summary
    print(f"\n{'='*50}")
    print("Summary:")
    print(f"  Updated: {updated_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Errors: {error_count}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()

