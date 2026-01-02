#!/usr/bin/env python3
"""
Transfer Jobs from HR4You column to Jobs column within the same board.

Reads items from board 9661290405, maps HR4You job values (dropdown_mktvs1mm)
to target job values (dropdown_mkws141v) using the mapping YAML file.
"""

import os
import sys
import json
import yaml
import time
from typing import Dict, List, Optional
from dotenv import load_dotenv
from export_boards import MondayAPIClient

# Board ID (same for source and target)
BOARD_ID = "9661290405"
SOURCE_COLUMN_ID = "dropdown_mktvs1mm"  # HR4You - Jobs
TARGET_COLUMN_ID = "dropdown_mkws141v"  # ➡️ Jobs

# Batch size for mutations
BATCH_SIZE = 50


class JobsTransfer:
    """Handles job value transfer within the same board."""
    
    def __init__(self, client: MondayAPIClient, mapping_config: Dict, board_columns: Dict):
        self.client = client
        self.mapping_config = mapping_config
        self.board_columns = board_columns
        self.stats = {
            "processed": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0,
            "no_mapping": 0
        }
        self.log_entries = []
        
        # Build mapping dictionaries
        self.source_value_to_target_value = {}
        self.target_value_to_option_id = {}
        
        # Build source_value -> target_value mapping
        for mapping in mapping_config.get("mappings", []):
            source_val = mapping.get("source_value", "").strip()
            target_val = mapping.get("target_value", "").strip()
            if source_val and target_val:
                self.source_value_to_target_value[source_val] = target_val
        
        # Build target_value -> option_id mapping from column definition
        self._build_target_option_mapping()
    
    def _build_target_option_mapping(self):
        """Build mapping from target value label to option ID."""
        # Find target column in board columns
        target_col = None
        for col in self.board_columns.get("columns", []):
            if col.get("id") == TARGET_COLUMN_ID:
                target_col = col
                break
        
        if not target_col:
            print(f"Error: Target column {TARGET_COLUMN_ID} not found in board columns")
            return
        
        # Parse settings_str to get labels
        settings_str = target_col.get("settings_str", "{}")
        try:
            settings = json.loads(settings_str)
            labels = settings.get("labels", [])
            
            for label in labels:
                label_id = label.get("id")
                label_name = label.get("name", "").strip()
                if label_id and label_name:
                    self.target_value_to_option_id[label_name] = label_id
            
            print(f"Loaded {len(self.target_value_to_option_id)} target option mappings")
        except Exception as e:
            print(f"Error parsing target column settings: {e}")
    
    def get_dropdown_value(self, col_value: Dict) -> Optional[str]:
        """Extract dropdown label text from column value."""
        if not col_value:
            return None
        
        text = col_value.get("text")
        if text and isinstance(text, str):
            text = text.strip()
            if text:
                return text
        
        # Try to parse value JSON
        value = col_value.get("value", "")
        if value:
            try:
                value_data = json.loads(value) if isinstance(value, str) else value
                if isinstance(value_data, dict):
                    # Dropdown values have structure like {"ids": [1, 2]}
                    ids = value_data.get("ids", [])
                    if ids:
                        # We need to look up the label name from the source column
                        # For now, return None and we'll use text field
                        pass
            except:
                pass
        
        return None
    
    def get_source_column_values(self, item: Dict) -> List[str]:
        """Get the source job values from item (can be multiple, comma-separated)."""
        for col_val in item.get("column_values", []):
            if col_val.get("id") == SOURCE_COLUMN_ID:
                value = self.get_dropdown_value(col_val)
                if value:
                    # Split by comma and strip whitespace
                    values = [v.strip() for v in value.split(",") if v.strip()]
                    return values
        return []
    
    def get_target_column_value(self, item: Dict) -> Optional[Dict]:
        """Get the target column value from item."""
        for col_val in item.get("column_values", []):
            if col_val.get("id") == TARGET_COLUMN_ID:
                return col_val
        return None
    
    def is_target_empty(self, target_col_value: Optional[Dict]) -> bool:
        """Check if target column is empty."""
        if not target_col_value:
            return True
        
        text = target_col_value.get("text", "")
        if text and isinstance(text, str) and text.strip():
            return False
        
        value = target_col_value.get("value", "")
        if value:
            try:
                value_data = json.loads(value) if isinstance(value, str) else value
                if isinstance(value_data, dict):
                    ids = value_data.get("ids", [])
                    return not ids
            except:
                pass
        
        return True
    
    def update_item_job(self, item_id: str, target_values: List[str], dry_run: bool = False):
        """Update item's target job column with new values (can be multiple)."""
        # Get option IDs for all target values
        option_ids = []
        missing_values = []
        
        for target_value in target_values:
            option_id = self.target_value_to_option_id.get(target_value)
            if option_id:
                option_ids.append(str(option_id))
            else:
                missing_values.append(target_value)
        
        if missing_values:
            self.stats["errors"] += 1
            self.log_entries.append({
                "action": "error",
                "item_id": item_id,
                "error": f"Target values not found in target column options: {missing_values}"
            })
            return False
        
        if not option_ids:
            return False
        
        if dry_run:
            self.stats["updated"] += 1
            return True
        
        # Prepare mutation
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
        
        # Dropdown values need format: {"ids": [option_id1, option_id2, ...]}
        column_value = json.dumps({"ids": option_ids})
        
        variables = {
            "boardId": BOARD_ID,
            "itemId": item_id,
            "columnId": TARGET_COLUMN_ID,
            "value": column_value
        }
        
        try:
            self.client.execute_query(mutation, variables)
            self.stats["updated"] += 1
            time.sleep(0.2)  # Rate limit protection
            return True
        except Exception as e:
            self.stats["errors"] += 1
            self.log_entries.append({
                "action": "update_error",
                "item_id": item_id,
                "error": str(e)
            })
            return False
    
    def process_item(self, item: Dict, dry_run: bool = False):
        """Process a single item."""
        item_id = item.get("id")
        item_name = item.get("name", "")
        
        # Get source job values (can be multiple)
        source_values = self.get_source_column_values(item)
        if not source_values:
            self.stats["skipped"] += 1
            return
        
        # Map each source value to target value
        target_values = []
        unmapped_values = []
        
        for source_value in source_values:
            target_value = self.source_value_to_target_value.get(source_value)
            if target_value and target_value != "":
                target_values.append(target_value)
            else:
                unmapped_values.append(source_value)
        
        # Log unmapped values
        if unmapped_values:
            self.stats["no_mapping"] += 1
            self.log_entries.append({
                "action": "no_mapping",
                "item_id": item_id,
                "item_name": item_name,
                "source_values": source_values,
                "unmapped_values": unmapped_values
            })
        
        # Skip if no target values found
        if not target_values:
            self.stats["skipped"] += 1
            return
        
        # Check if target column is empty (since merge_strategy is overwrite, we always update)
        # But we can still check for logging
        target_col_value = self.get_target_column_value(item)
        target_empty = self.is_target_empty(target_col_value)
        
        # Update item with all target values
        success = self.update_item_job(item_id, target_values, dry_run)
        
        if success:
            self.log_entries.append({
                "action": "update" if not dry_run else "would_update",
                "item_id": item_id,
                "item_name": item_name,
                "source_values": source_values,
                "target_values": target_values,
                "unmapped_values": unmapped_values if unmapped_values else None,
                "target_was_empty": target_empty
            })
    
    def transfer_jobs(self, limit: Optional[int] = None, dry_run: bool = False):
        """Main transfer process."""
        print(f"\nStarting job transfer process...")
        print(f"  Board: {BOARD_ID}")
        print(f"  Source column: {SOURCE_COLUMN_ID} (HR4You - Jobs)")
        print(f"  Target column: {TARGET_COLUMN_ID} (➡️ Jobs)")
        print(f"  Mappings: {len(self.source_value_to_target_value)}")
        print(f"  Target options: {len(self.target_value_to_option_id)}")
        print(f"  Dry run: {dry_run}")
        
        if dry_run:
            print("\n[DRY RUN MODE - No changes will be made]")
        
        cursor = None
        page = 1
        processed = 0
        
        while True:
            print(f"\nProcessing page {page}...", end=" ", flush=True)
            max_retries = 3
            retry_count = 0
            result = None
            
            while retry_count < max_retries:
                try:
                    result = self.client.get_all_items_paginated(BOARD_ID, cursor=cursor)
                    break
                except Exception as e:
                    error_msg = str(e)
                    if "CursorExpiredError" in error_msg or "CursorException" in error_msg:
                        print(f"\n  Cursor expired. Restarting from beginning (processed {processed} items so far)...")
                        cursor = None
                        page = 1
                        # Continue processing - items already updated won't be updated again due to overwrite strategy
                        result = None
                        break
                    elif "504" in error_msg or "Gateway Timeout" in error_msg or "HTTPError" in error_msg:
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = retry_count * 5
                            print(f"\n  Server timeout (attempt {retry_count}/{max_retries}). Waiting {wait_time}s...")
                            time.sleep(wait_time)
                            continue
                        else:
                            print(f"\n  Max retries reached. Saving progress and exiting.")
                            raise
                    else:
                        raise
            
            if result is None:
                continue
            
            items = result.get("items", [])
            
            if not items:
                break
            
            for item in items:
                if limit and processed >= limit:
                    break
                
                self.process_item(item, dry_run)
                processed += 1
                self.stats["processed"] += 1
                
                if processed % 100 == 0:
                    print(f"\n  Processed {processed} items...", end=" ", flush=True)
            
            if limit and processed >= limit:
                break
            
            cursor = result.get("cursor")
            if not cursor:
                break
            
            page += 1
            time.sleep(0.5)  # Rate limit protection
        
        # Print summary
        print(f"\n\n{'='*60}")
        print("Transfer Summary:")
        print(f"  Items processed: {self.stats['processed']}")
        print(f"  Updated: {self.stats['updated']}")
        print(f"  Skipped (no source value): {self.stats['skipped']}")
        print(f"  No mapping found: {self.stats['no_mapping']}")
        print(f"  Errors: {self.stats['errors']}")
        print(f"{'='*60}")
        
        return self.stats, self.log_entries


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Transfer jobs within same board")
    parser.add_argument("--mapping", default="column__mapping_jobs.yaml", help="Jobs mapping YAML file")
    parser.add_argument("--columns", default="output/export_20251209_161337/board_9661290405_columns.json", 
                       help="Board columns JSON file")
    parser.add_argument("--limit", type=int, help="Limit number of items to process (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode (no changes)")
    parser.add_argument("--log", help="Log file path")
    
    args = parser.parse_args()
    
    # Load API token
    load_dotenv()
    api_token = os.getenv("MONDAY_API_TOKEN")
    if not api_token:
        print("Error: MONDAY_API_TOKEN not found in .env file")
        sys.exit(1)
    
    # Load mapping configuration
    with open(args.mapping, 'r', encoding='utf-8') as f:
        mapping_config = yaml.safe_load(f)
    
    # Load board columns
    with open(args.columns, 'r', encoding='utf-8') as f:
        board_columns = json.load(f)
    
    client = MondayAPIClient(api_token)
    
    transfer = JobsTransfer(client, mapping_config, board_columns)
    
    # Run transfer
    try:
        stats, log_entries = transfer.transfer_jobs(
            args.limit,
            args.dry_run
        )
    except Exception as e:
        print(f"\nError during transfer: {e}")
        # Still save log with current progress
        stats = transfer.stats
        log_entries = transfer.log_entries
    
    # Save log
    if args.log:
        log_data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "stats": stats,
            "entries": log_entries
        }
        os.makedirs(os.path.dirname(args.log), exist_ok=True)
        with open(args.log, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)
        print(f"\nLog saved to: {args.log}")
    
    # Print items with no mapping
    no_mapping_items = [e for e in log_entries if e.get("action") == "no_mapping"]
    if no_mapping_items:
        print(f"\n\nItems with no mapping found ({len(no_mapping_items)}):")
        for entry in no_mapping_items[:10]:  # Show first 10
            unmapped = entry.get("unmapped_values", [])
            print(f"  - {entry.get('item_name')}: {unmapped}")
        if len(no_mapping_items) > 10:
            print(f"  ... and {len(no_mapping_items) - 10} more")


if __name__ == "__main__":
    main()




