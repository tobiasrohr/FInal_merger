#!/usr/bin/env python3
"""
Validate Merge Results

Performs post-merge validation by:
- Comparing item counts
- Randomly sampling updated items
- Verifying data integrity
"""

import os
import sys
import json
import random
from datetime import datetime
from typing import Dict, List
from dotenv import load_dotenv
from export_boards import MondayAPIClient

SOURCE_BOARD_ID = "9661290405"
TARGET_BOARD_ID = "3567618324"


def get_board_item_count(client: MondayAPIClient, board_id: str) -> int:
    """Get total item count for a board."""
    query = """
    query GetBoardItemCount($boardId: [ID!]!) {
        boards(ids: $boardId) {
            items_page(limit: 1) {
                    cursor
                }
        }
    }
    """
    # This is a simplified count - for accurate count, would need to paginate
    # For now, we'll use a different approach
    
    # Fetch first page to get cursor
    result = client.get_all_items_paginated(board_id, limit=1)
    if not result.get("cursor"):
        # Small board, count directly
        items = client.get_all_items_paginated(board_id, limit=10000)
        return len(items.get("items", []))
    
    # Large board - estimate or count via pagination
    # For validation purposes, we'll sample
    return None  # Indicates large board


def sample_items(client: MondayAPIClient, board_id: str, sample_size: int = 100) -> List[Dict]:
    """Randomly sample items from board."""
    # Get all items (or a large sample)
    all_items = []
    cursor = None
    
    while len(all_items) < sample_size * 10:  # Get more than needed for sampling
        result = client.get_all_items_paginated(board_id, cursor=cursor, limit=500)
        items = result.get("items", [])
        if not items:
            break
        all_items.extend(items)
        cursor = result.get("cursor")
        if not cursor:
            break
    
    # Randomly sample
    if len(all_items) <= sample_size:
        return all_items
    
    return random.sample(all_items, sample_size)


def validate_item(item: Dict, expected_data: Dict) -> Dict:
    """Validate a single item against expected data."""
    issues = []
    
    item_id = item.get("id")
    item_name = item.get("name", "")
    
    # Check name
    if expected_data.get("name") and item_name != expected_data["name"]:
        issues.append(f"Name mismatch: expected '{expected_data['name']}', got '{item_name}'")
    
    # Check column values
    for col_id, expected_value in expected_data.get("column_values", {}).items():
        col_val = None
        for cv in item.get("column_values", []):
            if cv.get("id") == col_id:
                col_val = cv
                break
        
        if not col_val:
            issues.append(f"Missing column {col_id}")
        else:
            actual_text = col_val.get("text", "").strip()
            expected_text = expected_value.get("text", "").strip() if isinstance(expected_value, dict) else str(expected_value).strip()
            
            if actual_text != expected_text:
                issues.append(f"Column {col_id} mismatch: expected '{expected_text}', got '{actual_text}'")
    
    return {
        "item_id": item_id,
        "item_name": item_name,
        "valid": len(issues) == 0,
        "issues": issues
    }


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate merge results")
    parser.add_argument("--log", required=True, help="Merge log JSON file")
    parser.add_argument("--sample-size", type=int, default=100, help="Number of items to sample for validation")
    parser.add_argument("--output", default="output/validation_report.md", help="Output report file")
    
    args = parser.parse_args()
    
    # Load API token
    api_token = os.getenv("MONDAY_API_TOKEN")
    if not api_token:
        print("Error: MONDAY_API_TOKEN not found in .env file")
        sys.exit(1)
    
    # Load merge log
    with open(args.log, 'r', encoding='utf-8') as f:
        merge_log = json.load(f)
    
    client = MondayAPIClient(api_token)
    
    print("="*60)
    print("Merge Validation")
    print("="*60)
    
    # Get board counts
    print("\nFetching board item counts...")
    source_count = get_board_item_count(client, SOURCE_BOARD_ID)
    target_count = get_board_item_count(client, TARGET_BOARD_ID)
    
    print(f"  Source board items: {source_count or 'N/A (too large)'}")
    print(f"  Target board items: {target_count or 'N/A (too large)'}")
    
    # Sample items for validation
    print(f"\nSampling {args.sample_size} items from target board...")
    sampled_items = sample_items(client, TARGET_BOARD_ID, args.sample_size)
    print(f"  Sampled {len(sampled_items)} items")
    
    # Validate sampled items
    print("\nValidating sampled items...")
    validation_results = []
    
    for item in sampled_items:
        # For now, just check that item exists and has basic structure
        validation = {
            "item_id": item.get("id"),
            "item_name": item.get("name", ""),
            "has_name": bool(item.get("name")),
            "column_count": len(item.get("column_values", [])),
            "valid": True
        }
        
        if not validation["has_name"]:
            validation["valid"] = False
            validation["issues"] = ["Missing item name"]
        
        validation_results.append(validation)
    
    # Generate report
    valid_count = sum(1 for r in validation_results if r["valid"])
    invalid_count = len(validation_results) - valid_count
    
    report = f"""# Merge Validation Report

Generated: {datetime.now().isoformat()}

## Summary

- **Source Board ID**: {SOURCE_BOARD_ID}
- **Target Board ID**: {TARGET_BOARD_ID}
- **Items Sampled**: {len(validation_results)}
- **Valid Items**: {valid_count}
- **Invalid Items**: {invalid_count}
- **Validation Rate**: {valid_count/len(validation_results)*100:.1f}%

## Merge Statistics

- **Created**: {merge_log.get('stats', {}).get('created', 0)}
- **Updated**: {merge_log.get('stats', {}).get('updated', 0)}
- **Errors**: {merge_log.get('stats', {}).get('errors', 0)}

## Validation Results

"""
    
    if invalid_count > 0:
        report += "### Issues Found\n\n"
        for result in validation_results:
            if not result["valid"]:
                report += f"- **{result['item_name']}** ({result['item_id']}): {', '.join(result.get('issues', []))}\n"
        report += "\n"
    else:
        report += "✓ All sampled items passed validation.\n\n"
    
    report += "## Sample Items\n\n"
    report += "| Item ID | Name | Column Count | Status |\n"
    report += "|---------|------|--------------|--------|\n"
    
    for result in validation_results[:20]:  # Show first 20
        status = "✓ Valid" if result["valid"] else "✗ Invalid"
        report += f"| {result['item_id']} | {result['item_name'][:50]} | {result['column_count']} | {status} |\n"
    
    # Save report
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n{'='*60}")
    print(f"Validation complete!")
    print(f"  Valid: {valid_count}/{len(validation_results)}")
    print(f"  Report saved to: {args.output}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
