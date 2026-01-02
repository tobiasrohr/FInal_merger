#!/usr/bin/env python3
"""
Find Column IDs

Helper script to find column IDs by title or partial match.
"""

import os
import sys
import csv
import argparse
from typing import List, Dict


def load_column_export(csv_path: str) -> List[Dict]:
    """Load column export CSV."""
    columns = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            columns.append({
                "id": row.get("column_id", ""),
                "title": row.get("title", ""),
                "type": row.get("type", "")
            })
    return columns


def find_columns(columns: List[Dict], search_term: str) -> List[Dict]:
    """Find columns matching search term."""
    search_lower = search_term.lower()
    matches = []
    
    for col in columns:
        title_lower = col["title"].lower()
        id_lower = col["id"].lower()
        
        if search_lower in title_lower or search_lower in id_lower:
            matches.append(col)
    
    return matches


def main():
    parser = argparse.ArgumentParser(description="Find column IDs by title")
    parser.add_argument("--export", required=True, help="Path to column export CSV")
    parser.add_argument("--search", required=True, help="Search term (title or ID)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.export):
        print(f"Error: File not found: {args.export}")
        sys.exit(1)
    
    columns = load_column_export(args.export)
    matches = find_columns(columns, args.search)
    
    if not matches:
        print(f"No columns found matching '{args.search}'")
        sys.exit(1)
    
    print(f"Found {len(matches)} matching column(s):\n")
    print(f"{'Column ID':<30} {'Title':<40} {'Type':<15}")
    print("-" * 85)
    
    for col in matches:
        print(f"{col['id']:<30} {col['title']:<40} {col['type']:<15}")


if __name__ == "__main__":
    main()
