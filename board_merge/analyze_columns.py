#!/usr/bin/env python3
"""
Analyze Column Mapping

Helps identify which columns need mapping and suggests transformations.
"""

import os
import sys
import csv
import json
import argparse
from typing import Dict, List


def load_column_export(csv_path: str) -> List[Dict]:
    """Load column export CSV."""
    columns = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            columns.append({
                "id": row.get("column_id", ""),
                "title": row.get("title", ""),
                "type": row.get("type", ""),
                "settings": row.get("settings_str", "")
            })
    return columns


def analyze_column_mapping(source_csv: str, target_csv: str, comparison_csv: str):
    """Analyze column mapping and suggest transformations."""
    
    source_cols = load_column_export(source_csv)
    target_cols = load_column_export(target_csv)
    
    # Load comparison
    matches = {}
    with open(comparison_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            src_id = row.get("source_column_id")
            tgt_id = row.get("target_column_id")
            match_type = row.get("match_type", "")
            
            if src_id and tgt_id:
                matches[src_id] = {
                    "target_id": tgt_id,
                    "match_type": match_type,
                    "source_title": row.get("source_title", ""),
                    "target_title": row.get("target_title", ""),
                    "source_type": row.get("source_type", ""),
                    "target_type": row.get("target_type", "")
                }
    
    print("="*80)
    print("Column Mapping Analysis")
    print("="*80)
    
    print("\n1. Direct Matches (ID or Title):")
    print("-" * 80)
    direct_matches = [m for m in matches.values() if m["match_type"] in ["id_match", "title_match"]]
    for match in direct_matches:
        print(f"  ✓ {match['source_title']} ({match['source_type']}) → {match['target_title']} ({match['target_type']})")
        if match['source_type'] != match['target_type']:
            print(f"    ⚠ Type mismatch: {match['source_type']} → {match['target_type']}")
    
    print(f"\n   Total: {len(direct_matches)} direct matches")
    
    print("\n2. Potential Transformations Needed:")
    print("-" * 80)
    transformations = []
    for match in direct_matches:
        src_type = match['source_type']
        tgt_type = match['target_type']
        
        if src_type == "text" and tgt_type == "numeric":
            # Check if it's likely a salary column
            title_lower = match['source_title'].lower()
            if any(keyword in title_lower for keyword in ['gehalt', 'salary', 'lohn', 'vergütung']):
                transformations.append({
                    "source": match['source_title'],
                    "target": match['target_title'],
                    "transform": "parse_salary",
                    "reason": "Text to numeric conversion (likely salary)"
                })
        elif src_type != tgt_type:
            transformations.append({
                "source": match['source_title'],
                "target": match['target_title'],
                "transform": "manual_review",
                "reason": f"Type conversion needed: {src_type} → {tgt_type}"
            })
    
    if transformations:
        for t in transformations:
            print(f"  ⚠ {t['source']} → {t['target']}")
            print(f"    Reason: {t['reason']}")
            if t['transform'] != "manual_review":
                print(f"    Suggested transform: {t['transform']}")
    else:
        print("  No transformations needed")
    
    print("\n3. Unmatched Source Columns:")
    print("-" * 80)
    unmatched = []
    for src_col in source_cols:
        src_id = src_col['id']
        if src_id not in matches or not matches[src_id].get("target_id"):
            unmatched.append(src_col)
    
    if unmatched:
        for col in unmatched[:20]:  # Show first 20
            print(f"  ? {col['title']} ({col['type']}) - ID: {col['id']}")
        if len(unmatched) > 20:
            print(f"  ... and {len(unmatched) - 20} more")
    else:
        print("  All columns matched")
    
    print("\n4. Target-Only Columns:")
    print("-" * 80)
    target_only = []
    matched_target_ids = {m["target_id"] for m in matches.values() if m.get("target_id")}
    for tgt_col in target_cols:
        if tgt_col['id'] not in matched_target_ids:
            target_only.append(tgt_col)
    
    if target_only:
        for col in target_only[:10]:  # Show first 10
            print(f"  → {col['title']} ({col['type']}) - ID: {col['id']}")
        if len(target_only) > 10:
            print(f"  ... and {len(target_only) - 10} more")
    else:
        print("  No target-only columns")
    
    print("\n" + "="*80)
    print("Summary:")
    print(f"  Source columns: {len(source_cols)}")
    print(f"  Target columns: {len(target_cols)}")
    print(f"  Direct matches: {len(direct_matches)}")
    print(f"  Transformations needed: {len(transformations)}")
    print(f"  Unmatched source columns: {len(unmatched)}")
    print(f"  Target-only columns: {len(target_only)}")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(description="Analyze column mapping")
    parser.add_argument("--source", required=True, help="Source board columns CSV")
    parser.add_argument("--target", required=True, help="Target board columns CSV")
    parser.add_argument("--comparison", required=True, help="Column comparison CSV")
    
    args = parser.parse_args()
    
    if not all(os.path.exists(f) for f in [args.source, args.target, args.comparison]):
        print("Error: One or more files not found")
        sys.exit(1)
    
    analyze_column_mapping(args.source, args.target, args.comparison)


if __name__ == "__main__":
    main()
