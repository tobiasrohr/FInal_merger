#!/usr/bin/env python3
"""
Build Duplicate Detection Index

Creates an index of target board items keyed by:
- Email address (lowercase)
- HF4U number (from link column)
- Candidate ID + Name combination
"""

import os
import sys
import json
import re
import time
from typing import Dict, List, Set, Optional
from dotenv import load_dotenv
from export_boards import MondayAPIClient

TARGET_BOARD_ID = "3567618324"
# Column IDs to check (will be determined from export)
# These are placeholders - should be updated after column export
EMAIL_COLUMN_ID = None  # Will be set from mapping
HF4U_LINK_COLUMN_ID = None  # Will be set from mapping
CANDIDATE_ID_COLUMN_ID = None  # Will be set from mapping


def normalize_person_name(name: str) -> str:
    """
    Normalize a person's name for fallback duplicate matching.

    Normalization rules (German-friendly):
    - trim + lower
    - normalize umlauts: ä->ae, ö->oe, ü->ue, ß->ss
    - unify separators/punctuation to spaces (commas, hyphens, dots, slashes)
    - collapse multiple whitespace
    """
    if not name:
        return ""

    s = name.strip().lower()

    # German transliteration
    s = (
        s.replace("ß", "ss")
        .replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
    )

    # Unify common separators to spaces
    s = re.sub(r"[\-_,./()]+", " ", s)

    # Remove any remaining characters that are not letters/numbers/spaces
    s = re.sub(r"[^a-z0-9\s]+", " ", s)

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()

    return s


def extract_email_from_column_value(col_value: Dict) -> Optional[str]:
    """Extract email from column value."""
    text = col_value.get("text", "").strip()
    value = col_value.get("value", "")
    
    # Try text first
    if text:
        # Simple email regex
        email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        if email_match:
            return email_match.group(0).lower()
    
    # Try value (might be JSON)
    if value:
        try:
            value_data = json.loads(value) if isinstance(value, str) else value
            if isinstance(value_data, dict):
                email = value_data.get("email") or value_data.get("text", "")
                if email and "@" in email:
                    return email.lower()
        except:
            pass
    
    return None


def extract_hf4u_number(col_value: Dict) -> Optional[str]:
    """Extract HF4U number from link column.
    
    Priority:
    1. Use 'text' field from JSON value (cleanest, e.g. "13986")
    2. Extract number from URL if text field not available
    3. Fallback: extract number from display text
    """
    value = col_value.get("value", "")
    
    # First priority: Try to get 'text' from JSON value (cleanest)
    if value:
        try:
            value_data = json.loads(value) if isinstance(value, str) else value
            if isinstance(value_data, dict):
                # Check for 'text' field first (this is the HR4You number)
                text_value = value_data.get("text", "")
                if text_value:
                    return str(text_value).strip()
                
                # Fallback: extract from URL
                url = value_data.get("url", "")
                if url:
                    numbers = re.findall(r'\d+', url)
                    if numbers:
                        # Return the longest number (likely the ID)
                        return max(numbers, key=len)
        except:
            pass
    
    # Last resort: extract from display text
    text = col_value.get("text", "").strip()
    if text:
        numbers = re.findall(r'\d+', text)
        if numbers:
            # Return the longest number (likely the ID)
            return max(numbers, key=len)
    
    return None


def build_duplicate_index(client: MondayAPIClient, target_board_id: str, 
                          email_col_id: str, hf4u_col_id: str, 
                          candidate_id_col_id: Optional[str] = None) -> Dict:
    """
    Build duplicate detection index from target board.
    
    Returns:
        {
            "by_email": {email: [{"target_item_id": "...", "email": "...", "name": "..."}]},
            "by_hf4u": {hf4u_number: [{"target_item_id": "...", "hf4u_number": "...", "name": "..."}]},
            "by_candidate_id_name": {(candidate_id, name): {"target_item_id": "...", ...}},
            "items": {item_id: item_data}
        }
    """
    print("Building duplicate detection index from target board...")
    
    index = {
        "by_email": {},
        "by_hf4u": {},
        "by_candidate_id_name": {},
        "by_name": {},
        "items": {}
    }
    
    cursor = None
    page = 1
    
    while True:
        print(f"  Processing page {page}...", end=" ", flush=True)
        result = client.get_all_items_paginated(target_board_id, cursor=cursor, include_updates=False)
        items = result.get("items", [])
        
        if not items:
            break
        
        for item in items:
            item_id = item.get("id")
            item_name = item.get("name", "").strip()
            
            # Store full item data
            index["items"][item_id] = item
            
            # Extract both email and HF4U number first (we need both for each entry)
            email = None
            hf4u_num = None
            candidate_id = None
            
            for col_val in item.get("column_values", []):
                col_id = col_val.get("id")
                
                if col_id == email_col_id:
                    email = extract_email_from_column_value(col_val)
                
                elif col_id == hf4u_col_id:
                    hf4u_num = extract_hf4u_number(col_val)
                
                elif candidate_id_col_id and col_id == candidate_id_col_id:
                    candidate_id = col_val.get("text", "").strip()
            
            # Create entry with all available information
            entry = {
                "target_item_id": item_id,
                "name": item_name
            }
            if email:
                entry["email"] = email
            if hf4u_num:
                entry["hf4u_number"] = hf4u_num
            if candidate_id:
                entry["candidate_id"] = candidate_id
            
            # Add to email index
            if email:
                if email not in index["by_email"]:
                    index["by_email"][email] = []
                index["by_email"][email].append(entry)
            
            # Add to HF4U index
            if hf4u_num:
                if hf4u_num not in index["by_hf4u"]:
                    index["by_hf4u"][hf4u_num] = []
                index["by_hf4u"][hf4u_num].append(entry)
            
            # Add to candidate ID index
            if candidate_id and item_name:
                key = (candidate_id.lower(), item_name.lower())
                index["by_candidate_id_name"][key] = entry

            # Add to name-only index (fallback matching)
            if item_name:
                norm_name = normalize_person_name(item_name)
                if norm_name:
                    if norm_name not in index["by_name"]:
                        index["by_name"][norm_name] = []
                    index["by_name"][norm_name].append(entry)
        
        print(f"processed {len(items)} items")
        
        cursor = result.get("cursor")
        if not cursor:
            break
        
        page += 1
        time.sleep(0.5)  # Rate limit protection
    
    # Summary
    print(f"\nIndex summary:")
    print(f"  Total items indexed: {len(index['items'])}")
    print(f"  Items with email: {sum(1 for v in index['by_email'].values() if v)}")
    print(f"  Items with HF4U number: {sum(1 for v in index['by_hf4u'].values() if v)}")
    print(f"  Items with candidate ID+name: {len(index['by_candidate_id_name'])}")
    print(f"  Items with name key: {sum(1 for v in index['by_name'].values() if v)}")
    
    # Check for duplicates in target board
    email_dupes = {k: v for k, v in index["by_email"].items() if len(v) > 1}
    hf4u_dupes = {k: v for k, v in index["by_hf4u"].items() if len(v) > 1}
    
    if email_dupes:
        print(f"\n  Warning: Found {len(email_dupes)} duplicate emails in target board")
    if hf4u_dupes:
        print(f"  Warning: Found {len(hf4u_dupes)} duplicate HF4U numbers in target board")
    
    return index


def find_duplicate(item: Dict, index: Dict, email_col_id: str, 
                   hf4u_col_id: str, candidate_id_col_id: Optional[str] = None) -> Optional[Dict]:
    """
    Check if item is a duplicate in target board.
    
    Args:
        item: Source board item (contains source_item_id)
        index: Duplicate index
        email_col_id: Email column ID
        hf4u_col_id: HF4U link column ID
        candidate_id_col_id: Optional candidate ID column ID
    
    Returns:
        Dict with keys:
            - "target_item_id": ID in target board
            - "source_item_id": ID in source board (from item)
            - "match_type": "email" | "hf4u" | "candidate_id"
            - "email": email if matched by email
            - "hf4u_number": HF4U number if matched by HF4U
        None if no duplicate found
    """
    source_item_id = item.get("id")
    item_name = item.get("name", "").strip()
    
    # Extract values from source item first
    source_email = None
    source_hf4u_num = None
    source_candidate_id = None
    
    for col_val in item.get("column_values", []):
        col_id = col_val.get("id")
        
        if col_id == email_col_id:
            source_email = extract_email_from_column_value(col_val)
        elif col_id == hf4u_col_id:
            source_hf4u_num = extract_hf4u_number(col_val)
        elif candidate_id_col_id and col_id == candidate_id_col_id:
            source_candidate_id = col_val.get("text", "").strip()
    
    # Check by email
    if source_email and source_email in index["by_email"]:
        match_entry = index["by_email"][source_email][0]
        result = {
            "target_item_id": match_entry["target_item_id"],
            "source_item_id": source_item_id,
            "match_type": "email",
            "email": source_email,
            "name": item_name
        }
        # Add HR4You number if available (from match entry or source)
        if "hf4u_number" in match_entry:
            result["hf4u_number"] = match_entry["hf4u_number"]
        elif source_hf4u_num:
            result["hf4u_number"] = source_hf4u_num
        return result
    
    # Check by HF4U number
    if source_hf4u_num and source_hf4u_num in index["by_hf4u"]:
        match_entry = index["by_hf4u"][source_hf4u_num][0]
        result = {
            "target_item_id": match_entry["target_item_id"],
            "source_item_id": source_item_id,
            "match_type": "hf4u",
            "hf4u_number": source_hf4u_num,
            "name": item_name
        }
        # Add email if available (from match entry or source)
        if "email" in match_entry:
            result["email"] = match_entry["email"]
        elif source_email:
            result["email"] = source_email
        return result
    
    # Check by candidate ID + name
    if source_candidate_id and item_name:
        key = (source_candidate_id.lower(), item_name.lower())
        if key in index["by_candidate_id_name"]:
            match_entry = index["by_candidate_id_name"][key]
            result = {
                "target_item_id": match_entry["target_item_id"],
                "source_item_id": source_item_id,
                "match_type": "candidate_id",
                "candidate_id": source_candidate_id,
                "name": item_name
            }
            # Add email and HF4U if available
            if "email" in match_entry:
                result["email"] = match_entry["email"]
            elif source_email:
                result["email"] = source_email
            if "hf4u_number" in match_entry:
                result["hf4u_number"] = match_entry["hf4u_number"]
            elif source_hf4u_num:
                result["hf4u_number"] = source_hf4u_num
            return result

    # Fallback: name-only match (only if email and HF4U are missing)
    if (not source_email) and (not source_hf4u_num) and item_name:
        norm = normalize_person_name(item_name)
        matches = index.get("by_name", {}).get(norm, [])
        if len(matches) == 1:
            match_entry = matches[0]
            return {
                "target_item_id": match_entry["target_item_id"],
                "source_item_id": source_item_id,
                "match_type": "name_only",
                "name": item_name,
                "normalized_name": norm
            }
        elif len(matches) > 1:
            # Ambiguous: do NOT match; caller can log this
            return {
                "source_item_id": source_item_id,
                "match_type": "name_only_ambiguous",
                "name": item_name,
                "normalized_name": norm,
                "candidates": [m.get("target_item_id") for m in matches if m.get("target_item_id")]
            }
    
    return None


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Build duplicate detection index")
    parser.add_argument("--email-column", required=True, help="Email column ID in target board")
    parser.add_argument("--hf4u-column", required=True, help="HF4U link column ID in target board")
    parser.add_argument("--candidate-id-column", help="Candidate ID column ID (optional)")
    parser.add_argument("--output", default="output/duplicate_index.json", help="Output file path")
    
    args = parser.parse_args()
    
    # Load API token
    api_token = os.getenv("MONDAY_API_TOKEN")
    if not api_token:
        print("Error: MONDAY_API_TOKEN not found in .env file")
        sys.exit(1)
    
    client = MondayAPIClient(api_token)
    
    # Build index
    index = build_duplicate_index(
        client,
        TARGET_BOARD_ID,
        args.email_column,
        args.hf4u_column,
        args.candidate_id_column
    )
    
    # Save to file
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(index, f, indent=2, ensure_ascii=False)
    
    print(f"\nIndex saved to: {args.output}")


if __name__ == "__main__":
    main()
