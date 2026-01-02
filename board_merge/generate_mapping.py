#!/usr/bin/env python3
"""Generate column_mapping.yaml from column_comparison.csv"""

import csv
import yaml
import sys

def generate_mapping(comparison_csv: str, output_yaml: str):
    """Generate column_mapping.yaml from comparison CSV."""
    
    mappings = []
    
    with open(comparison_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_id = row.get("source_column_id", "").strip()
            target_id = row.get("target_column_id", "").strip()
            match_type = row.get("match_type", "").strip()
            source_title = row.get("source_title", "").strip()
            source_type = row.get("source_type", "").strip()
            target_type = row.get("target_type", "").strip()
            
            # Skip if no target column (unmatched source columns)
            if not target_id:
                continue
            
            # Skip if match_type is "target_only" (only exists in target)
            if match_type == "target_only":
                continue
            
            # Determine merge strategy
            # For identifiers (email, link), use only_if_empty
            if source_id in ["e_mail", "link"]:
                merge_strategy = "only_if_empty"
            # For most other fields, also use only_if_empty to be safe
            else:
                merge_strategy = "only_if_empty"
            
            # Check if transformation needed
            transform = None
            if source_type == "text" and target_type == "numbers":
                # Check if it's likely a salary field
                if "gehalt" in source_title.lower() or "salary" in source_title.lower():
                    transform = "parse_salary"
            
            mapping = {
                "source_column_id": source_id,
                "target_column_id": target_id,
                "merge_strategy": merge_strategy
            }
            
            if transform:
                mapping["transform"] = transform
            
            # Add comment/note
            if match_type == "title_match":
                mapping["_note"] = f"Title match: {source_title}"
            
            mappings.append(mapping)
    
    # Create YAML structure
    yaml_data = {
        "mappings": mappings,
        "skip_columns": [
            "color_mktve1f6"  # HR4You Status (not in target)
        ],
        "transformations": {
            "parse_salary": {
                "description": "Parse salary from text format (e.g., '€ 45.000' -> 45000)",
                "function": "parse_salary_text_to_number"
            }
        }
    }
    
    # Write YAML file
    with open(output_yaml, 'w', encoding='utf-8') as f:
        # Write header comment
        f.write("# Column Mapping Configuration\n")
        f.write("# Auto-generated from column_comparison.csv\n")
        f.write("# Maps columns from source board (9661290405) to target board (3567618324)\n\n")
        
        # Write mappings
        f.write("mappings:\n")
        for mapping in mappings:
            f.write(f"  # {mapping.get('_note', mapping['source_column_id'])}\n")
            f.write(f"  - source_column_id: \"{mapping['source_column_id']}\"\n")
            f.write(f"    target_column_id: \"{mapping['target_column_id']}\"\n")
            f.write(f"    merge_strategy: \"{mapping['merge_strategy']}\"\n")
            if 'transform' in mapping:
                f.write(f"    transform: \"{mapping['transform']}\"\n")
            f.write("\n")
        
        # Write skip columns
        f.write("# Columns to skip (don't transfer)\n")
        f.write("skip_columns:\n")
        for skip_col in yaml_data["skip_columns"]:
            f.write(f"  - \"{skip_col}\"\n")
        
        # Write transformations
        f.write("\n# Column transformations\n")
        f.write("transformations:\n")
        for trans_name, trans_info in yaml_data["transformations"].items():
            f.write(f"  {trans_name}:\n")
            f.write(f"    description: \"{trans_info['description']}\"\n")
            f.write(f"    function: \"{trans_info['function']}\"\n")
    
    print(f"Generated {len(mappings)} mappings")
    print(f"Saved to: {output_yaml}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_mapping.py <comparison_csv> [output_yaml]")
        sys.exit(1)
    
    comparison_csv = sys.argv[1]
    output_yaml = sys.argv[2] if len(sys.argv) > 2 else "column_mapping.yaml"
    
    generate_mapping(comparison_csv, output_yaml)
