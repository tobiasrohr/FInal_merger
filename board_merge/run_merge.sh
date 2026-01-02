#!/bin/bash
# Helper script to run the merge process step by step

set -e

echo "=========================================="
echo "Monday.com Board Merge - Step by Step"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Backup
echo -e "${YELLOW}Step 1: Creating backup of target board...${NC}"
python backup_target_board.py
echo ""

# Step 2: Export structure
echo -e "${YELLOW}Step 2: Exporting board structures...${NC}"
python export_boards.py
echo ""

# Get latest export directory
LATEST_EXPORT=$(ls -td output/export_* | head -1)
echo -e "${GREEN}Export saved to: $LATEST_EXPORT${NC}"
echo ""

# Step 3: Find column IDs
echo -e "${YELLOW}Step 3: Finding column IDs...${NC}"
echo "Please identify the following column IDs from the export:"
echo "  - Email column"
echo "  - HF4U link column (column ID: 'link')"
echo "  - Candidate ID column (optional)"
echo ""
echo "You can use:"
echo "  python find_column_ids.py --export $LATEST_EXPORT/board_*_columns.csv --search 'email'"
echo ""

read -p "Email column ID: " EMAIL_COL
read -p "HF4U link column ID: " HF4U_COL
read -p "Candidate ID column ID (optional, press Enter to skip): " CANDIDATE_COL

# Step 4: Build index
echo ""
echo -e "${YELLOW}Step 4: Building duplicate index...${NC}"
if [ -z "$CANDIDATE_COL" ]; then
    python build_duplicate_index.py \
        --email-column "$EMAIL_COL" \
        --hf4u-column "$HF4U_COL" \
        --output output/duplicate_index.json
else
    python build_duplicate_index.py \
        --email-column "$EMAIL_COL" \
        --hf4u-column "$HF4U_COL" \
        --candidate-id-column "$CANDIDATE_COL" \
        --output output/duplicate_index.json
fi
echo ""

# Step 5: Dry run
echo -e "${YELLOW}Step 5: Running dry-run (100 items)...${NC}"
read -p "Continue with dry-run? (y/n): " CONTINUE

if [ "$CONTINUE" = "y" ]; then
    if [ -z "$CANDIDATE_COL" ]; then
        python merge_boards.py \
            --mapping column_mapping.yaml \
            --index output/duplicate_index.json \
            --email-column "$EMAIL_COL" \
            --hf4u-column "$HF4U_COL" \
            --limit 100 \
            --dry-run \
            --log logs/merge_dry_run_$(date +%Y%m%d_%H%M%S).json
    else
        python merge_boards.py \
            --mapping column_mapping.yaml \
            --index output/duplicate_index.json \
            --email-column "$EMAIL_COL" \
            --hf4u-column "$HF4U_COL" \
            --candidate-id-column "$CANDIDATE_COL" \
            --limit 100 \
            --dry-run \
            --log logs/merge_dry_run_$(date +%Y%m%d_%H%M%S).json
    fi
    echo ""
    echo -e "${GREEN}Dry-run complete! Review the output above.${NC}"
fi

# Step 6: Production run
echo ""
echo -e "${YELLOW}Step 6: Production merge${NC}"
read -p "Ready for production merge? This will process ALL items. (y/n): " PROD_RUN

if [ "$PROD_RUN" = "y" ]; then
    LOG_FILE="logs/merge_production_$(date +%Y%m%d_%H%M%S).json"
    
    if [ -z "$CANDIDATE_COL" ]; then
        python merge_boards.py \
            --mapping column_mapping.yaml \
            --index output/duplicate_index.json \
            --email-column "$EMAIL_COL" \
            --hf4u-column "$HF4U_COL" \
            --log "$LOG_FILE"
    else
        python merge_boards.py \
            --mapping column_mapping.yaml \
            --index output/duplicate_index.json \
            --email-column "$EMAIL_COL" \
            --hf4u-column "$HF4U_COL" \
            --candidate-id-column "$CANDIDATE_COL" \
            --log "$LOG_FILE"
    fi
    
    echo ""
    echo -e "${GREEN}Merge complete! Log saved to: $LOG_FILE${NC}"
    
    # Step 7: Validation
    echo ""
    echo -e "${YELLOW}Step 7: Running validation...${NC}"
    python validate_merge.py \
        --log "$LOG_FILE" \
        --sample-size 100 \
        --output output/validation_report.md
    
    echo ""
    echo -e "${GREEN}All done!${NC}"
fi
