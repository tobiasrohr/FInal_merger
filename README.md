# Monday.com Gender to Salutation Mapper

This script maps gender values (männlich/weiblich) from a source column to salutations (Herr/Frau) in a target column on a Monday.com board.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your Monday.com API token in the `.env` file:
```
MONDAY_API_TOKEN=your_api_token_here
```

You can find your API token in Monday.com under your profile settings → API.

## Usage

### Basic Usage

Run the script with default settings (board 9661290405):
```bash
python mapper.py
```

### Dry Run (Recommended First)

Test what would be updated without making changes:
```bash
python mapper.py --dry-run
```

### Custom Configuration

Specify custom board and column IDs:
```bash
python mapper.py --board YOUR_BOARD_ID --source-column SOURCE_COLUMN_ID --target-column TARGET_COLUMN_ID
```

### Command Line Options

- `--board`: Monday.com board ID (default: 9661290405)
- `--source-column`: Source column ID containing gender values (default: dropdown_mktvnt0e)
- `--target-column`: Target column ID for salutations (default: drop_down4)
- `--dry-run`: Show what would be updated without making changes

## How It Works

1. **Fetches board metadata** to retrieve dropdown option IDs for the target column
2. **Retrieves all items** from the specified board (with pagination)
3. **Maps values**:
   - `weiblich` → `Frau`
   - `männlich` → `Herr`
4. **Updates target column** only if:
   - Source column has a valid gender value
   - Target column doesn't already have the correct salutation
5. **Provides summary** of updated, skipped, and error counts

## Mapping Logic

- Source values are case-insensitive
- Items with empty or invalid source values are skipped
- Items that already have the correct target value are skipped
- Rate limiting is handled automatically with retry logic

## Example Output

```
Fetching board metadata for board 9661290405...
Found 2 options in target column:
  - Frau: 12345
  - Herr: 67890

Fetching items from board...
Found 150 items

Processing items...
Updated item 'John Doe' (1234567890): männlich -> Herr
Updated item 'Jane Smith' (0987654321): weiblich -> Frau
...

==================================================
Summary:
  Updated: 45
  Skipped: 100
  Errors: 5
==================================================
```

## Error Handling

- Rate limiting: Automatically waits and retries on 429 responses
- Missing options: Warns if required dropdown options don't exist
- API errors: Logs errors and continues with remaining items






