# Quick Start Guide

## Schnellstart für den Board-Merge

### Vorbereitung

1. **Dependencies installieren:**
```bash
pip install -r ../requirements.txt
```

2. **API Token prüfen:**
Stelle sicher, dass `MONDAY_API_TOKEN` in der `.env` Datei im Hauptverzeichnis gesetzt ist.

### Schritt 1: Backup erstellen

```bash
python backup_target_board.py
```

### Schritt 2: Board-Struktur exportieren

```bash
python export_boards.py
```

Wähle `y` wenn nach Item-Export gefragt wird (optional, aber empfohlen).

### Schritt 3: Spalten-IDs finden

Finde die benötigten Spalten-IDs:

```bash
# Email-Spalte finden
python find_column_ids.py --export output/export_*/board_9661290405_columns.csv --search "email"

# HF4U/Link-Spalte finden (Spalten-ID sollte "link" enthalten)
python find_column_ids.py --export output/export_*/board_9661290405_columns.csv --search "link"

# Bewerber-ID finden
python find_column_ids.py --export output/export_*/board_9661290405_columns.csv --search "bewerber"
```

Notiere dir die Spalten-IDs!

### Schritt 4: Spalten-Mapping konfigurieren

Öffne `column_mapping.yaml` und trage die Mappings ein basierend auf `output/export_*/column_comparison.csv`.

### Schritt 5: Duplikat-Index erstellen

```bash
python build_duplicate_index.py \
  --email-column "DEINE_EMAIL_SPALTEN_ID" \
  --hf4u-column "DEINE_LINK_SPALTEN_ID" \
  --candidate-id-column "DEINE_BEWERBER_ID_SPALTEN_ID" \
  --output output/duplicate_index.json
```

### Schritt 6: Dry-Run (Test)

```bash
python merge_boards.py \
  --mapping column_mapping.yaml \
  --index output/duplicate_index.json \
  --email-column "DEINE_EMAIL_SPALTEN_ID" \
  --hf4u-column "DEINE_LINK_SPALTEN_ID" \
  --candidate-id-column "DEINE_BEWERBER_ID_SPALTEN_ID" \
  --limit 100 \
  --dry-run \
  --log logs/merge_dry_run.json
```

### Schritt 7: Produktions-Merge

**WICHTIG:** Nur ausführen wenn Dry-Run erfolgreich war!

```bash
python merge_boards.py \
  --mapping column_mapping.yaml \
  --index output/duplicate_index.json \
  --email-column "DEINE_EMAIL_SPALTEN_ID" \
  --hf4u-column "DEINE_LINK_SPALTEN_ID" \
  --candidate-id-column "DEINE_BEWERBER_ID_SPALTEN_ID" \
  --log logs/merge_production_$(date +%Y%m%d_%H%M%S).json
```

### Schritt 8: Validierung

```bash
python validate_merge.py \
  --log logs/merge_production_*.json \
  --sample-size 100 \
  --output output/validation_report.md
```

## Oder: Alles auf einmal

Verwende das interaktive Script:

```bash
./run_merge.sh
```

Dies führt dich Schritt für Schritt durch den gesamten Prozess.
