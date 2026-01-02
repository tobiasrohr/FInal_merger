# Monday.com Board Merge Tool

Tool zum sicheren Zusammenführen von zwei Monday.com Boards ohne Überschreibung bestehender Daten.

## Übersicht

Dieses Tool führt Bewerber aus dem Quellenboard (9661290405) in das Zielboard (3567618324) zusammen:
- **Duplikate** werden erkannt und nur mit fehlenden Informationen ergänzt
- **Neue Items** werden erstellt
- **Spalten-Mapping** ermöglicht Transformationen (z.B. Text → Zahl)

## Voraussetzungen

1. Python 3.7+
2. Monday.com API Token (in `.env` Datei)
3. Installierte Dependencies:

```bash
pip install -r requirements.txt
```

Die `requirements.txt` sollte enthalten:
```
python-dotenv==1.0.0
requests==2.31.0
pyyaml==6.0.1
```

## Schritt-für-Schritt Anleitung

### Schritt 1: Board-Struktur exportieren

Exportiert Spalten-Metadaten und optional Items von beiden Boards:

```bash
cd board_merge
python export_boards.py
```

Dies erstellt:
- `output/export_YYYYMMDD_HHMMSS/board_9661290405_columns.csv`
- `output/export_YYYYMMDD_HHMMSS/board_3567618324_columns.csv`
- `output/export_YYYYMMDD_HHMMSS/column_comparison.csv`

### Schritt 2: Spalten-Mapping definieren

1. Öffne `column_comparison.csv` und identifiziere:
   - Welche Spalten direkt gemappt werden können
   - Welche Transformationen nötig sind (z.B. Gehalt Text → Zahl)
   - Welche Spalten übersprungen werden sollen

2. Bearbeite `column_mapping.yaml`:

```yaml
mappings:
  # Beispiel: Email-Spalte
  - source_column_id: "email_xyz123"
    target_column_id: "email_abc456"
    merge_strategy: "only_if_empty"
  
  # Beispiel: Gehalt Text → Zahl
  - source_column_id: "salary_text_xyz"
    target_column_id: "salary_number_abc"
    transform: "parse_salary"
    merge_strategy: "only_if_empty"
```

**Merge-Strategien:**
- `only_if_empty`: Nur aktualisieren wenn Zielspalte leer ist
- `overwrite`: Immer überschreiben (Vorsicht!)
- `skip`: Überspringen

### Schritt 3: Duplikat-Index erstellen

Erstellt einen Index aller Items im Zielboard für schnelle Duplikatserkennung:

```bash
python build_duplicate_index.py \
  --email-column "email_column_id" \
  --hf4u-column "link_column_id" \
  --candidate-id-column "candidate_id_column_id" \
  --output output/duplicate_index.json
```

**Wichtig:** Verwende die Spalten-IDs aus dem Export (nicht die Titel).

### Schritt 4: Dry-Run durchführen

Teste den Merge-Prozess ohne Änderungen:

```bash
python merge_boards.py \
  --mapping column_mapping.yaml \
  --index output/duplicate_index.json \
  --email-column "email_column_id" \
  --hf4u-column "link_column_id" \
  --candidate-id-column "candidate_id_column_id" \
  --limit 100 \
  --dry-run \
  --log logs/merge_dry_run.json
```

Prüfe die Ausgabe und das Log-File.

### Schritt 5: Produktions-Merge

Wenn der Dry-Run erfolgreich war:

```bash
python merge_boards.py \
  --mapping column_mapping.yaml \
  --index output/duplicate_index.json \
  --email-column "email_column_id" \
  --hf4u-column "link_column_id" \
  --candidate-id-column "candidate_id_column_id" \
  --log logs/merge_production_$(date +%Y%m%d_%H%M%S).json
```

**Hinweise:**
- Der Prozess kann mehrere Stunden dauern (17.000 Items)
- Rate Limiting wird automatisch behandelt
- Bei Fehlern kann der Prozess neu gestartet werden (idempotent)

### Schritt 6: Validierung

Validiere die Merge-Ergebnisse:

```bash
python validate_merge.py \
  --log logs/merge_production_YYYYMMDD_HHMMSS.json \
  --sample-size 100 \
  --output output/validation_report.md
```

## Duplikatserkennung

Das Tool verwendet drei Kriterien zur Duplikatserkennung:

1. **E-Mail-Adresse** (case-insensitive)
2. **HF4U-Nummer** (aus Link-Spalte extrahiert)
3. **Bewerber-ID + Name** (Kombination)

Wenn eines dieser Kriterien übereinstimmt, wird das Item als Duplikat behandelt.

## Spalten-Transformationen

### Gehalt: Text → Zahl

Das Tool kann Gehaltsangaben aus Textformaten parsen:
- `€ 45.000` → `45000`
- `45.000 EUR` → `45000`
- `45000` → `45000`

Definiere in `column_mapping.yaml`:
```yaml
- source_column_id: "salary_text"
  target_column_id: "salary_number"
  transform: "parse_salary"
  merge_strategy: "only_if_empty"
```

## Fehlerbehandlung

- **Rate Limiting**: Automatisches Warten und Retry bei 429-Fehlern
- **Fehler-Logging**: Alle Fehler werden im Log-File gespeichert
- **Idempotenz**: Mehrfaches Ausführen überspringt bereits verarbeitete Items

## Backup

Vor dem Produktions-Merge wird empfohlen:
1. Manuelles Backup des Zielboards in Monday.com
2. Export der aktuellen Items (via `export_boards.py`)

## Logs

Alle Logs werden gespeichert in:
- `logs/merge_*.json`: Detaillierte Merge-Logs
- `output/validation_report.md`: Validierungsbericht

## Troubleshooting

### "Column not found"
- Prüfe Spalten-IDs im Mapping (müssen exakt sein)
- Verwende IDs aus `column_comparison.csv`

### "Rate limit exceeded"
- Normal bei großen Boards
- Tool wartet automatisch, kann aber lange dauern

### "Duplicate detection not working"
- Prüfe Spalten-IDs für Email/HF4U/Candidate-ID
- Stelle sicher, dass Daten im erwarteten Format vorliegen

## Support

Bei Problemen:
1. Prüfe Log-Files
2. Führe Dry-Run mit `--limit 10` aus
3. Validiere Spalten-Mapping
