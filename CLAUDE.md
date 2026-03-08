# Epstein Files — Data Repository

## What This Is

The DOJ released 2.73 million pages of Epstein case files on January 30, 2026, across 12 datasets at [justice.gov/epstein](https://www.justice.gov/epstein/). This repository contains **searchable databases** built from those files — 1,385,916 documents and 2,771,231 pages of extracted text, fully indexed for full-text search.

The databases are hosted as GitHub releases because they're too large for the repository itself (~17 GB uncompressed total).

## First-Time Setup

### 1. Check for sqlite3

```bash
sqlite3 --version
```

If missing: Mac/Linux usually have it pre-installed. On Ubuntu/Debian: `sudo apt install sqlite3`. On Windows: download from [sqlite.org/download.html](https://www.sqlite.org/download.html) and add to PATH, or use `winget install SQLite.SQLite`.

### 2. Download the databases

You need files from **three releases**. Use `gh release download` if available, otherwise `curl -LO`.

#### Full text corpus (v5.0) — the main database

```bash
# Download both parts (~2.3 GB total compressed)
gh release download v5.0 --repo rhowardstone/Epstein-research-data --pattern "full_text_corpus.db.gz.*"

# Reassemble and decompress
cat full_text_corpus.db.gz.part_aa full_text_corpus.db.gz.part_ab > full_text_corpus.db.gz
gunzip full_text_corpus.db.gz
# Result: full_text_corpus.db (~6.3 GB)

# Clean up parts
rm full_text_corpus.db.gz.part_aa full_text_corpus.db.gz.part_ab
```

#### Concordance + alteration analysis (v5.1)

```bash
gh release download v5.1 --repo rhowardstone/Epstein-research-data --pattern "*.db.gz"
gunzip concordance_complete.db.gz alteration_results.db.gz
```

#### All other databases (v4.0)

```bash
# Download all database files (skip the full_text_corpus parts — you already have v5.0)
gh release download v4.0 --repo rhowardstone/Epstein-research-data --pattern "*.db.gz"
gh release download v4.0 --repo rhowardstone/Epstein-research-data --pattern "*.db"

# Decompress
gunzip *.db.gz
```

If `gh` is not available, download manually from:
- https://github.com/rhowardstone/Epstein-research-data/releases/tag/v5.0
- https://github.com/rhowardstone/Epstein-research-data/releases/tag/v5.1
- https://github.com/rhowardstone/Epstein-research-data/releases/tag/v4.0

### 3. Verify setup

```bash
sqlite3 full_text_corpus.db "SELECT COUNT(*) || ' documents, ' || (SELECT COUNT(*) FROM pages) || ' pages' FROM documents;"
```

Expected output: `1385916 documents, 2771231 pages`

---

## Database Reference

### `full_text_corpus.db` (6.3 GB) — Primary search database

The main database. Every page of every document, with full-text search.

**Tables:**

```sql
-- documents: one row per PDF/document
-- Columns: id, efta_number (unique), dataset (1-12, 98, 99), file_path, total_pages, file_size
SELECT * FROM documents WHERE efta_number = 'EFTA00074206';

-- pages: one row per page of each document
-- Columns: id, efta_number, page_number, text_content, char_count
SELECT * FROM pages WHERE efta_number = 'EFTA00074206' ORDER BY page_number;

-- pages_fts: FTS5 full-text search index on pages
-- Searchable columns: efta_number, text_content
SELECT * FROM pages_fts WHERE pages_fts MATCH 'search terms';
```

### `concordance_complete.db` (729 MB) — Cross-reference metadata

DOJ production metadata: original filenames, email headers, folder paths, dates, custodians, MD5 hashes.

**Key columns in `documents` table:** bates_begin, bates_end, original_filename, document_extension, original_folder_path, author, custodian, date_sent, email_from, email_to, email_cc, email_subject, efta_number

Also has: `email_threads`, `folder_inventory`, `extraction_stats`, `cross_references`

### `redaction_analysis_v2.db` (1.0 GB) — Redaction patterns

2.6 million detected redaction rectangles across 850K documents. Includes OCR of text under improperly applied redactions.

**Tables:** `redactions` (efta_number, page_number, redaction_type, ocr_text, confidence), `document_summary`, `reconstructed_pages`

### `alteration_results.db` (557 MB) — DOJ document alteration tracking

212,730 change units tracking differences between original and current versions of DOJ-hosted documents.

**Table:** `altered_files` (efta_number, dataset, diff_type, categories, removed_names_json, llm_classification, llm_sensitivity, llm_justification, anomaly_flag)

### `image_analysis.db` (407 MB) — Extracted images

21,859 images extracted from PDFs, analyzed with AI vision. FTS5 searchable.

**Table:** `images` (image_name, efta_number, page_number, analysis_text, people, text_content, objects, setting, notable)

### `transcripts.db` (5 MB) — Audio/video transcriptions

1,530 media files, 375 with speech, 92K words total. Whisper large-v3 transcriptions.

**Tables:** `transcripts` (efta_number, transcript, duration_secs, word_count), `transcript_segments` (start_time, end_time, text)

### `prosecutorial_query_graph.db` (2.5 MB) — Subpoena analysis

Grand jury subpoena tracking: what was demanded vs. what was produced.

**Tables:** `subpoenas`, `rider_clauses`, `returns`, `subpoena_return_links`, `clause_fulfillment`, `investigative_gaps`

### Other databases

| Database | Size | Contents |
|----------|------|----------|
| `redaction_analysis_ds10.db` | 557 MB | DS10-specific redaction analysis |
| `knowledge_graph.db` | 782 KB | 606 entities, 2,302 relationships |
| `ocr_database.db` | 71 MB | Tesseract OCR results for scanned pages |
| `spreadsheet_corpus.db` | 4 MB | Native spreadsheet data from DS8 |
| `communications.db` | 24 MB | Extracted communications metadata |

---

## Search Cookbook

### Full-text search (fastest)

```sql
-- Search for a term using FTS5
SELECT p.efta_number, p.page_number, substr(p.text_content, 1, 500)
FROM pages_fts fts
JOIN pages p ON p.rowid = fts.rowid
WHERE pages_fts MATCH 'Leon Black'
AND p.char_count > 50
LIMIT 20;
```

FTS5 supports phrases (`"exact phrase"`), AND/OR (`term1 AND term2`), NOT (`term1 NOT term2`), prefix (`term*`), and column filters (`text_content:term`).

### LIKE search (for partial matches, wildcards)

```sql
-- Slower but catches OCR variations and partial words
SELECT efta_number, page_number, substr(text_content, 1, 500)
FROM pages
WHERE text_content LIKE '%Deutsche Bank%'
AND char_count > 50
LIMIT 20;
```

### Read a specific document

```sql
-- Get all pages of one document
SELECT page_number, text_content
FROM pages
WHERE efta_number = 'EFTA00074206'
ORDER BY page_number;
```

### Find which dataset an EFTA belongs to

```sql
SELECT efta_number, dataset, total_pages, file_path
FROM documents
WHERE efta_number = 'EFTA00074206';
```

### Person search with context

```sql
-- Find documents mentioning a person, with surrounding text
SELECT p.efta_number, p.page_number,
       substr(p.text_content,
              MAX(1, INSTR(LOWER(p.text_content), LOWER('Ghislaine Maxwell')) - 200),
              500) AS context
FROM pages p
WHERE p.text_content LIKE '%Ghislaine Maxwell%'
AND p.char_count > 50
LIMIT 30;
```

### Co-occurrence: two people in the same document

```sql
SELECT DISTINCT p1.efta_number
FROM pages p1
JOIN pages p2 ON p1.efta_number = p2.efta_number
WHERE p1.text_content LIKE '%Bill Clinton%'
AND p2.text_content LIKE '%Jeffrey Epstein%'
LIMIT 20;
```

### Search email metadata (concordance DB)

```sql
-- Find emails from a specific person
SELECT bates_begin, email_from, email_to, email_subject, date_sent, efta_number
FROM documents
WHERE email_from LIKE '%@gmail.com%'
AND email_subject IS NOT NULL
ORDER BY date_sent
LIMIT 20;
```

### Search images

```sql
-- Find images by description
SELECT image_name, efta_number, page_number, analysis_text
FROM images_fts
WHERE images_fts MATCH 'swimming pool'
LIMIT 10;
```

### Search audio transcripts

```sql
SELECT efta_number, substr(transcript, 1, 500), duration_secs, word_count
FROM transcripts
WHERE transcript LIKE '%lawyer%'
LIMIT 10;
```

### Look up subpoena gaps

```sql
-- Find investigative gaps
SELECT * FROM investigative_gaps ORDER BY gap_type;
```

### Cross-database: EFTA text + concordance metadata

```sql
-- Attach concordance to get both text content and file metadata
ATTACH 'concordance_complete.db' AS conc;

SELECT p.efta_number, p.page_number, substr(p.text_content, 1, 300),
       c.original_filename, c.email_from, c.email_to, c.date_sent
FROM pages p
JOIN conc.documents c ON c.efta_number = p.efta_number
WHERE p.text_content LIKE '%wire transfer%'
AND p.char_count > 50
LIMIT 20;
```

---

## EFTA Numbering System

EFTA (Electronic File Transfer Agreement) numbers are **per-page Bates stamps**, not per-document. A 10-page document starting at EFTA00000001 occupies EFTA00000001 through EFTA00000010.

### Dataset Boundaries

| Dataset | EFTA Range | Approximate Content |
|---------|-----------|-------------------|
| 1 | 00000001 – 00003158 | Prosecution case files |
| 2 | 00003159 – 00003857 | Grand jury materials |
| 3 | 00003858 – 00005586 | Court filings |
| 4 | 00005705 – 00008320 | NPA-era documents |
| 5 | 00008409 – 00008528 | Supplemental filings |
| 6 | 00008529 – 00008998 | Additional court records |
| 7 | 00009016 – 00009664 | Case correspondence |
| **8** | **00009676 – 00039023** | Native files (spreadsheets, media) |
| **9** | **00039025 – 01262781** | FBI investigation (531K docs) |
| **10** | **01262782 – 02205654** | SDNY prosecution (503K docs) |
| **11** | **02205655 – 02730264** | Defense materials (332K docs) |
| 12 | 02730265 – 02858497 | March 2026 expansion |
| 98 | — | FBI Vault (separate numbering) |
| 99 | — | House Oversight Committee |

### Opening documents on justice.gov

Every EFTA document can be viewed as a PDF:

```
https://www.justice.gov/epstein/files/DataSet%20{N}/EFTA{NUMBER}.pdf
```

**Always look up the dataset number from the database first:**

```sql
SELECT dataset FROM documents WHERE efta_number = 'EFTA00074206';
-- Returns: 9
-- URL: https://www.justice.gov/epstein/files/DataSet%209/EFTA00074206.pdf
```

Do NOT guess the dataset from the EFTA number — the boundaries have gaps and irregularities.

---

## Structured Data Files (in this repository)

| File | Contents |
|------|----------|
| `persons_registry.json` | 1,614 named persons with aliases and descriptions |
| `knowledge_graph_entities.json` | 606 entities (people, orgs, locations) |
| `knowledge_graph_relationships.json` | 2,302 entity relationships |
| `phone_numbers_enriched.csv` | Phone numbers found in documents |
| `extracted_entities_filtered.json` | Named entities extracted from corpus |
| `extracted_names_multi_doc.csv` | Names appearing across multiple documents |
| `image_catalog.csv.gz` | Catalog of all extracted images |
| `efta_dataset_mapping.csv` / `.json` | EFTA-to-dataset lookup table |
| `VERIFICATION_URLS.csv` | DOJ document URLs with verification status |
| `GRAND_JURY_SUBPOENAS.csv` | Identified grand jury subpoenas |

---

## Critical Rules

1. **Never include real victim names or identifying details.** Use pseudonyms (Jane Doe, JD#1) or EFTA references. The goal is exposing the system, not retraumatizing victims.

2. **Corpus absence ≠ non-existence.** A missing document may be under seal, in a separate case, or outside the EFTA production. Do not assume DOJ non-compliance from a missing return.

3. **Present data, not conclusions.** Show what the documents say. Let readers draw their own inferences.

4. **Verify before citing.** Always confirm EFTA numbers and dataset assignments with actual database queries before constructing URLs or making claims about document contents.

---

## Companion Repository

Investigation reports analyzing these documents: [rhowardstone/epstein-research](https://github.com/rhowardstone/epstein-research) — 165+ forensic analysis reports organized by topic, each citing specific EFTA source documents.
