# Epstein Files: Structured Data Exports

Structured data exports from the forensic analysis of the 218GB DOJ Jeffrey Epstein file release (all 12 datasets + House Oversight Estate + FBI Vault: 1,385,879 documents, 2,770,154 pages).

I have threaded through these databases into a searchable visual interface, with an AI-assistant, at https://epstein-data.com

Note the latest release, v3.0, at: https://github.com/rhowardstone/Epstein-research-data/releases/tag/v3.0

For info on the full dataset integrated on the website, please see: https://github.com/stonesalltheway1/Epstein-Pipeline

**Results repo:** [Epstein-research](https://github.com/rhowardstone/Epstein-research) — 100+ forensic investigation reports with DOJ source citations.

## What's Here

### Knowledge Graph (Curated)

| File | Records | Description |
|------|---------|-------------|
| `knowledge_graph_entities.json` | 524 | Curated entities: 489 people, 12 shell companies, 9 organizations, 7 properties, 4 aircraft, 3 locations. Each entry includes aliases, metadata (occupation, legal status, mention counts), and entity type. |
| `knowledge_graph_relationships.json` | 2,096 | Relationships between entities with types (traveled_with, associated_with, owned_by, victim_of, etc.), weights, date ranges, and source/target entity names. |

**Note on knowledge graph:** The knowledge graph was curated during the initial investigation phases and does not include NER (Named Entity Recognition) run against the full OCR corpus. It covers the most frequently-referenced and manually-verified entities. For comprehensive name extraction from the full text, see `extracted_entities_filtered.json` below or query the full databases directly.

### Person Registry

| File | Records | Description |
|------|---------|-------------|
| `persons_registry.json` | 1,614 | Unified person registry merged from 6 sources: epstein-pipeline (1,195), knowledge-graph (285), la-rana-chicana (237), Wikipedia Epstein files list (45), Bondi PEP letter Feb 2026 (19), and jmail.world (9). Each entry includes name, aliases, category (political/business/academic/staff/financial/legal/media/other), search terms, and source attribution. |

**Note:** This registry is broader than the knowledge graph — it includes every named individual identified across all investigation phases, congressional disclosures, and cross-referenced sources. Categories reflect the person's primary role relative to the Epstein case, not an accusation. Many entries (e.g., Bondi PEP letter names) appear in the files only in incidental contexts such as news clippings or tips.

### Entity Extraction (Filtered from 107K raw)

| File | Records | Description |
|------|---------|-------------|
| `extracted_entities_filtered.json` | 8,081 | Filtered entity extractions: 3,881 names (appearing in 2+ documents), 2,238 phone numbers, 1,489 amounts, 357 emails, 116 organizations. Each entry includes the EFTA document numbers where it appears. |
| `extracted_names_multi_doc.csv` | 3,881 | Names appearing in multiple EFTA documents with document counts and sample EFTA references. CSV format for easy browsing. |

**Note on quality:** The raw extraction table contains 107,422 entities, many of which are OCR artifacts from redacted/degraded documents. The filtered exports remove garbled text and require multi-document co-occurrence for names.

### Image Catalog

| File | Records | Description |
|------|---------|-------------|
| `image_catalog.csv.gz` | 38,955 | Complete image catalog (gzipped). Fields: id, image_name, efta_number, page_number, people, text_content, objects, setting, activity, notable, analyzed_at. |
| `image_catalog_notable.json.gz` | 38,864 | Images with people or notable content identified (gzipped JSON). Truncated fields for manageable size. |

### Document Summaries

| File | Records | Description |
|------|---------|-------------|
| `document_summary.csv.gz` | 519,438 | Per-document redaction summary for every EFTA document (gzipped). Fields: efta_number, total_redactions, bad_redactions, proper_redactions, has_recoverable_text, dataset_source. |

### Reconstructed Pages (High Interest)

| File | Records | Description |
|------|---------|-------------|
| `reconstructed_pages_high_interest.json.gz` | 39,588 | Pages where hidden text was recovered from under redactions (gzipped JSON). Fields include efta_number, page_number, num_fragments, reconstructed_text, interest_score, and names_found. Higher interest scores indicate more substantive recovered content. |

### EFTA-to-DOJ URL Mapping

| File | Description |
|------|-------------|
| `efta_dataset_mapping.csv` | EFTA number ranges for each of the 12 DOJ datasets, with URL templates. |
| `efta_dataset_mapping.json` | Same mapping in JSON format for programmatic use. |

**URL Pattern:** `https://www.justice.gov/epstein/files/DataSet%20{N}/EFTA{XXXXXXXX}.pdf`

| Dataset | EFTA Start | EFTA End | Note |
|---------|-----------|----------|------|
| 1 | 00000001 | 00003158 | |
| 2 | 00003159 | 00003857 | |
| 3 | 00003858 | 00005586 | |
| 4 | 00005705 | 00008320 | Gap: 5587-5704 (files exist in adjacent datasets) |
| 5 | 00008409 | 00008528 | Gap: 8321-8408 (files exist in adjacent datasets) |
| 6 | 00008529 | 00008998 | |
| 7 | 00009016 | 00009664 | Gap: 8999-9015 (files exist in adjacent datasets) |
| 8 | 00009676 | 00039023 | Largest single dataset |
| 9 | 00039025 | 01262781 | Labeled "REMOVED" but returns 200 |
| 10 | 01262782 | 02205654 | |
| 11 | 02205655 | 02730264 | |
| 12 | 02730265 | 02731783 | |

## Full Database Downloads

All source databases are available in the [v3.0 release](https://github.com/rhowardstone/Epstein-research-data/releases/tag/v3.0):

| Database | Compressed | Uncompressed | Contents |
|----------|-----------|-------------|----------|
| [full_text_corpus.db.gz](https://github.com/rhowardstone/Epstein-research-data/releases/tag/v3.0) | 2.3GB (split) | 6.3GB | 1,385,879 documents, 2,770,154 pages with full text, FTS5 search index. All 12 EFTA datasets + House Oversight Estate (DS99) + FBI Vault (DS98) + native spreadsheets + recovered EFTAs. Download both `.part_aa` and `.part_ab` and concatenate: `cat full_text_corpus.db.gz.part_* > full_text_corpus.db.gz` |
| [redaction_analysis_v2.db.gz](https://github.com/rhowardstone/Epstein-research-data/releases/download/v3.0/redaction_analysis_v2.db.gz) | 166MB | 971MB | 2.59M redaction records, 849K document summaries, 39K reconstructed pages, 107K extracted entities |
| [redaction_analysis_ds10.db.gz](https://github.com/rhowardstone/Epstein-research-data/releases/download/v3.0/redaction_analysis_ds10.db.gz) | 87MB | 532MB | Dataset 10 deep analysis (EFTA01262782-02205654) |
| [image_analysis.db.gz](https://github.com/rhowardstone/Epstein-research-data/releases/download/v3.0/image_analysis.db.gz) | 64MB | 389MB | 38,955 images with AI-generated descriptions |
| [ocr_database.db.gz](https://github.com/rhowardstone/Epstein-research-data/releases/download/v3.0/ocr_database.db.gz) | 25MB | 68MB | OCR extraction data |
| [transcripts.db.gz](https://github.com/rhowardstone/Epstein-research-data/releases/download/v3.0/transcripts.db.gz) | 1.7MB | 4.8MB | 1,628 media file entries, 435 with speech content, 189,982 words (faster-whisper large-v3) |
| [knowledge_graph.db](https://github.com/rhowardstone/Epstein-research-data/releases/download/v3.0/knowledge_graph.db) | 764KB | 764KB | 524 curated entities, 2,096 relationships (uncompressed SQLite) |

**Total:** ~2.6GB compressed / ~8.9GB uncompressed

```bash
# Download and decompress the full text corpus (split into 2 parts)
wget https://github.com/rhowardstone/Epstein-research-data/releases/download/v3.0/full_text_corpus.db.gz.part_aa
wget https://github.com/rhowardstone/Epstein-research-data/releases/download/v3.0/full_text_corpus.db.gz.part_ab
cat full_text_corpus.db.gz.part_* > full_text_corpus.db.gz
gunzip full_text_corpus.db.gz

# Search the full text corpus
sqlite3 full_text_corpus.db "SELECT efta_number, page_number, substr(text_content, 1, 200) FROM pages WHERE text_content LIKE '%Leon Black%' LIMIT 10;"

# Search redacted content
sqlite3 redaction_analysis_v2.db "SELECT efta_number, page_number, substr(hidden_text, 1, 300) FROM redactions WHERE hidden_text LIKE '%TERM%' AND length(hidden_text) > 20 LIMIT 20;"
```

## Processing Tools (Replication Pipeline)

The [`tools/`](tools/) directory contains all Python scripts used to build the databases from raw PDFs. Use these to replicate the analysis, extend it with new data, or adapt for your own pipeline.

### Core Pipeline

| Tool | Description |
|------|-------------|
| `tools/ingest_house_estate.py` | Ingests House Oversight Estate documents (Concordance format, OCR with configurable workers) |
| `tools/ingest_spreadsheets.py` | Ingests native XLS/XLSX/CSV files into full_text_corpus.db |
| `tools/transcribe_media.py` | GPU transcription of audio/video using faster-whisper large-v3 |
| `tools/prescreen_media.py` | Pre-screens media files to classify and skip surveillance footage |
| `tools/redaction_detector_v2.py` | Spatial redaction analysis: finds black rectangles, extracts underlying text |
| `tools/build_person_registry.py` | Builds unified person registry from 6 sources |
| `tools/build_knowledge_graph.py` | Constructs entity relationship graph |
| `tools/build_native_files_catalog.py` | Generates NATIVE_FILES_CATALOG.csv |

### Search & Analysis

| Tool | Description |
|------|-------------|
| `tools/person_search.py` | FTS5 cross-reference search with co-occurrence analysis and CSV export |
| `tools/congressional_scorer.py` | Scores documents by redacted-name density for congressional reading room prioritization |
| `tools/generate_gov_reports.py` | Searches corpus for current government officials |
| `tools/search_judicial.py` | Searches corpus for federal judges |
| `tools/extract_subpoena_riders.py` | Extracts and catalogs subpoena rider documents |

### Data Integrity

| Tool | Description |
|------|-------------|
| `tools/find_missing_efta.py` | Gap detection across EFTA numbering |
| `tools/recover_missing_efta.py` | Recovers missing EFTAs from DOJ server or forensic carving |
| `tools/run_post_ingestion_pipeline.sh` | Chains all post-ingestion steps (transcription, registry, catalog) |

### Data Acquisition

The raw PDFs can be obtained from:

| Source | URL | Contents |
|--------|-----|----------|
| **DOJ Epstein Library** | [justice.gov/epstein](https://www.justice.gov/epstein) | Datasets 1-12 (individual PDFs). Bulk downloads removed Feb 6, 2026. |
| **Archive.org DS9** | [full.tar.bz2](https://archive.org/download/Epstein-Dataset-9-2026-01-30/full.tar.bz2) | 103.6 GiB. Largest single dataset. |
| **Archive.org DS11** | [DataSet 11.zip](https://archive.org/download/Epstein-Data-Sets-So-Far/DataSet%2011.zip) | 25.6 GiB. 267,651 PDFs. |
| **Archive.org DS1-5** | [combined-all-epstein-files](https://archive.org/details/combined-all-epstein-files/) | First 5 datasets combined. |
| **House Oversight** | [oversight.house.gov](https://oversight.house.gov/release/oversight-committee-releases-epstein-records-provided-by-the-department-of-justice/) | Estate documents, DOJ-provided records, photo releases. |

See [COMMUNITY_PLATFORMS.md](https://github.com/rhowardstone/Epstein-research/blob/main/COMMUNITY_PLATFORMS.md) in the research repo for a full directory of 78+ community tools and mirrors.

## Integration Notes

For developers building tools on top of this data:

- EFTA numbers are the universal key. Every document in the DOJ release has one.
- The `efta_dataset_mapping` files let you resolve any EFTA number to a DOJ PDF URL.
- Entity `efta_numbers` arrays give you cross-references: "this person appears in these documents."
- Knowledge graph `weight` on relationships indicates strength of connection (higher = more documented).
- Image `image_name` format is `EFTA{number}_p{page}_i{index}_{hash}.png` — parse EFTA number and page from the filename.
- **Gap EFTAs:** The gaps between datasets (e.g., 5587-5704) are not missing — those files exist and resolve via DOJ URLs in adjacent datasets. When resolving an EFTA in a gap range, try the dataset on either side.

## License

This is analysis of public government records released under the Epstein Files Transparency Act (Public Law 118-299). The underlying documents are U.S. government works. This structured data is released into the public domain.

## Contact

Please open an issue if you find any problems! We will respond promptly.
