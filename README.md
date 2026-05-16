# Epstein Files: Structured Data Exports

Structured data exports from the forensic analysis of the DOJ Jeffrey Epstein file release under the [Epstein Files Transparency Act](https://www.congress.gov/bill/119th-congress/house-bill/42) (Public Law 119-38). All 12 DOJ datasets plus House Oversight Estate and FBI Vault materials: **1.4M+ documents, 2.9M+ pages**.

**[epstein-data.com](https://epstein-data.com)** — searchable interface with full-text search, AI research assistant, document viewer, redaction analysis, deposition transcripts, geographic heatmap, and more.

**[Investigation Reports](https://epstein-data.com/reports/)** — 180+ forensic reports with DOJ source citations, organized by topic.

**[Desktop Install](https://epstein-data.com/investigate)** — run the full Claude-powered investigator locally with all databases. No tech skills required. ([CLI version](https://epstein-data.com/investigate_cli))

---

## Table of Contents

- [What's Here](#whats-here)
  - [Knowledge Graph](#knowledge-graph-curated)
  - [Person Registry](#person-registry)
  - [Entity Extraction](#entity-extraction-filtered-from-107k-raw)
  - [Image Catalog](#image-catalog)
  - [Document Summaries](#document-summaries)
  - [Reconstructed Pages](#reconstructed-pages-high-interest)
  - [EFTA-to-DOJ URL Mapping](#efta-to-doj-url-mapping)
- [DOJ Document Removal Audit](#doj-document-removal-audit)
- [Alteration Analysis](#alteration-analysis)
- [Recovered Corrupted PDFs](#recovered-corrupted-pdfs)
- [Investigation Reports & Reference Files](#investigation-reports--reference-files)
- [Additional Data Files](#additional-data-files)
- [Full Database Downloads](#full-database-downloads)
- [Processing Tools (Replication Pipeline)](#processing-tools-replication-pipeline)
- [Integration Notes](#integration-notes)
- [License](#license)
- [Contact](#contact)

## What's Here
## Investigation Reports

180+ reports across 18 categories. Each report cites specific EFTA documents you can verify at [epstein-data.com](https://epstein-data.com). Browse all reports in the [grid view](https://epstein-data.com/reports/).

**[Executive Summaries](https://epstein-data.com/reports/#cat-overview)** — [Final Investigation Report](https://epstein-data.com/reports/overview/FINAL_INVESTIGATION_REPORT.html) | [Master Report](https://epstein-data.com/reports/overview/MASTER_REPORT.html) | [Phase 4 Briefing Kit](https://epstein-data.com/reports/overview/PHASE4_BRIEFING_KIT.html)

**[Individual Investigations](https://epstein-data.com/reports/#cat-individuals)** — [Trump](https://epstein-data.com/reports/individuals/DONALD_TRUMP_INVESTIGATION.html) | [Rothschild](https://epstein-data.com/reports/individuals/ROTHSCHILD_INVESTIGATION.html) | [Pseudonym & Codename Registry](https://epstein-data.com/reports/individuals/PSEUDONYM_CODENAME_REGISTRY.html)

**[Institutional Failures](https://epstein-data.com/reports/#cat-institutional)** — [DOJ Document Removal Audit](https://epstein-data.com/reports/institutional/DOJ_DOCUMENT_REMOVAL_AUDIT.html) | [DS12 Expansion Analysis](https://epstein-data.com/reports/institutional/DS12_EXPANSION_ANALYSIS.html) | [Alteration Forensics](https://epstein-data.com/reports/institutional/DOJ_DOCUMENT_ALTERATION_FORENSICS.html)

**[Congressional Briefings](https://epstein-data.com/reports/#cat-congressional)** — [Subpoena Guide](https://epstein-data.com/reports/congressional/CONGRESSIONAL_SUBPOENA_GUIDE.html) | [Witness Brief: Indyke](https://epstein-data.com/reports/congressional/WITNESS_BRIEF_INDYKE.html) | [Deposition Analysis: Indyke](https://epstein-data.com/reports/congressional/DEPOSITION_ANALYSIS_INDYKE.html)

**[Financial Forensics](https://epstein-data.com/reports/#cat-financial)** — [DiIorio / Apollo Whistleblower](https://epstein-data.com/reports/financial/DILORIO_APOLLO_WHISTLEBLOWER.html) | [Shell Entity Map](https://epstein-data.com/reports/financial/SHELL_ENTITY_MAP.html) | [Leon Black Prosecution Failure](https://epstein-data.com/reports/individuals/LEON_BLACK_PROSECUTION_FAILURE.html)

**[Evidence & Device Forensics](https://epstein-data.com/reports/#cat-evidence)** — [Evidence Compilation](https://epstein-data.com/reports/evidence/EVIDENCE_COMPILATION.html) | [Device Forensics](https://epstein-data.com/reports/evidence/DEVICE_FORENSICS_COMPLETE.html) | [MCC Inmate Witness Interviews](https://epstein-data.com/reports/evidence/MCC_INMATE_WITNESS_INTERVIEWS.html)

**[Intelligence Networks](https://epstein-data.com/reports/#cat-intelligence)** — [FBI Intelligence Investigations](https://epstein-data.com/reports/intelligence/FBI_INTELLIGENCE_INVESTIGATIONS.html) | [Israel Deep Dive](https://epstein-data.com/reports/intelligence/ISRAEL_DEEP_DIVE_V2.html)

**[Social Network Analysis](https://epstein-data.com/reports/#cat-social-networks)** — [Geffen](https://epstein-data.com/reports/social-networks/GEFFEN_INVESTIGATION.html) | [Kotick / Activision](https://epstein-data.com/reports/social-networks/KOTICK_ACTIVISION.html) | [Peggy Siegal Pipeline](https://epstein-data.com/reports/social-networks/PEGGY_SIEGAL_PIPELINE.html)

**[Victim Census & Routes](https://epstein-data.com/reports/#cat-victims)** | **[Art Market](https://epstein-data.com/reports/#cat-art)** | **[Science Network](https://epstein-data.com/reports/#cat-scientists)** | **[Government Officials](https://epstein-data.com/reports/#cat-government-officials)** | **[Dataset Analysis](https://epstein-data.com/reports/#cat-raw-dataset-analysis)** | **[Methodology](https://epstein-data.com/reports/#cat-methodology)** | **[Internet Theories](https://epstein-data.com/reports/#cat-internet-theories)** | **[Prosecutorial Query Graph](https://epstein-data.com/reports/#cat-pqg_lines_of_investigation)**

---

## Quick Start

All databases are in a single release: **[v5.2 — Complete Database Collection](https://github.com/rhowardstone/Epstein-research-data/releases/tag/v5.2)** (PII-redacted).

```bash
# Create the data directory (required for search tools)
mkdir -p data

# Download the split corpus parts (~2.3 GB total)
wget https://github.com/rhowardstone/Epstein-research-data/releases/download/v5.2/ftc_clean_part_aa
wget https://github.com/rhowardstone/Epstein-research-data/releases/download/v5.2/ftc_clean_part_ab
wget https://github.com/rhowardstone/Epstein-research-data/releases/download/v5.2/ftc_clean_part_ac

# Reassemble, decompress, and move to data/
cat ftc_clean_part_* > full_text_corpus.db.gz
gunzip full_text_corpus.db.gz
mv full_text_corpus.db data/

# Search the full text corpus
sqlite3 data/full_text_corpus.db "SELECT efta_number, page_number, substr(text_content, 1, 200) FROM pages WHERE text_content LIKE '%Leon Black%' LIMIT 10;"

# FTS5 full-text search (faster)
sqlite3 data/full_text_corpus.db "SELECT p.efta_number, p.page_number, substr(p.text_content, 1, 200) FROM pages_fts fts JOIN pages p ON p.rowid = fts.rowid WHERE pages_fts MATCH 'shell company' LIMIT 10;"
```

---

## Databases

All databases available in the [v5.2 release](https://github.com/rhowardstone/Epstein-research-data/releases/tag/v5.2).

| Database | Compressed | Description |
|----------|-----------|-------------|
| `full_text_corpus.db` (after reassembly) | 2.3 GB (split) | Master text database (split into `ftc_clean_part_aa/ab/ac`). 1.4M docs, 2.9M pages with full text and FTS5 search. DS1-12 + House Oversight (DS99) + FBI Vault (DS98). |
| `concordance_complete.db` | 137 MB | Cross-reference with email threads, folder inventory, production metadata. |
| `alteration_results.db` | 183 MB | 212K change units with diff text, pixel-diff, LLM classification of post-release DOJ document modifications. |
| `redaction_analysis_v2.db` | 166 MB | 2.6M redaction boxes, 850K doc summaries, 39K reconstructed pages, 107K extracted entities. |
| `redaction_analysis_ds10.db` | 87 MB | Dataset 10 deep redaction analysis. |
| `image_analysis.db` | 64 MB | 38K+ extracted images with AI-generated descriptions. |
| `secondary_stamps.db` | — | 835K documents mapped to parallel Bates numbering systems (R1, JPM-SDNY, DB-SDNY, SDNY-GM). |
| `deposition_transcripts.db` | — | Whisper-transcribed depositions with speaker diarization (Wexner, Bill Clinton, Hillary Clinton, and others). |
| `document_status.db` | — | Document availability tracking across DOJ website. |
| `handwriting_transcriptions.db` | — | Handwritten document transcriptions. |
| `native_files.db` | — | Metadata for 3,800+ non-PDF native files (XLSX, media, etc.). |
| `transcripts.db` | 1.7 MB | 1,600+ media files, 435 with speech, 190K words (faster-whisper large-v3). |
| `ocr_database.db` | 25 MB | Tesseract OCR results for degraded documents. |
| `spreadsheet_corpus.db` | — | Native spreadsheet data extracted from Dataset 8. |
| `prosecutorial_query_graph.db` | 2.5 MB | Subpoena analysis: riders, returns, clause fulfillment, investigative gaps. |
| `knowledge_graph.db` | 764 KB | 524 curated entities, 2,096 relationships. |
| `communications.db` | — | Email thread analysis. |
| `persons_registry.json` | — | 1,600+ persons merged from 9 sources with aliases and categories. |

---

## Data Exports

### Knowledge Graph

| File | Records | Description |
|------|---------|-------------|
| `knowledge_graph_entities.json` | 606 | Curated entities: people, shell companies, organizations, properties, aircraft, locations. |
| `knowledge_graph_relationships.json` | 2,302 | Typed relationships with weights, date ranges, and source/target entity names. |

### Person Registry

| File | Records | Description |
|------|---------|-------------|
| `persons_registry.json` | 1,614 | Unified person registry from 9 sources. Includes name, aliases, category, search terms. |

### Entity Extraction

| File | Records | Description |
|------|---------|-------------|
| `extracted_entities_filtered.json` | 8,085 | Filtered entities: 3,881 names (2+ documents), 2,238 phone numbers, 1,489 amounts, 357 emails, 116 organizations. |
| `extracted_names_multi_doc.csv` | 3,881 | Names appearing in multiple documents with counts and sample EFTA references. |

### Image Catalog

| File | Records | Description |
|------|---------|-------------|
| `image_catalog.csv.gz` | 38,955 | Complete image catalog with AI descriptions, people identified, objects, settings. |

### Document Summaries

| File | Records | Description |
|------|---------|-------------|
| `document_summary.csv.gz` | 519,438 | Per-document redaction summary for every EFTA document. |

### Reconstructed Pages

| File | Records | Description |
|------|---------|-------------|
| `reconstructed_pages_high_interest.json.gz` | 39,588 | Pages where hidden text was recovered from under redactions. |

---

## DOJ Document Audit

The [`doj_audit/`](doj_audit/) directory documents a comprehensive audit of the DOJ Epstein Library conducted in February-March 2026.

The audit identified ~64,000 documents that were temporarily removed from the DOJ website after the initial public release, as well as ~24,000 documents with post-release size changes suggesting modification. **As of March 2026, previously removed documents have been restored to justice.gov.** The audit data is preserved for the historical record.

| File | Records | Description |
|------|---------|-------------|
| `doj_audit/CONFIRMED_REMOVED.csv` | 67,784 | Documents confirmed removed at time of audit (since restored). |
| `doj_audit/FLAGGED_documents.csv` | 96,112 | All flagged documents with DOJ URLs and dataset info. |
| `doj_audit/FLAGGED_documents_details.csv` | 102,223 | Detailed metadata including status, category, document type, priority score. |
| `doj_audit/SIZE_MISMATCHES.csv` | 23,989 | Documents with file size changes between original and current DOJ versions. |

Full reports: [Document Removal Audit](https://epstein-data.com/reports/institutional/DOJ_DOCUMENT_REMOVAL_AUDIT.html) | [Alteration Forensics](https://epstein-data.com/reports/institutional/DOJ_DOCUMENT_ALTERATION_FORENSICS.html)

### Alteration Analysis

The [`alteration_analysis/`](alteration_analysis/) directory contains analysis of documents where content was modified between versions.

| File | Records | Description |
|------|---------|-------------|
| `alteration_analysis/classified_alterations.csv` | 21,803 | Classified alteration types with sensitivity ratings and LLM reasoning. |
| `alteration_analysis/removed_entities_export.csv` | 146,209 | Entities (names, accounts, phone numbers) removed between document versions. |

---

## EFTA Numbering and DOJ URLs

EFTA numbers are assigned **per page**, not per document. A 10-page document consumes 10 consecutive EFTA numbers.

**URL pattern:** `https://www.justice.gov/epstein/files/DataSet%20{N}/EFTA{XXXXXXXX}.pdf`

## Investigation Reports & Reference Files

| File | Description |
|------|-------------|
| [`FRENCH_CONNECTION_INVESTIGATION.md`](FRENCH_CONNECTION_INVESTIGATION.md) | Detailed investigation report on Jeffrey Epstein's operations in France, covering the modeling pipeline, 22 Avenue Foch, Jack Lang, and financial infrastructure — with EFTA source citations throughout. |
| [`COUNTING_METHODOLOGY_ANALYSIS.txt`](COUNTING_METHODOLOGY_ANALYSIS.txt) | Comprehensive counting and gap analysis of the full corpus, explaining EFTA per-page numbering, dataset coverage, inter-dataset gaps, and the 730K-page gap to the DOJ's "3.5 million pages" claim. |
| [`MISSING_EFTA_INVESTIGATION.txt`](MISSING_EFTA_INVESTIGATION.txt) | Investigation and verification of EFTA numbering completeness, correcting earlier analysis that mistakenly reported 49% of documents withheld. |
| [`QUICK_REFERENCE_COUNTS.txt`](QUICK_REFERENCE_COUNTS.txt) | Quick-reference summary of corpus statistics: document counts, page counts, EFTA coverage, database sizes, and key takeaways. |

## Additional Data Files

| File | Records | Description |
|------|---------|-------------|
| `la-rana-chicana-list_2-11-26_10am.csv` | 265 | Named individuals list from the La Rana Chicana source with last name, first name, description, and involvement details. Used as one of 6 sources for the person registry. |
| `NON_EFTA_VERIFICATION_URLS.csv` | — | Verification URLs for non-EFTA documents (FBI Vault, House Oversight) with document IDs, datasets, page counts, and archive.org links. |

## Full Database Downloads
| Dataset | EFTA Start | EFTA End | Notes |
|---------|-----------|----------|-------|
| 1 | 00000001 | 00003158 | |
| 2 | 00003159 | 00003857 | |
| 3 | 00003858 | 00005586 | |
| 4 | 00005705 | 00008320 | |
| 5 | 00008409 | 00008528 | |
| 6 | 00008529 | 00008998 | |
| 7 | 00009016 | 00009664 | |
| 8 | 00009676 | 00039023 | |
| 9 | 00039025 | 01262781 | Largest (103 GB) |
| 10 | 01262782 | 02205654 | |
| 11 | 02205655 | 02730264 | |
| 12 | 02730265 | 02858497 | Post-release expansion |

No gaps between datasets 1-11 — every apparent gap is a multi-page document at a boundary. Dataset 12 has internal gaps (~100K unassigned numbers).

Also available as `efta_dataset_mapping.csv` and `efta_dataset_mapping.json`.

---

## Processing Tools

The `tools/` directory contains 34 Python scripts used to build all databases from raw PDFs. Use these to replicate or extend the analysis.

All tools auto-detect the data directory via `EPSTEIN_DATA_DIR` environment variable, or look relative to the script location.

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
| `tools/bulk_ocr.py` | Bulk OCR of all extracted images into searchable SQLite database |
| `tools/bulk_ocr_fast.py` | Bulk OCR with maximum parallelism |
| `tools/document_classifier.py` | Classifies documents by type based on OCR content |
| `tools/populate_evidence_db.py` | Populates the evidence database from processed outputs |

### Search & Analysis

| Tool | Description |
|------|-------------|
| `tools/person_search.py` | FTS5 cross-reference search with co-occurrence analysis and CSV export |
| `tools/name_search.py` | Searches for known names/entities across all OCR'd documents |
| `tools/congressional_scorer.py` | Scores documents by redacted-name density for congressional reading room prioritization |
| `tools/generate_gov_reports.py` | Searches corpus for current government officials |
| `tools/search_judicial.py` | Searches corpus for federal judges |
| `tools/search_all_judges.py` | Searches full_text_corpus.db for all Article III federal judges from FJC database |
| `tools/search_gov_officials.py` | Searches full_text_corpus.db for government officials from CSV and executive branch lists |
| `tools/extract_subpoena_riders.py` | Extracts and catalogs subpoena rider documents |
| `tools/knowledge_graph.py` | Builds and queries the knowledge graph from extracted entities |
| `tools/update_kg_with_ds10.py` | Updates the knowledge graph with Dataset 10 analysis results |

### Data Integrity

| Tool | Description |
|------|-------------|
| `tools/find_missing_efta.py` | Gap detection across EFTA numbering |
| `tools/recover_missing_efta.py` | Recovers missing EFTAs from DOJ server or forensic carving |
| `tools/redaction_detector.py` | Original redaction detector for black rectangles in images |
| `tools/redaction_detector_ds10.py` | Redaction detector specialized for Dataset 10 deep analysis |
| `tools/redaction_detector_incremental.py` | Incremental redaction detector for processing new documents |
| `tools/run_post_ingestion_pipeline.sh` | Chains all post-ingestion steps (transcription, registry, catalog) |
| `tools/run_dependent_analysis.sh` | Waits for OCR to process enough documents, then runs dependent analysis |
| `tools/check_status.sh` | Reports OCR and redaction processing status |

### PQG Pipeline (Prosecutorial Query Generation)

A 6-step pipeline for concordance analysis, subpoena rider decomposition, and fulfillment scoring:

| Tool | Description |
|------|-------------|
| `tools/pqg_00_extract_concordance.py` | Parses all concordance files (DAT + OPT) across all datasets into a unified database |
| `tools/pqg_01_decompose_riders.py` | Decomposes subpoena rider documents into individual requests |
| `tools/pqg_02_match_returns.py` | Matches document returns to subpoena requests |
| `tools/pqg_03_score_fulfillment.py` | Scores subpoena fulfillment completeness |
| `tools/pqg_04_build_graph.py` | Builds relationship graph from matched subpoena data |
| `tools/pqg_05_report.py` | Generates final PQG analysis reports |

### Multi-Agent Analysis Pipeline

The [`tools/pipeline/`](tools/pipeline/) directory contains a multi-agent document analysis system:

| Tool | Description |
|------|-------------|
| `tools/pipeline/agent_coordinator.py` | Coordinates parallel sub-agents for deep document reading, person identification, pattern detection, and cross-referencing |
| `tools/pipeline/batch_processor.py` | Batch processing of documents through the analysis pipeline |
| `tools/pipeline/entity_registry.py` | Entity registry for tracking persons and relationships across documents |
| `tools/pipeline/extraction_only.py` | Extraction-only mode for entity and relationship extraction |
| `tools/pipeline/victim_classifier.py` | Classifies identified persons as victims, perpetrators, or associates based on document context |

### Recovered Corrupted PDFs

The [`recovered_corrupted_pdfs/`](recovered_corrupted_pdfs/) directory contains text forensically recovered from 5 corrupted PDFs through byte-level carving: `EFTA00593870`, `EFTA00597207`, `EFTA00645624`, `EFTA01175426`, `EFTA01220934`. See: [Corrupted PDF Forensics](https://epstein-data.com/reports/evidence/CORRUPTED_PDF_FORENSICS.html)

---

## Raw Data Sources

| Source | Contents |
|--------|----------|
| [DOJ Epstein Library](https://www.justice.gov/epstein) | Datasets 1-12 (individual PDFs) |
| [Archive.org DS9](https://archive.org/download/Epstein-Dataset-9-2026-01-30/full.tar.bz2) | 103.6 GB, largest single dataset |
| [Archive.org DS11](https://archive.org/download/Epstein-Data-Sets-So-Far/DataSet%2011.zip) | 25.6 GB, 267K PDFs |
| [Archive.org DS1-5](https://archive.org/details/combined-all-epstein-files/) | First 5 datasets combined |
| [House Oversight](https://oversight.house.gov/release/oversight-committee-releases-epstein-records-provided-by-the-department-of-justice/) | Estate documents, DOJ-provided records |

See [COMMUNITY_PLATFORMS.md](https://github.com/rhowardstone/epstein-research/blob/main/COMMUNITY_PLATFORMS.md) for a directory of 79+ community tools, mirrors, and analysis platforms.

---

## Integration Notes

- **EFTA numbers are the universal key.** Every document in the DOJ release has one.
- **Join on `efta_number`**, not `document_id`, when linking across databases.
- **FTS5 search**: `SELECT ... FROM pages_fts WHERE pages_fts MATCH 'term'` — join on `rowid`.
- Entity `efta_numbers` arrays give cross-references: "this entity appears in these documents."
- Image filename format: `EFTA{number}_p{page}_i{index}_{hash}.png`.

## License

Analysis of public government records released under the Epstein Files Transparency Act (Public Law 119-38). Underlying documents are U.S. government works. Structured data released under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).

## Contact

Questions or corrections? Use the feedback form at [epstein-data.com](https://epstein-data.com).


## Defective Redactions Analysis

**NEW:** [Defective Redactions in DOJ Court Filings](defective_redactions/) — Recovery of hidden text from 12,000+ court filing PDFs with faulty redactions (Dec 2025 - Feb 2026).

- **[Quick Start Guide](defective_redactions/README.md)** — Copy/paste technique, download tools, case inventory
- **[Technical Analysis](defective_redactions/docs/technical_report.md)** — Full vulnerability assessment and methodology  
- **[Public Guide](defective_redactions/docs/public_guide.md)** — Non-technical explanation for journalists and researchers

**Key Findings:** .3M+ property tax details, 6M+ in undisclosed loans, previously unknown financial entities, evidence destruction instructions — all recoverable from original Wayback Machine archives.
