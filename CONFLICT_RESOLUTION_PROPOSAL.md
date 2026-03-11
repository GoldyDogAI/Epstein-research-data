# README Conflict Resolution Proposal

**PR:** [rhowardstone/Epstein-research-data#2](https://github.com/rhowardstone/Epstein-research-data/pull/2) — "add table of contents, other readme improvements"
**Status:** Merge conflict (`mergeable_state: dirty`)

## Summary

PR #2 from GoldyDogAI adds structural improvements to the README (Table of Contents, YouTube video link, expanded documentation sections, and more tool listings). Since the PR was opened, the upstream repository (rhowardstone) has made significant content updates to the same README—updated statistics, new release versions (v5.0/v5.1), additional databases, and revised section content. These parallel changes create merge conflicts in several areas.

This document catalogs each conflict and proposes a resolution strategy.

---

## Conflict Areas

### 1. Introduction / Header

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| YouTube thumbnail | Added ✅ | Not present | **Keep PR addition** — valuable for discoverability |
| Document count | 1,385,879 docs / 2,770,154 pages | 1,385,916 docs / 2,771,231 pages | **Accept upstream** — reflects DS12 expansion |
| CLI/desktop install links | Not present | Added (Claude-powered investigator) | **Accept upstream** — new feature |
| Release version | v4.0 | v5.1 | **Accept upstream** — latest release |
| Results repo count | 100+ reports | 165+ reports | **Accept upstream** — updated count |

### 2. Table of Contents

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Table of Contents section | Added ✅ | Not present | **Keep PR addition** — improves navigation |

The ToC should be updated to include any new sections from upstream (e.g., adjusted anchor links).

### 3. Knowledge Graph Section

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Entity count | 524 (detailed breakdown) | 606 (summary description) | **Accept upstream count (606)** with PR's detailed breakdown style where possible |
| Relationship count | 2,096 | 2,302 | **Accept upstream** |

### 4. Person Registry Section

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Source count | "merged from 6 sources" | "merged from 9 sources" (adds corpus-investigation, khanna-massie-2026, doj-release-2026) | **Accept upstream** — more complete |

### 5. Entity Extraction Section

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Filtered entity count | 8,081 | 8,085 | **Accept upstream** — updated count |

### 6. EFTA Mapping Table

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Dataset 12 EFTA End | 02731783 | 02858497 | **Accept upstream** — DS12 expansion |

### 7. DOJ Document Removal Audit Section

Both the PR and upstream added this section (it was absent from the base). Differences:

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| FLAGGED_documents_details records | 96,112 | 102,223 | **Accept upstream** — updated count |
| `sample_verify.py` entry | Listed | Removed | **Accept upstream** — tool removed from repo |
| Link to full report | Not present | Added | **Accept upstream** — adds useful cross-reference |

### 8. Alteration Analysis Section

Both sides added this section. Differences:

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Alteration database reference | Not present | Added (`alteration_results.db.gz` in v5.1) | **Accept upstream** — additional context |
| Link to full report | Not present | Added | **Accept upstream** |

### 9. Recovered Corrupted PDFs Section

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Description wording | "forensic carving" | "forensic byte-level carving" | **Accept upstream** — more precise |
| EFTA list formatting | Bullet points (`- EFTA...`) | Inline backtick list | **Accept upstream** — more compact |
| Link to full report | Not present | Added | **Accept upstream** |

### 10. Full Database Downloads Section (Major Conflict)

This is the largest conflict area. Both sides changed this section significantly from the base (which referenced v3.0).

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Release structure | Single v4.0 release | Split across v5.0/v5.1/v4.0 | **Accept upstream** — current release structure |
| Table columns | 4 columns (no Release) | 5 columns (includes Release) | **Accept upstream** |
| New databases | None added | concordance_complete.db.gz, alteration_results.db.gz, communications.db.gz, prosecutorial_query_graph.db | **Accept upstream** — new databases available |
| Download URLs in code block | v4.0 URLs | v5.0 URLs | **Accept upstream** |
| Total size | ~2.6GB / ~8.9GB | ~3.0GB / ~17GB | **Accept upstream** |
| KG db description | Basic | Updated with note about updated JSON files | **Accept upstream** |

### 11. Processing Tools Section

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Auto-detect data directory note | Not present | Added | **Accept upstream** |
| Additional Core Pipeline tools (bulk_ocr, document_classifier, etc.) | Added ✅ | Not present | **Keep PR additions** — documents existing tools |
| Additional Search & Analysis tools (name_search, search_all_judges, etc.) | Added ✅ | Not present | **Keep PR additions** — documents existing tools |
| Additional Data Integrity tools (redaction_detector variants, etc.) | Added ✅ | Not present | **Keep PR additions** — documents existing tools |
| PQG Pipeline subsection | Added ✅ | Not present | **Keep PR additions** — documents existing pipeline |
| Multi-Agent Analysis Pipeline subsection | Added ✅ | Not present | **Keep PR additions** — documents existing pipeline |

### 12. Investigation Reports & Reference Files Section

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Section added | ✅ | Not present (content existed in base without this section) | **Keep PR addition** — better organization |

### 13. Additional Data Files Section

| Element | PR Version | Upstream Version | Resolution |
|---------|-----------|-----------------|------------|
| Section added | ✅ | Not present | **Keep PR addition** — better organization |

---

## Resolution Strategy

The recommended approach is to **rebase or merge the PR branch onto the current upstream main**, accepting upstream changes for all data/statistics/release updates while preserving the PR's structural improvements:

1. **Accept all upstream data updates** — document counts, entity counts, release versions, new databases, EFTA mapping corrections, report links
2. **Keep all PR structural improvements** — YouTube thumbnail, Table of Contents, expanded tool documentation, new organizational sections (Investigation Reports, Additional Data Files)
3. **Merge both sides' new sections** — Where both added the same section (DOJ Audit, Alteration Analysis, Recovered PDFs), use upstream's content (more current) with PR's organizational style

### Implementation

The resolved README should combine:
- PR's YouTube video link at top
- Upstream's updated introduction text (document counts, release version, install links)
- PR's Table of Contents (updated to match final section structure)
- Upstream's updated record counts throughout
- Both sides' new sections, preferring upstream content where different
- PR's expanded tool documentation
- Upstream's auto-detect data directory note
- Upstream's Full Database Downloads restructuring (v5.0/v5.1/v4.0 split)

**Note on Knowledge Graph entity descriptions:** The upstream changed the entity description from a detailed per-type breakdown (e.g., "489 people, 12 shell companies, ...") to generic categories ("people, shell companies, organizations, ...") because the entity count increased from 524 to 606 and the per-type numbers changed. Without knowing the exact new breakdown, we adopt the upstream's generic description to avoid publishing inaccurate sub-counts.

## Status

This proposal has been implemented in the accompanying README.md changes in this branch.
