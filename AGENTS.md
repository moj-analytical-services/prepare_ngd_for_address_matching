# NGD Pipeline Development Guidelines

## Project Overview

This project transforms NGD (National Geographic Database) data into a clean flatfile format suitable for UK address matching.

## Repository Structure

```
├── config.example.yaml # Pipeline configuration template (copy to config.yaml)
├── script.py           # Main entry point
├── pyproject.toml      # Project metadata and dependencies
├── README.md           # User documentation
├── .env.example        # Environment variable template
├── src/
│   └── ngd_pipeline/
│       ├── __init__.py
│       ├── settings.py      # Configuration loading
│       ├── os_downloads.py  # OS Data Hub API interactions
│       ├── extract.py       # Zip extraction and CSV to parquet
│       ├── to_flatfile.py   # Transform to final format
│       └── pipeline.py      # Pipeline orchestration
└── tests/
    ├── test_smoke.py        # Integration tests
    └── data/
        └── sample_ngd_*.csv # Test fixtures
```

## Pipeline Steps

1. **download** - Download NGD data from OS Data Hub API
2. **extract** - Extract CSVs from zip, convert to parquet
3. **flatfile** - Transform to final address matching format

## Key Design Decisions

- Each step is idempotent (safe to re-run)
- Uses DuckDB for efficient data processing
- Supports chunked output for memory-constrained systems
- Welsh language variants extracted where available
- Deduplication with feature/status priority rules

## Development Commands

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Run linter
uv run ruff check src/ tests/

# Format code
uv run ruff format src/ tests/
```

## Configuration

Environment variables:
- `OS_PROJECT_API_KEY` - Required for download step

Config file (`config.yaml`):
- `paths.*` - Data directories
- `os_downloads.*` - API package/version IDs
- `processing.num_chunks` - Output file splitting
