# NGD Pipeline

Transform NGD (National Geographic Database) data into a clean flatfile format suitable for use with [`uk_address_matcher`](https://github.com/moj-analytical-services/uk_address_matcher).

## Overview

This package downloads, extracts, and transforms [OS NGD](https://www.ordnancesurvey.co.uk/products/os-ngd-features) address data from the OS Data Hub into a single parquet file optimized for address matching with [uk_address_matcher](https://github.com/RobinL/uk_address_matcher).

NGD data is available to many government users under the [PSGA](https://www.ordnancesurvey.co.uk/customers/public-sector/public-sector-geospatial-agreement).

## Quick Start

### 1. Prerequisites

- Create a datapackage on the OS Data Hub containing the NGD address data you need
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- OS Data Hub API key (get one at https://osdatahub.os.uk/) — only required for the download step

### 2. Setup

```bash
# Clone the repository
git clone <repo-url>
cd ngd-pipeline

# Install dependencies
uv sync

# Create environment file with your API credentials
cp .env.example .env
# Edit .env and add your OS_PROJECT_API_KEY
```

### 3. Configure

Edit `config.yaml` to customize paths if needed (defaults work out of the box):

```yaml
paths:
  work_dir: ./data
  downloads_dir: ./data/downloads
  extracted_dir: ./data/extracted
  output_dir: ./data/output

os_downloads:
  package_id: "16331"
  version_id: "103792"  # Update when new data is released

processing:
  # Number of chunks to split flatfile processing into
  # Use higher values (e.g., 10) for lower memory usage on laptops
  num_chunks: 1
```

### 4. Run

The recommended way is to edit `script.py` and then run it:

```bash
# Edit script.py to configure which steps to run, then:
uv run python script.py
```

Or import and use the pipeline programmatically:

```python
from ngd_pipeline.pipeline import run
from ngd_pipeline.settings import load_settings

settings = load_settings("config.yaml")

# Run individual steps
run("download", settings)
run("extract", settings)
run("flatfile", settings)

# Or run all steps at once
run("all", settings)
```

## Pipeline Stages

1. **Download** - Downloads NGD data from OS Data Hub
2. **Extract** - Extracts zip files to CSV and converts to parquet
3. **Flatfile** - Transforms into final address matching format with deduplication

Each stage is **idempotent** - safe to re-run. Use `force=True` to overwrite existing outputs.

## Smoke Tests

Run the smoke tests (uses the CSV fixtures in [tests/data](tests/data)):

```bash
uv run pytest tests/test_smoke.py
```

To run only the flatfile-related smoke tests:

```bash
uv run pytest tests/test_smoke.py -k "flatfile"
```

## Output Format

The final output is written to `data/output/` as one or more parquet files:

- **Single chunk mode** (`num_chunks: 1`): `ngd_for_uk_address_matcher.chunk_001_of_001.parquet`
- **Multi-chunk mode** (`num_chunks: N`): `ngd_for_uk_address_matcher.chunk_001_of_00N.parquet`, `chunk_002_of_00N.parquet`, etc.

Chunking reduces memory usage by processing UPRNs in batches. The union of all chunk files equals the single-chunk output. Use a higher `num_chunks` (e.g., 10) for laptops with limited RAM.

Each file contains:

| Column | Description |
|--------|-------------|
| `uprn` | Unique Property Reference Number |
| `full_address_with_postcode` | Complete address string including postcode |
| `filename` | Source file name |
| `feature_type` | Type: Built Address, Pre-Build Address, Royal Mail Address, Historic Address |
| `address_status` | Status: Approved, Provisional, Alternative, Historical |
| `build_status` | Build status: Built Complete, Under Construction, Prebuild, etc. |
| `postcodesource` | Source of postcode data |
| `classification_code` | Property classification code |
| `matched_address_feature_type` | For Royal Mail addresses, the matched feature type |
| `language` | Language: 'eng' (English) or 'cym' (Welsh) |

## Data Sources

The pipeline processes the following NGD address feature types:

- **Built Address** (`add_gb_builtaddress`) - Current physical addresses
- **Pre-Build Address** (`add_gb_prebuildaddress`) - Planned/future addresses
- **Historic Address** (`add_gb_historicaddress`) - Historical addresses
- **Non-Addressable Object** (`add_gb_nonaddressableobject`) - Excluded from output
- **Royal Mail Address** (`add_gb_royalmailaddress`) - PAF delivery points
- **Alternate addresses** (`*_altadd`) - Alternative address variants

Welsh language variants are extracted where available.

## Deduplication Logic

When the same UPRN+address combination appears in multiple sources, records are deduplicated using these priority rules:

**Feature Type Priority:**
1. Built Address (highest)
2. Pre-Build Address
3. Royal Mail Address
4. Historic Address
5. Non-Addressable Object (excluded)

**Address Status Priority:**
1. Approved (highest)
2. Provisional
3. Alternative
4. Historical

**Build Status Priority:**
1. Built Complete (highest)
2. Under Construction
3. Prebuild
4. Historic
5. Demolished

## Downloading Files Manually

If you prefer to download manually:
- Log into https://osdatahub.os.uk/
- Create a datapackage with NGD address features
- Download the zip file

To run the pipeline from a manual download:

1. Place the zip in the downloads directory configured in [config.yaml](config.yaml)
   - By default this is `data/downloads/`
   - The extract step looks for `*.zip` files in this folder

2. Run the pipeline starting from extract:
   - In [script.py](script.py) set `STEP = ["extract", "flatfile"]`
   - Then run: `uv run python script.py`

## OS Downloads API

To use the OS Downloads API:
1. [Set up](https://www.ordnancesurvey.co.uk/products/os-downloads-api) an API key
2. Add your key to `.env`: `OS_PROJECT_API_KEY=your_key_here`
3. Find your datapackage ID and version ID from the OS Data Hub
4. Update `config.yaml` with the package and version IDs

### API Reference

```
Base URL: https://api.os.uk/downloads/v1
Authentication: Header - key: OS_PROJECT_API_KEY

1. List versions for a datapackage:
   GET /dataPackages/{package_id}/versions
   Pick the version ID from the response (field: id)

2. List files available for download:
   GET /dataPackages/{package_id}/versions/{version_id}
   Read downloads[] for fileName, size, md5, url

3. Download data:
   Use the url from downloads[] with ?key=YOUR_API_KEY appended
```

## Related Projects

- [uk_address_matcher](https://github.com/moj-analytical-services/uk_address_matcher) - Address matching library
- [prepare_addressbase_for_address_matching](https://github.com/moj-analytical-services/prepare_addressbase_for_address_matching) - Companion repo for AddressBase Premium data

## Time taken

Assuming the raw .zip files are already downloaded, on a Macbook Pro M4:
- Extracting: 2 mintes
- Processing into output in 5 chunks: 14 minutes