# NGD Pipeline

Transform OS NGD (National Geographic Database) address data into parquet output suitable for `uk_address_matcher`.

## Requirements

- Python `3.12+`
- [`uv`](https://github.com/astral-sh/uv)
- OS Data Hub package and version IDs
- Network access to OS Downloads API
- Credentials in `.env`:
  - `OS_PROJECT_API_KEY`
  - `OS_PROJECT_API_SECRET`

## Install from PyPI

```bash
pip install ukam-ngd-pipeline
```

Or with `uv`:

```bash
uv tool install ukam-ngd-pipeline
```

## Run without installing (uvx)

You can run commands directly from PyPI without a permanent install:

```bash
uvx --from ukam-ngd-pipeline ukam-ngd-setup --help
uvx --from ukam-ngd-pipeline ukam-ngd-build --help
```

Example full run:

```bash
uvx --from ukam-ngd-pipeline ukam-ngd-setup --config-out config.yaml
uvx --from ukam-ngd-pipeline ukam-ngd-build --config config.yaml --step all
```

After installation, CLI commands are available directly:

```bash
ukam-ngd-setup --help
ukam-ngd-build --help
```

## Quick start

### Workflow 1: CLI

1) Generate config with the setup wizard

```bash
ukam-ngd-setup --config-out config.yaml
```

This writes `config.yaml` and, by default, `.env` placeholders if `.env` does not already exist.

2) Add real credentials

Edit `.env`:

```dotenv
OS_PROJECT_API_KEY=your_api_key_here
OS_PROJECT_API_SECRET=your_api_secret_here
```

3) Run the full pipeline

```bash
ukam-ngd-build --config config.yaml --step all
```

### Workflow 2: Python functions

```python
from ngd_pipeline import create_config_and_env, run_from_config

create_config_and_env(
  config_out="config.yaml",
  env_out=".env",
  package_id="16331",
  version_id="104444",
)

run_from_config(config_path="config.yaml", step="all")
```

<details>
<summary>Configure manually</summary>

If you prefer not to use the setup wizard, edit `config.yaml` directly.
Set `os_downloads.package_id` and `os_downloads.version_id`, then adjust `paths` and `processing` as needed.

</details>

## CLI commands and key options

| Command | Purpose | Key options |
|---|---|---|
| `ukam-ngd-setup` | Create or update pipeline config interactively | `--config-out`, `--env-out`, `--overwrite-env`, `--non-interactive`, `--package-id`, `--version-id` |
| `ukam-ngd-build` | Run pipeline stages (`download`, `extract`, `flatfile`, `all`) | `--config`, `--env-file`, `--step`, `--force`, `--list-only`, `--package-id`, `--version-id`, `--work-dir`, `--downloads-dir`, `--extracted-dir`, `--output-dir`, `--num-chunks`, `--duckdb-memory-limit`, `--parquet-compression`, `--parquet-compression-level`, `--verbose` |

### Command notes

- `--list-only` is only valid with `--step download`.
- CLI overrides take precedence over values in `config.yaml`.
- By default, `ukam-ngd-build` loads `.env` from the same directory as your config, unless `--env-file` is supplied.

## Full-run examples

### Example A: guided setup then full run

```bash
ukam-ngd-setup --config-out config.yaml
ukam-ngd-build --config config.yaml --step all
```

### Example B: non-interactive setup and tuned full run

```bash
ukam-ngd-setup \
  --non-interactive \
  --package-id 16331 \
  --version-id <version_id> \
  --config-out config.yaml

ukam-ngd-build \
  --config config.yaml \
  --step all \
  --num-chunks 20 \
  --duckdb-memory-limit 8GB
```

## Pipeline stages

1. `download` - fetch package metadata and zip files from OS Data Hub.
2. `extract` - extract CSVs from downloaded zip files and convert to parquet.
3. `flatfile` - transform and deduplicate into final output parquet file(s).

All stages are idempotent. Use `--force` to regenerate outputs.

## Output

Final outputs are parquet files in `paths.output_dir`:

- Single chunk: `ngd_for_uk_address_matcher.chunk_001_of_001.parquet`
- Multi-chunk: `ngd_for_uk_address_matcher.chunk_001_of_00N.parquet`, `...chunk_00N_of_00N.parquet`

Chunking reduces memory use by processing UPRNs in batches. The union of all chunk files equals the single-chunk output. Use a higher `num_chunks` (for example `10`) for laptops with limited RAM.

Each file contains:

| Column | Type | Description |
|--------|------|-------------|
| `uprn` | BIGINT | Unique Property Reference Number |
| `address_concat` | VARCHAR | Address string without postcode |
| `postcode` | VARCHAR | UK postcode |
| `filename` | VARCHAR | Source file name (for example `add_gb_builtaddress.parquet`) |
| `classificationcode` | VARCHAR | Property classification code (for example RD06 for residential) |
| `parentuprn` | BIGINT | Parent UPRN for hierarchical addresses |
| `rootuprn` | BIGINT | Root UPRN at the top of the hierarchy |
| `hierarchylevel` | INTEGER | Level in the address hierarchy (1 = root) |
| `floorlevel` | VARCHAR | Floor level identifier |
| `lowestfloorlevel` | DOUBLE | Lowest floor number |
| `highestfloorlevel` | DOUBLE | Highest floor number |

Metadata columns (`classificationcode`, `parentuprn`, `rootuprn`, `hierarchylevel`, `floorlevel`, `lowestfloorlevel`, `highestfloorlevel`) are enriched via UPRN lookup from core address files. This means Royal Mail addresses and alternate address records receive metadata from their corresponding Built/Historic/Pre-Build records.

## Data Sources

The pipeline processes these NGD address feature types:

- **Built Address** (`add_gb_builtaddress`) - Current physical addresses
- **Pre-Build Address** (`add_gb_prebuildaddress`) - Planned or future addresses
- **Historic Address** (`add_gb_historicaddress`) - Historical addresses
- **Non-Addressable Object** (`add_gb_nonaddressableobject`) - Excluded from output
- **Royal Mail Address** (`add_gb_royalmailaddress`) - PAF delivery points
- **Alternate addresses** (`*_altadd`) - Alternative address variants

Welsh language variants are extracted where available and appear as separate rows in the output.

## Deduplication

When the same UPRN and address combination appears in multiple sources, records are deduplicated using these internal priority rules:

**Feature type priority:**
1. Built Address (highest)
2. Pre-Build Address
3. Royal Mail Address
4. Historic Address
5. Non-Addressable Object (excluded)

**Address status priority:**
1. Approved (highest)
2. Provisional
3. Alternative
4. Historical

**Build status priority:**
1. Built Complete (highest)
2. Under Construction
3. Prebuild
4. Historic
5. Demolished

## Manual Download

If you prefer to download manually:
- Sign in to https://osdatahub.os.uk/
- Create a datapackage with NGD address features
- Download the zip file

To run the pipeline from a manual download:

1. Place the zip in the downloads directory configured in `config.yaml`
   - By default this is `data/downloads/`
   - The extract step looks for `*.zip` files in this folder

2. Run the pipeline starting from extract:

```bash
ukam-ngd-build --config config.yaml --step extract
ukam-ngd-build --config config.yaml --step flatfile
```

## OS Downloads API

To use the OS Downloads API:
1. [Set up](https://www.ordnancesurvey.co.uk/products/os-downloads-api) an API key
2. Add your key to `.env`: `OS_PROJECT_API_KEY=your_key_here`
3. Find your datapackage ID and version ID from the OS Data Hub
4. Update `config.yaml` with the package and version IDs

### API reference

```text
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

## Config shape (`config.yaml`)

```yaml
paths:
  work_dir: ./data
  downloads_dir: ./data/downloads
  extracted_dir: ./data/extracted
  output_dir: ./data/output

os_downloads:
  package_id: "<your_package_id>"
  version_id: "<your_version_id>"
  connect_timeout_seconds: 30
  read_timeout_seconds: 300

processing:
  parquet_compression: zstd
  parquet_compression_level: 9
  num_chunks: 1
  # duckdb_memory_limit: "8GB"
```

## Smoke test

```bash
pytest tests/test_smoke.py
```

## Related projects

- [uk_address_matcher](https://github.com/moj-analytical-services/uk_address_matcher)
- [prepare_addressbase_for_address_matching](https://github.com/moj-analytical-services/prepare_addressbase_for_address_matching)