# UKAM OS Builder

Build OS address data for `uk_address_matcher` from either NGD (National Geographic Database) or ABP (AddressBase Premium).

## Requirements

- Python `3.10+`
- OS Data Hub package and version IDs
- Network access to OS Downloads API
- Credentials in `.env`:
  - `OS_PROJECT_API_KEY`
  - `OS_PROJECT_API_SECRET`

## Install from PyPI

```bash
pip install ukam-os-builder
```

Or with `uv`:

```bash
uv tool install ukam-os-builder
```

## Run without installing (uvx)

You can run commands directly from PyPI without a permanent install:

```bash
uvx --from ukam-os-builder ukam-os-setup --help
uvx --from ukam-os-builder ukam-os-build --help
```

Example full run:

```bash
uvx --from ukam-os-builder ukam-os-setup --config-out config.yaml
uvx --from ukam-os-builder ukam-os-build --config config.yaml
```

After installation, CLI commands are available directly:

```bash
ukam-os-setup --help
ukam-os-build --help
```

## Quick start

### Workflow 1: CLI

1) Generate config with the setup wizard

```bash
ukam-os-setup --config-out config.yaml
```

This writes `config.yaml` and, by default, `.env` placeholders if `.env` does not already exist. The setup flow asks which source to use (`ngd` or `abp`) and stores it in `config.yaml`.

2) Add real credentials

Edit `.env`:

```dotenv
OS_PROJECT_API_KEY=your_api_key_here
OS_PROJECT_API_SECRET=your_api_secret_here
```

3) Run the full pipeline

```bash
ukam-os-build --config config.yaml
```

`--config` is the standard argument for selecting your configuration file.

### Workflow 2: Python functions

```python
from ukam_os_builder import create_config_and_env, run_from_config

create_config_and_env(
  config_out="config.yaml",
  env_out=".env",
  source="ngd",
  package_id="16331",
  version_id="104444",
)

run_from_config(config_path="config.yaml", step="all")
```

### Inspect output variants

Use the reusable inspection function to find high-variant UPRNs in output parquet files:

```python
from ukam_os_builder import inspect_flatfile_variants

result = inspect_flatfile_variants(config_path="config.yaml", top_offset=0, show=True)
print(result["selected_uprn"], result["variant_count"])
```

You can also import directly from the inspection module:

```python
from ukam_os_builder.os_builder.inspect_results import inspect_flatfile_variants

result = inspect_flatfile_variants(config_path="config.yaml", top_offset=0, show=True)
```

<details>
<summary>Configure manually</summary>

If you prefer not to use the setup wizard, edit `config.yaml` directly.
Set `source.type`, `os_downloads.package_id`, and `os_downloads.version_id`, then adjust `paths` and `processing` as needed.

</details>

## CLI commands and key options

| Command | Purpose | Key options |
|---|---|---|
| `ukam-os-setup` | Create or update pipeline config interactively | `--config-out`, `--env-out`, `--overwrite-env`, `--non-interactive`, `--source`, `--package-id`, `--version-id` |
| `ukam-os-build` | Run pipeline stages (`download`, `extract`, `split`, `flatfile`, `all`) | `--config`, `--source`, `--env-file`, `--step`, `--overwrite`, `--list-only`, `--package-id`, `--version-id`, `--work-dir`, `--downloads-dir`, `--extracted-dir`, `--output-dir`, `--num-chunks`, `--duckdb-memory-limit`, `--parquet-compression`, `--parquet-compression-level`, `--verbose` |

### Command notes

- `step` only supports `download` and `all` to simplify usage. Use `--overwrite` to re-run a step with the same parameters.
- CLI overrides take precedence over values in `config.yaml`.
- By default, `ukam-os-build` loads `.env` from the same directory as your config, unless `--env-file` is supplied.

## Full-run examples

### Example A: guided setup then full run

```bash
ukam-os-setup --config-out config.yaml
ukam-os-build --config config.yaml
```

### Example B: non-interactive setup and tuned full run

```bash
ukam-os-setup --source abp --config-out config.yaml --non-interactive --package-id <package_id> --version-id <version_id>
ukam-os-build --config config.yaml
```

## Pipeline stages

1. `download` - fetch package metadata and zip files from OS Data Hub.
2. `extract` - extract CSVs from downloaded zip files and convert to parquet.
3. `split` - ABP only: split raw records into parquet staging files.
4. `flatfile` - transform and deduplicate into final output parquet file(s).

All stages are idempotent. Use `--overwrite` to regenerate outputs (`--force` is accepted as a backward-compatible alias).

## Output

Final outputs are parquet files in `paths.output_dir`:

- Single chunk: `ngd_for_uk_address_matcher.chunk_001_of_001.parquet`
- Multi-chunk: `ngd_for_uk_address_matcher.chunk_001_of_00N.parquet`, `...chunk_00N_of_00N.parquet`

Chunking reduces memory use by processing UPRNs in batches. The union of all chunk files equals the single-chunk output. Use a higher `num_chunks` (for example `10`) for laptops with limited RAM.

## Schemas

<details>
<summary><strong>NGD output schema</strong></summary>

### Output

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

Metadata columns (`classificationcode`, `parentuprn`, `rootuprn`, `hierarchylevel`, `floorlevel`, `lowestfloorlevel`, `highestfloorlevel`) are enriched via UPRN lookup from core address files. This means Royal Mail addresses and alternate address records receive metadata from their corresponding Built, Historic, or Pre-Build records.

</details>

<details>
<summary><strong>AddressBase Premium output schema</strong></summary>

### Output format

The final output is written to `paths.output_dir` as one or more parquet files:

- Single chunk mode (`num_chunks: 1`): `abp_for_uk_address_matcher.chunk_001_of_001.parquet`
- Multi-chunk mode (`num_chunks: N`): `abp_for_uk_address_matcher.chunk_001_of_00N.parquet`, `chunk_002_of_00N.parquet`, and so on

Chunking reduces memory usage by processing UPRNs in batches. The union of all chunk files equals the single-chunk output. Use a higher `num_chunks` (for example `10`) for laptops with limited RAM.

Each file contains:

| Column | Description |
|--------|-------------|
| `uprn` | Unique Property Reference Number |
| `postcode` | Postcode |
| `address_concat` | Concatenated address string (without postcode) |
| `classification_code` | Property classification |
| `logical_status` | Address status (1 = Approved, 3 = Alternative, and so on) |
| `blpu_state` | Building state |
| `postal_address_code` | Postal address indicator |
| `udprn` | Royal Mail delivery point reference |
| `parent_uprn` | Parent UPRN for hierarchical addresses |
| `hierarchy_level` | C = Child, P = Parent, S = Singleton |
| `source` | Data source (LPI, ORGANISATION, DELIVERY_POINT, CUSTOM_LEVEL) |
| `variant_label` | Address variant type |
| `is_primary` | Whether this is the primary address for the UPRN |

</details>

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
source:
  type: ngd  # or abp

paths:
  work_dir: ./data
  downloads_dir: ./data/downloads
  extracted_dir: ./data/extracted
  parquet_dir: ./data/parquet
  output_dir: ./data/output

os_downloads:
  package_id: "<your_package_id>"
  version_id: "<your_version_id>"
  connect_timeout_seconds: 30
  read_timeout_seconds: 300

processing:
  parquet_compression: zstd
  parquet_compression_level: 9
  num_chunks: 20
  # duckdb_memory_limit: "8GB"
```

## Smoke test

```bash
pytest tests/test_smoke.py
```

## Related projects

- [uk_address_matcher](https://github.com/moj-analytical-services/uk_address_matcher)
- [prepare_addressbase_for_address_matching](https://github.com/moj-analytical-services/prepare_addressbase_for_address_matching)
- [OS Data Hub](https://osdatahub.os.uk/) - package/version management and downloads