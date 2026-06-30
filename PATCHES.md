# Patches

This fork is based on `apache/paimon` at tag `release-1.4.1`. The following patches are applied on
top of the upstream release. Each patch is documented here and in the corresponding commit message.

---

## P1 — `metadata.iceberg.changelog.storage`: Iceberg metadata for changelog files

**Patch ID:** `ov0`  
**Version:** `1.4.1-hds.1`  
**Upstream PR:** (pending — will file once validated)

### Problem

`metadata.iceberg.storage: hadoop-catalog` emits Iceberg v2 metadata for Paimon data files
(`data-*.parquet`), enabling BigQuery BigLake external tables. However, Paimon's changelog
producer (`changelog-producer: lookup`) also writes `changelog-*.parquet` files to GCS alongside
the data files — these contain the full CDC stream (`_VALUE_KIND`, `_SEQUENCE_NUMBER`, plus all
source columns). The Iceberg compat layer ignores changelog files, so BigQuery cannot query the
CDC history.

### Solution

Add a new config option `metadata.iceberg.changelog.storage` that, when set alongside
`metadata.iceberg.storage`, emits a companion Iceberg table for the changelog files:

```yaml
metadata.iceberg.storage: hadoop-catalog           # existing
metadata.iceberg.changelog.storage: hadoop-catalog # new — P1 patch
```

On every Paimon snapshot commit, a new `IcebergChangelogCommitCallback` reads the changelog
manifest lists from all currently-retained Paimon snapshots and writes a fresh Iceberg
`v{N}.metadata.json` at:

```
<iceberg-root>/<db>/<table>_changelog/metadata/v{N}.metadata.json
```

No data is copied — the manifest pointers reference the existing `changelog-*.parquet` files
in-place. The companion Iceberg table exposes all source columns plus:

| Column             | Type | Description |
|--------------------|------|-------------|
| `_value_kind`      | int  | 0=INSERT, 1=UPDATE_BEFORE, 2=UPDATE_AFTER, 3=DELETE |
| `_sequence_number` | long | Monotonically increasing within a checkpoint |

### Files changed

| File | Change |
|------|--------|
| `paimon-core/src/main/java/org/apache/paimon/iceberg/IcebergOptions.java` | Added `METADATA_ICEBERG_CHANGELOG_STORAGE` config option |
| `paimon-core/src/main/java/org/apache/paimon/iceberg/IcebergChangelogCommitCallback.java` | New class implementing the changelog Iceberg metadata writer |
| `paimon-core/src/main/java/org/apache/paimon/AbstractFileStore.java` | Wire up `IcebergChangelogCommitCallback` alongside `IcebergCommitCallback` |

### Upstream contribution plan

The patch is self-contained and adds value for any Paimon user with `changelog-producer=lookup`
and an Iceberg-compatible query engine (BigQuery, Spark, Trino). We intend to open a PR to
`apache/paimon` once validated against the production GCS warehouse.
