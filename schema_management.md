# Schema Management
Timestamp: 2026-04-12 22:00:00 America/Argentina/Cordoba

## Goal

Quickwit-through-DataFusion needs a single canonical output schema per table scan.
That schema must be fixed before execution. Workers should never infer the output
schema from opened splits or from returned batches.

## Two Schema Authorities

### 1. Catalog-backed schema

This is the default path for normal Quickwit queries such as:

```sql
SELECT * FROM logs
```

Source of truth:
- metastore index metadata
- index config
- doc mapper

Behavior:
- schema is resolved at planning time
- no splits are opened during planning
- the canonical schema is stable across queries for the same index / doc mapping
- this is the Quickwit-native path

In practice:
- `CatalogProvider` / `SchemaProvider` should resolve table metadata from the
  metastore
- `quickwit-datafusion` should build the canonical Arrow schema from the doc mapper
- `tantivy-datafusion` should receive that canonical schema plus split descriptors

### 2. Request-local forced schema

This is the override path for ad hoc SQL, compatibility work, or experiments such as:

```sql
CREATE EXTERNAL TABLE logs (
  ts TIMESTAMP,
  level VARCHAR,
  body VARCHAR
) STORED AS tantivy LOCATION 'logs';
```

Source of truth:
- the declared DDL schema

Behavior:
- schema is scoped to the request / session
- declared schema wins over catalog inference
- planner still resolves splits from metastore
- workers coerce and null-pad split-native data into the declared schema

This is the escape hatch for:
- forcing strings
- forcing floats
- hiding schema drift
- compatibility shims for external SQL consumers

## Canonical Schema Rules

The canonical schema must be chosen before execution and carried through the plan.

`tantivy-datafusion` should not discover schema from:
- opened splits on workers
- returned RecordBatches
- scan-time field inspection

Instead:
- `quickwit-datafusion` chooses the canonical schema
- `tantivy-datafusion` executes against that schema

## Doc Mapper’s Role

The doc mapper is Quickwit’s logical schema-and-query-semantics object.

It is used to:
- define the logical fields and Tantivy schema
- validate requests
- build queries against split schemas
- reconstruct canonical JSON from fetched documents

It is not a return-path schema reconciliation layer.

That means:
- schema is fixed before leaf execution
- leaves may adapt query building to a split’s actual Tantivy schema
- but leaves do not decide the canonical output schema on the way back

## Recommended Planner Contract

At planning time:

1. Resolve table identity from catalog metadata or DDL.
2. Choose canonical schema.
3. Build split descriptors from metastore metadata only.
4. Build a `TableProvider` / `DataSourceExec` using:
   - canonical Arrow schema
   - Tantivy schema
   - split descriptors

At execution time:

1. Workers open splits locally from split descriptors.
2. Workers prepare and cache split runtime locally.
3. Each split emits batches projected/coerced/null-padded into the canonical schema.

## Policy Modes

### Strict doc-mapper mode

Use when:
- the doc mapping is the authoritative schema
- splits are expected to be compatible

Rules:
- canonical schema comes from the doc mapper
- doc mapping UID mismatch across queried data is an error

### DDL override mode

Use when:
- caller needs an explicit schema
- caller wants forced coercions

Rules:
- canonical schema comes from DDL
- coercion and null padding happen in `tantivy-datafusion`

### Future merged-manifest mode

Use when:
- schema drift across splits must be surfaced and merged intentionally

Rules:
- planner merges split manifests into a canonical schema
- still decided before execution
- not inferred from returned rows

## Implementation Boundary

`quickwit-datafusion` owns:
- schema choice
- catalog integration
- DDL handling
- split descriptor planning

`tantivy-datafusion` owns:
- split-local execution
- worker-side split preparation
- coercion
- null padding
- native pushdown

## Non-goals

These are not part of schema management:
- opening splits during planning
- deriving schema from segment metadata on workers
- reconciling schema from returned split batches
- using the return path to discover canonical output types
