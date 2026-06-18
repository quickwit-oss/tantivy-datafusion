# Planning Notes

These documents capture implementation history and integration planning. They
are useful background, but they are not public API contracts for
`tantivy-datafusion`.

The current repository goal is SQL execution over Tantivy through DataFusion.
Elasticsearch compatibility design notes were intentionally removed from the
root-level OSS presentation to keep the project focused.

Relevant notes:

- `multi-split.md`: multi-split `TantivyTableProvider` design for Quickwit-style
  execution.
- `runtime-alignment.md`: runtime alignment notes for sync Tantivy work inside
  async systems.
- `schema-management.md`: schema evolution and canonical fast-field schema
  notes.
- `split-planning.md`: planner/worker split metadata boundary.
- `todo.md`: remaining follow-up items.
