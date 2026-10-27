# datapipe

A concurrent data processing pipeline in Go. Jobs are submitted over a REST API; each job reads records from CSV files, JSON files or HTTP APIs, validates and transforms them, computes aggregations and exports the results, while exposing progress, metrics and errors.

## Project layout

```
cmd/server/        entry point
internal/record/   canonical Record type and value conversion
internal/api/      HTTP routing and handlers
testdata/          sample CSV and JSON inputs
```
