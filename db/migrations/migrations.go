package migrations

import _ "embed"

//go:embed 001_initial.sql
var Initial string

//go:embed 002_internal_cancellation.sql
var InternalCancellation string

//go:embed 003_pipeline_semantics.sql
var PipelineSemantics string

//go:embed 004_observability.sql
var Observability string
