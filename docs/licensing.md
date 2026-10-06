# Licensing and dependency notices

ForgeGrid source is licensed under [MIT](../LICENSE), selected by the repository owner for this release. It is supplied without warranty; a license is not a production-readiness or security guarantee.

Third-party dependencies and container images retain their own upstream licenses. `go.mod`/`go.sum` and `frontend/package-lock.json` pin the application dependencies; ForgeGrid's license does not relicense those packages. Preserve their LICENSE/NOTICE files when distributing their source or binaries. Generated Protobuf bindings are intentionally committed and retain their generated headers.

Dependency notice locations:

- Go: the downloaded module's LICENSE/NOTICE in the module cache (`go env GOMODCACHE`) and the upstream source identified by `go list -m -json all`.
- Frontend: installed packages' LICENSE/NOTICE under `frontend/node_modules`, with exact versions in the lockfile. React, React DOM, React Router, TypeScript, Playwright, Vite and Vitest are third-party packages.
- Docker images: PostgreSQL, Docker CLI, Nginx, Go/base OS, Collector, Prometheus and Jaeger carry their own upstream and bundled package notices. The application license does not cover every image component.

No vendored dependency source or third-party image/logo asset is added. Portfolio PNGs are actual ForgeGrid console captures of local demo execution, not stock media. This notice index is not a complete binary/image redistribution notice bundle; upstream notices remain authoritative.
