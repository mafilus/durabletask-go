# Postgres Backend

## Connection pool

`NewPostgresOptions` starts the pgx pool with 16 connections (`DefaultMaxConns`).
This is a starting point for the default worker parallelism; set
`options.PgOptions.MaxConns` explicitly for the deployment's worker count and
PostgreSQL connection budget.

### Testing
Database-backed tests are skipped unless `POSTGRES_ENABLED=true`; unit tests still run.
Use only an isolated, disposable database: durability and stress tests truncate
tables, and migration tests drop/recreate tables. Never point these tests at production.

The backend package tests honor `PGHOST` (default `127.0.0.1`), `PGPORT` (`5432`),
`PGDATABASE` (`durabletask`), `PGUSER` (`postgres`), and `PGPASSWORD` (`postgres`).
Run them with `POSTGRES_ENABLED=true go test -count=1 ./backend/postgres`.
Chaos tests additionally require `STRIX_TEST_POSTGRES_CHAOS=1` and their isolated
Docker Compose configuration; both manual CI workflows set the database opt-in explicitly.
