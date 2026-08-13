# Postgres Backend

## Connection pool

`NewPostgresOptions` starts the pgx pool with 16 connections (`DefaultMaxConns`).
This is a starting point for the default worker parallelism; set
`options.PgOptions.MaxConns` explicitly for the deployment's worker count and
PostgreSQL connection budget.

### Testing
By default, the postgres tests are skipped. To run the tests, set the environment variable `POSTGRES_ENABLED` to `true` before running the tests and have a postgres server running on `localhost:5432` with a database named `postgres` and a user `postgres` with password `postgres`.
