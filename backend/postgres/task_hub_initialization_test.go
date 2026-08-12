package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/dapr/durabletask-go/backend"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

func TestCreateTaskHubReturnsSchemaErrorAndClosesPool(t *testing.T) {
	mockDB, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mockDB.Close)
	schemaErr := errors.New("injected schema error")
	mockDB.ExpectExec("CREATE TABLE IF NOT EXISTS Instances").WillReturnError(schemaErr)
	mockDB.ExpectClose()

	be := &postgresBackend{
		openDB:  func(context.Context, *pgxpool.Config) (postgresDB, error) { return mockDB, nil },
		options: &PostgresOptions{PgOptions: &pgxpool.Config{}},
		logger:  backend.DefaultLogger(),
	}
	var createErr error
	require.NotPanics(t, func() { createErr = be.CreateTaskHub(context.Background()) })
	require.ErrorIs(t, createErr, schemaErr)
	require.Nil(t, be.db)
	require.NoError(t, mockDB.ExpectationsWereMet())
}
