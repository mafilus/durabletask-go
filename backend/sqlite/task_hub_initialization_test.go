package sqlite

import (
	"context"
	"testing"

	"github.com/dapr/durabletask-go/backend"
	"github.com/stretchr/testify/require"
)

func TestCreateTaskHubReturnsSchemaErrorWithoutPanic(t *testing.T) {
	be := NewSqliteBackend(NewSqliteOptions(t.TempDir()), backend.DefaultLogger()).(*sqliteBackend)
	var createErr error
	require.NotPanics(t, func() { createErr = be.CreateTaskHub(context.Background()) })
	require.Error(t, createErr)
	require.Nil(t, be.db)
}
