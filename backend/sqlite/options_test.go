package sqlite

import (
	"context"
	"testing"

	"github.com/dapr/durabletask-go/backend"
	"github.com/stretchr/testify/require"
)

func TestNewSqliteBackendNilOptionsUsesDefaults(t *testing.T) {
	be := NewSqliteBackend(nil, backend.DefaultLogger()).(*sqliteBackend)
	require.NotNil(t, be.options)
	require.Equal(t, NewSqliteOptions(""), be.options)
	require.NotPanics(t, func() { _ = be.String() })
	require.NoError(t, be.CreateTaskHub(context.Background()))
	t.Cleanup(func() { _ = be.DeleteTaskHub(context.Background()) })
}
