package postgres

import (
	"testing"

	"github.com/dapr/durabletask-go/backend"
	"github.com/stretchr/testify/require"
)

func TestPostgresBackendStringRedactsPassword(t *testing.T) {
	const password = "top-secret-password"
	options := NewPostgresOptions("db.example.test", 5432, "durabletask", "service-user", password)
	be := NewPostgresBackend(options, backend.DefaultLogger()).(*postgresBackend)

	connection := be.String()
	require.NotContains(t, connection, password)
	require.NotContains(t, connection, ":"+password+"@")
	require.Equal(t, "postgresql://service-user@db.example.test:5432/durabletask", connection)
}
