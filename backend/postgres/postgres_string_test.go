package postgres

import (
	"testing"

	"github.com/mafilus/durabletask-go/backend"
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

func TestNewPostgresOptionsUsesDocumentedPoolDefault(t *testing.T) {
	options := NewPostgresOptions("db.example.test", 5432, "durabletask", "service-user", "password")
	require.Equal(t, DefaultMaxConns, options.PgOptions.MaxConns)
}

func TestNewPostgresOptionsHonorsPGSSLMODE(t *testing.T) {
	t.Setenv("PGSSLMODE", "disable")

	options := NewPostgresOptions("db.example.test", 5432, "durabletask", "service-user", "password")
	require.Nil(t, options.PgOptions.ConnConfig.Config.TLSConfig)
	require.Empty(t, options.PgOptions.ConnConfig.Config.Fallbacks)
}
