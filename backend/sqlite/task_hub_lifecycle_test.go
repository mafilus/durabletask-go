package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestDeleteTaskHubClosesDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectClose()

	be := &sqliteBackend{db: db, options: NewSqliteOptions("")}
	require.NoError(t, be.DeleteTaskHub(context.Background()))
	require.Nil(t, be.db)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteTaskHubClosesDatabaseWhenFileRemovalFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectClose()

	filePath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(filePath, "keep"), []byte("keep"), 0o600))
	be := &sqliteBackend{db: db, options: NewSqliteOptions(filePath)}
	require.Error(t, be.DeleteTaskHub(context.Background()))
	require.Nil(t, be.db)
	require.NoError(t, mock.ExpectationsWereMet())
}
