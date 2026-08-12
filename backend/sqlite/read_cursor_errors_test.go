package sqlite

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestReadQueriesReturnDeferredCursorErrors(t *testing.T) {
	payload, err := backend.MarshalHistoryEvent(&protos.HistoryEvent{EventId: 1, Timestamp: timestamppb.Now()})
	require.NoError(t, err)

	tests := []struct {
		name    string
		query   string
		columns []string
		row     []driver.Value
		read    func(context.Context, *sqliteBackend) error
	}{
		{
			name:    "workflow runtime state",
			query:   "SELECT \\[EventPayload\\] FROM History",
			columns: []string{"EventPayload"},
			row:     []driver.Value{payload},
			read: func(ctx context.Context, be *sqliteBackend) error {
				_, err := be.GetWorkflowRuntimeState(ctx, &backend.WorkflowWorkItem{InstanceID: "cursor-error-workflow"})
				return err
			},
		},
		{
			name:    "history",
			query:   "SELECT \\[EventPayload\\] FROM History",
			columns: []string{"EventPayload"},
			row:     []driver.Value{payload},
			read: func(ctx context.Context, be *sqliteBackend) error {
				_, err := be.GetInstanceHistory(ctx, &backend.GetInstanceHistoryRequest{InstanceId: "cursor-error-workflow"})
				return err
			},
		},
		{
			name:    "instance IDs",
			query:   "SELECT \\[InstanceID\\] FROM Instances",
			columns: []string{"InstanceID"},
			row:     []driver.Value{"cursor-error-workflow"},
			read: func(ctx context.Context, be *sqliteBackend) error {
				_, err := be.ListInstanceIDs(ctx, &backend.ListInstanceIDsRequest{})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			cursorErr := errors.New("injected cursor error")
			mock.ExpectQuery(test.query).WillReturnRows(
				sqlmock.NewRows(test.columns).AddRow(test.row...).CloseError(cursorErr),
			).RowsWillBeClosed()

			be := &sqliteBackend{db: db}
			require.ErrorIs(t, test.read(context.Background(), be), cursorErr)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
