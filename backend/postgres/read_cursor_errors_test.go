package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/dapr/durabletask-go/backend"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

func TestReadQueriesReturnDeferredCursorErrors(t *testing.T) {
	payload, err := backend.MarshalHistoryEvent(historyEvent(1))
	require.NoError(t, err)

	tests := []struct {
		name    string
		query   string
		columns []string
		row     []any
		withArg bool
		read    func(context.Context, *postgresBackend) error
	}{
		{
			name:    "history",
			query:   "SELECT EventPayload FROM History",
			columns: []string{"EventPayload"},
			row:     []any{payload},
			withArg: true,
			read: func(ctx context.Context, be *postgresBackend) error {
				_, err := be.GetInstanceHistory(ctx, &backend.GetInstanceHistoryRequest{InstanceId: "cursor-error-workflow"})
				return err
			},
		},
		{
			name:    "instance IDs",
			query:   "SELECT InstanceID FROM Instances",
			columns: []string{"InstanceID"},
			row:     []any{"cursor-error-workflow"},
			read: func(ctx context.Context, be *postgresBackend) error {
				_, err := be.ListInstanceIDs(ctx, &backend.ListInstanceIDsRequest{})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockDB, err := pgxmock.NewPool()
			require.NoError(t, err)
			t.Cleanup(mockDB.Close)
			cursorErr := errors.New("injected cursor error")
			expectedQuery := mockDB.ExpectQuery(test.query)
			if test.withArg {
				expectedQuery.WithArgs(pgxmock.AnyArg())
			}
			expectedQuery.WillReturnRows(
				mockDB.NewRows(test.columns).AddRow(test.row...).CloseError(cursorErr),
			).RowsWillBeClosed()

			be := &postgresBackend{db: mockDB}
			require.ErrorIs(t, test.read(context.Background(), be), cursorErr)
			require.NoError(t, mockDB.ExpectationsWereMet())
		})
	}
}
