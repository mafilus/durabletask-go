package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/mafilus/durabletask-go/backend"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

func TestWorkflowDequeueRollsBackOnReturningCursorError(t *testing.T) {
	ctx := context.Background()
	mockDB, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mockDB.Close)

	payload, err := backend.MarshalHistoryEvent(historyEvent(1))
	require.NoError(t, err)
	cursorErr := errors.New("injected RETURNING cursor error")

	mockDB.ExpectBegin()
	mockDB.ExpectQuery("UPDATE Instances SET").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(mockDB.NewRows([]string{"InstanceID"}).AddRow("cursor-error-workflow"))
	mockDB.ExpectQuery("UPDATE NewEvents SET").WillReturnRows(
		mockDB.NewRows([]string{"SequenceNumber", "EventPayload", "DequeueCount"}).
			AddRow(int64(1), payload, int32(1)).
			CloseError(cursorErr),
	).WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).RowsWillBeClosed()
	mockDB.ExpectRollback()

	be := &postgresBackend{db: mockDB, options: &PostgresOptions{WorkflowLockTimeout: 1}, logger: backend.DefaultLogger()}
	_, err = be.GetWorkflowWorkItem(ctx)
	require.ErrorIs(t, err, cursorErr)
	require.NoError(t, mockDB.ExpectationsWereMet())
}
