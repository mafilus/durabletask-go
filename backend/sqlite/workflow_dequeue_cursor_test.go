package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWorkflowDequeueRollsBackOnReturningCursorError(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	payload, err := backend.MarshalHistoryEvent(&protos.HistoryEvent{EventId: 1, Timestamp: timestamppb.Now()})
	require.NoError(t, err)
	cursorErr := errors.New("injected RETURNING cursor error")

	mock.ExpectBegin()
	mock.ExpectQuery("UPDATE Instances SET").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(
		sqlmock.NewRows([]string{"InstanceID"}).AddRow("cursor-error-workflow"),
	)
	mock.ExpectQuery("UPDATE NewEvents SET").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(
		sqlmock.NewRows([]string{"SequenceNumber", "EventPayload", "DequeueCount"}).
			AddRow(int64(1), payload, int32(1)).
			CloseError(cursorErr),
	).RowsWillBeClosed()
	mock.ExpectRollback()

	be := &sqliteBackend{db: db, options: &SqliteOptions{WorkflowLockTimeout: time.Second}, logger: backend.DefaultLogger()}
	_, err = be.getWorkflowWorkItem(ctx)
	require.ErrorIs(t, err, cursorErr)
	require.NoError(t, mock.ExpectationsWereMet())
}
