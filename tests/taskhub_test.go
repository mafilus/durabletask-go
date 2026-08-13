package tests

import (
	"context"
	"testing"

	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/tests/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_TaskHubWorkerStartsDependencies(t *testing.T) {
	ctx := context.Background()

	be := mocks.NewBackend(t)
	orchWorker := mocks.NewTaskWorker[*backend.WorkflowWorkItem](t)
	actWorker := mocks.NewTaskWorker[*backend.ActivityWorkItem](t)

	be.EXPECT().CreateTaskHub(ctx).Return(nil).Once()
	be.EXPECT().Start(ctx).Return(nil).Once()
	orchWorker.EXPECT().Start(ctx).Return().Once()
	actWorker.EXPECT().Start(ctx).Return().Once()

	w := backend.NewTaskHubWorker(be, orchWorker, actWorker, logger)
	err := w.Start(ctx)
	assert.NoError(t, err)
}

func Test_TaskHubWorkerStopsDependencies(t *testing.T) {
	ctx := context.Background()

	be := mocks.NewBackend(t)
	orchWorker := mocks.NewTaskWorker[*backend.WorkflowWorkItem](t)
	actWorker := mocks.NewTaskWorker[*backend.ActivityWorkItem](t)

	be.EXPECT().CreateTaskHub(ctx).Return(nil).Once()
	be.EXPECT().Start(ctx).Return(nil).Once()
	orchWorker.EXPECT().Start(ctx).Return().Once()
	actWorker.EXPECT().Start(ctx).Return().Once()
	be.EXPECT().Stop(mock.Anything).Return(nil).Once()
	orchWorker.EXPECT().StopAndDrain(mock.Anything).Return(nil).Once()
	actWorker.EXPECT().StopAndDrain(mock.Anything).Return(nil).Once()

	w := backend.NewTaskHubWorker(be, orchWorker, actWorker, logger)
	assert.NoError(t, w.Start(ctx))
	err := w.Shutdown(ctx)
	assert.NoError(t, err)
}
