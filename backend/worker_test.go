package backend

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultMaxParallelism(t *testing.T) {
	want := int32(4 * runtime.GOMAXPROCS(0))
	if want < minDefaultMaxParallelism {
		want = minDefaultMaxParallelism
	}
	if want > maxDefaultMaxParallelism {
		want = maxDefaultMaxParallelism
	}
	require.Equal(t, want, DefaultMaxParallelism())
}

func TestNewTaskWorkerLimitsParallelismByDefault(t *testing.T) {
	worker := NewTaskWorker[*ActivityWorkItem](nil, nil).(*worker[*ActivityWorkItem])
	require.Equal(t, int(DefaultMaxParallelism()), cap(worker.parallelLock))
}

func TestNewTaskWorkerAllowsExplicitParallelismOverride(t *testing.T) {
	worker := NewTaskWorker[*ActivityWorkItem](nil, nil, WithMaxParallelism(3)).(*worker[*ActivityWorkItem])
	require.Equal(t, 3, cap(worker.parallelLock))
}
