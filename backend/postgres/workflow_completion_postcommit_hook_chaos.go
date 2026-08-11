//go:build postgreschaos

package postgres

var afterWorkflowCompletionCommitHook func()

func afterWorkflowCompletionCommit() {
	if afterWorkflowCompletionCommitHook != nil {
		afterWorkflowCompletionCommitHook()
	}
}
