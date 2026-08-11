//go:build !postgreschaos

package postgres

func afterWorkflowCompletionCommit() {}
