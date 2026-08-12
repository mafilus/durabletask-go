// Package ptr provides internal pointer helpers.
package ptr

// Of returns a pointer to v.
func Of[T any](v T) *T {
	return &v
}
