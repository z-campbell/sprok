package utils

// Removes specified index from slice
func remove[T any](i int, s []T) []T {
	return append(s[:i], s[i+1:]...)
}
