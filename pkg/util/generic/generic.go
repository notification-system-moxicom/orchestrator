package generic

func DefaultIfZero[T comparable](value, defaultV T) T {
	if value == *new(T) {
		return defaultV
	}

	return value
}

func ZeroIfTrue[T any](needEmpty bool, value T) T {
	if needEmpty {
		return *new(T)
	}

	return value
}
