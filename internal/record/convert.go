package record

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

var (
	ErrEmpty     = errors.New("empty value")
	ErrNotNumber = errors.New("not a number")
	ErrNotInt    = errors.New("not an integer")
	ErrNotBool   = errors.New("not a boolean")
)

//INFO: ToFloat converts strings and JSON/Go numeric values to float64.
func ToFloat(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, ErrEmpty
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("%q: %w", x, ErrNotNumber)
		}
		return f, nil
	case nil:
		return 0, ErrEmpty
	default:
		return 0, fmt.Errorf("%T: %w", v, ErrNotNumber)
	}
}

//INFO: ToInt converts strings and numeric values to int64. Floats must be whole numbers.
func ToInt(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	case float64:
		if x != math.Trunc(x) || x >= 1<<63 || x < -(1<<63) {
			return 0, fmt.Errorf("%v: %w", x, ErrNotInt)
		}
		return int64(x), nil
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, ErrEmpty
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%q: %w", x, ErrNotInt)
		}
		return n, nil
	case nil:
		return 0, ErrEmpty
	default:
		return 0, fmt.Errorf("%T: %w", v, ErrNotInt)
	}
}

//INFO: ToBool accepts true/false, yes/no, 1/0 (case-insensitive) and JSON booleans.
func ToBool(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "true", "yes", "1":
			return true, nil
		case "false", "no", "0":
			return false, nil
		case "":
			return false, ErrEmpty
		}
		return false, fmt.Errorf("%q: %w", x, ErrNotBool)
	case nil:
		return false, ErrEmpty
	default:
		return false, fmt.Errorf("%T: %w", v, ErrNotBool)
	}
}
