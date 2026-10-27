package record

import (
	"errors"
	"testing"
)

func TestToFloat(t *testing.T) {
	tests := []struct {
		name    string
		in      any
		want    float64
		wantErr error
	}{
		{"plain string", "12.5", 12.5, nil},
		{"spaces trimmed", "  7 ", 7, nil},
		{"negative", "-3.25", -3.25, nil},
		{"json number", 42.0, 42, nil},
		{"go int", 3, 3, nil},
		{"empty string", "", 0, ErrEmpty},
		{"only spaces", "   ", 0, ErrEmpty},
		{"nil", nil, 0, ErrEmpty},
		{"letters", "abc", 0, ErrNotNumber},
		{"NaN rejected", "NaN", 0, ErrNotNumber},
		{"Inf rejected", "Inf", 0, ErrNotNumber},
		{"bool rejected", true, 0, ErrNotNumber},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ToFloat(tc.in)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestToInt(t *testing.T) {
	tests := []struct {
		name    string
		in      any
		want    int64
		wantErr error
	}{
		{"plain string", "42", 42, nil},
		{"negative", "-7", -7, nil},
		{"whole float", 10.0, 10, nil},
		{"fractional float", 10.5, 0, ErrNotInt},
		{"decimal string", "1.5", 0, ErrNotInt},
		{"too large float", 1e19, 0, ErrNotInt},
		{"empty", "", 0, ErrEmpty},
		{"nil", nil, 0, ErrEmpty},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ToInt(tc.in)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestToBool(t *testing.T) {
	tests := []struct {
		in      any
		want    bool
		wantErr error
	}{
		{"true", true, nil},
		{"YES", true, nil},
		{"1", true, nil},
		{"false", false, nil},
		{" no ", false, nil},
		{false, false, nil},
		{"", false, ErrEmpty},
		{"maybe", false, ErrNotBool},
		{1, false, ErrNotBool},
	}
	for _, tc := range tests {
		got, err := ToBool(tc.in)
		if !errors.Is(err, tc.wantErr) {
			t.Fatalf("ToBool(%#v) err = %v, want %v", tc.in, err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("ToBool(%#v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
