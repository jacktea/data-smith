package utils

import "testing"

func TestJoinWrap(t *testing.T) {
	tests := []struct {
		name   string
		arr    []string
		wrap   string
		sep    string
		expect string
	}{
		{"normal comma wrap", []string{"a", "b", "c"}, "'", ",", "'a','b','c'"},
		{"no wrap", []string{"a", "b"}, "", ",", "a,b"},
		{"space sep", []string{"x", "y"}, "\"", " ", "\"x\" \"y\""},
		{"single element", []string{"z"}, "[", ",", "[z["},
		{"empty array", []string{}, "'", ",", ""},
		{"custom wrap/sep", []string{"1", "2", "3"}, "<", ":", "<1<:<2<:<3<"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := JoinWrap(tt.arr, tt.wrap, tt.sep)
			if got != tt.expect {
				t.Errorf("Join(%v, %q, %q) = %q, want %q", tt.arr, tt.wrap, tt.sep, got, tt.expect)
			}
		})
	}
}

func TestJoinPreSuf(t *testing.T) {
	tests := []struct {
		name   string
		arr    []string
		pre    string
		suf    string
		sep    string
		expect string
	}{
		{"normal pre suf", []string{"a", "b", "c"}, "(", ")", ",", "(a),(b),(c)"},
		{"no pre suf", []string{"x", "y"}, "", "", ":", "x:y"},
		{"only pre", []string{"1", "2"}, "[", "", "|", "[1|[2"},
		{"only suf", []string{"a", "b"}, "", "]", " ", "a] b]"},
		{"single element", []string{"z"}, "<", ">", ",", "<z>"},
		{"empty array", []string{}, "(", ")", ",", ""},
		{"custom pattern", []string{"x", "y", "z"}, "{{", "}}", "::", "{{x}}::{{y}}::{{z}}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := JoinPreSuf(tt.arr, tt.pre, tt.suf, tt.sep)
			if got != tt.expect {
				t.Errorf("JoinPreSuf(%v, %q, %q, %q) = %q, want %q", tt.arr, tt.pre, tt.suf, tt.sep, got, tt.expect)
			}
		})
	}
}
