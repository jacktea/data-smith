package utils

import "strings"

func JoinWrap(arr []string, wrap, sep string) string {
	if len(arr) == 0 {
		return ""
	}
	return wrap + strings.Join(arr, wrap+sep+wrap) + wrap
}

func JoinPreSuf(arr []string, pre, suf, sep string) string {
	if len(arr) == 0 {
		return ""
	}
	return pre + strings.Join(arr, suf+sep+pre) + suf
}
