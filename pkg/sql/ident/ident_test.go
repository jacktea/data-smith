package ident

import "testing"

func TestQuoteAndQualifiedEscapeEmbeddedDelimiters(t *testing.T) {
	if got, want := Quote(DoubleQuote, `odd"name`), `"odd""name"`; got != want {
		t.Fatalf("postgres identifier: got %q want %q", got, want)
	}
	if got, want := Qualified(DoubleQuote, `sales"Ops`, "Order"), `"sales""Ops"."Order"`; got != want {
		t.Fatalf("postgres qualified name: got %q want %q", got, want)
	}
	if got, want := Qualified(Backtick, "odd`db", "select"), "`odd``db`.`select`"; got != want {
		t.Fatalf("mysql qualified name: got %q want %q", got, want)
	}
}
