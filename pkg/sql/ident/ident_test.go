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
	if got, want := Qualified(DoubleQuote, "", "standalone"), `"standalone"`; got != want {
		t.Fatalf("unqualified name: got %q want %q", got, want)
	}
}

func TestListQuotesEveryIdentifierInOrder(t *testing.T) {
	if got, want := List(DoubleQuote, []string{"select", `odd"name`, "MixedCase"}, ", "), `"select", "odd""name", "MixedCase"`; got != want {
		t.Fatalf("PostgreSQL identifier list: got %q want %q", got, want)
	}
	if got, want := List(Backtick, nil, ", "), ""; got != want {
		t.Fatalf("empty identifier list: got %q want %q", got, want)
	}
}
