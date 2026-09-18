package exec

import (
	"strings"
	"testing"
)

func TestScanSQL(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		wantCommands []string
		wantTx       string
	}{
		{
			name:         "plain statements",
			sql:          "INSERT INTO t VALUES (1);\nUPDATE t SET n = 2;",
			wantCommands: []string{"INSERT", "UPDATE"},
		},
		{
			name:         "quoted semicolons and transaction words",
			sql:          "INSERT INTO `BEGIN;table` (\"COMMIT;column\", value) VALUES ('BEGIN; COMMIT;', 'it''s; ok'); SELECT 1;",
			wantCommands: []string{"INSERT", "SELECT"},
		},
		{
			name:         "comments are inert",
			sql:          "-- BEGIN; hidden\n/* COMMIT; /* nested ROLLBACK; */ still hidden */\nDELETE FROM t;",
			wantCommands: []string{"DELETE"},
		},
		{
			name: "postgres dollar quote is opaque",
			sql: `CREATE FUNCTION f() RETURNS void AS $body$
BEGIN;
  PERFORM 'COMMIT;';
END;
$body$ LANGUAGE plpgsql;
SELECT 1;`,
			wantCommands: []string{"CREATE", "SELECT"},
		},
		{
			name:         "empty dollar quote tag",
			sql:          "DO $$ BEGIN; RAISE NOTICE 'x;'; END; $$; UPDATE t SET n = 1;",
			wantCommands: []string{"DO", "UPDATE"},
		},
		{
			name:         "top level begin is detected",
			sql:          "/* leading */ BEGIN; INSERT INTO t VALUES (1); COMMIT;",
			wantCommands: []string{"BEGIN", "INSERT", "COMMIT"},
			wantTx:       "BEGIN",
		},
		{
			name:         "with dml resolves command",
			sql:          "WITH changed AS (SELECT 1) UPDATE t SET n = 2;",
			wantCommands: []string{"UPDATE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan, err := scanSQL(tt.sql)
			if err != nil {
				t.Fatalf("scanSQL() error = %v", err)
			}
			if len(scan.statements) != len(tt.wantCommands) {
				t.Fatalf("statement count = %d, want %d", len(scan.statements), len(tt.wantCommands))
			}
			for i, want := range tt.wantCommands {
				if got := scan.statements[i].command(); got != want {
					t.Errorf("statement %d command = %q, want %q", i+1, got, want)
				}
			}
			gotTx := ""
			for _, statement := range scan.statements {
				if gotTx = statement.transactionControl(); gotTx != "" {
					break
				}
			}
			if gotTx != tt.wantTx {
				t.Errorf("transaction control = %q, want %q", gotTx, tt.wantTx)
			}
		})
	}
}

func TestScanSQLUsesDialectSpecificLineComments(t *testing.T) {
	postgresScan, err := scanSQL("SELECT 1 # 2; BEGIN;")
	if err != nil {
		t.Fatal(err)
	}
	if len(postgresScan.statements) != 2 || postgresScan.statements[1].transactionControl() != "BEGIN" {
		t.Fatalf("PostgreSQL-style scan failed to retain BEGIN after #: %#v", postgresScan.statements)
	}

	mysqlOptions := sqlScannerOptions{hashLineComments: true, dashDashRequiresSpace: true}
	mysqlScan, err := scanSQLWithOptions("# BEGIN; hidden\nDELETE FROM t;", mysqlOptions)
	if err != nil {
		t.Fatal(err)
	}
	if len(mysqlScan.statements) != 1 || mysqlScan.statements[0].command() != "DELETE" {
		t.Fatalf("MySQL-style scan failed to ignore # comment: %#v", mysqlScan.statements)
	}

	mysqlScan, err = scanSQLWithOptions("UPDATE t SET n=n--1; DROP TABLE t;", mysqlOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateMySQLDryRun(mysqlScan); err == nil || !strings.Contains(err.Error(), "DROP") {
		t.Fatalf("MySQL -- without trailing whitespace hid DDL: %v", err)
	}

	mysqlScan, err = scanSQLWithOptions("UPDATE t SET n = 1; /* outer /* ends here */ DROP TABLE t; /* tail */", mysqlOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateMySQLDryRun(mysqlScan); err == nil || !strings.Contains(err.Error(), "DROP") {
		t.Fatalf("MySQL non-nested block comment hid DDL: %v", err)
	}

	mysqlScan, err = scanSQLWithOptions("UPDATE t SET n = $tag$; DROP TABLE t; $tag$", mysqlOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateMySQLDryRun(mysqlScan); err == nil || !strings.Contains(err.Error(), "DROP") {
		t.Fatalf("PostgreSQL dollar-quote rules hid MySQL DDL: %v", err)
	}
}

func TestScanSQLUsesDialectSpecificBackslashEscapes(t *testing.T) {
	postgresScan, err := scanSQL(`SELECT '\'; BEGIN;`)
	if err != nil {
		t.Fatal(err)
	}
	if len(postgresScan.statements) != 2 || postgresScan.statements[1].transactionControl() != "BEGIN" {
		t.Fatalf("PostgreSQL standard string hid BEGIN: %#v", postgresScan.statements)
	}

	postgresScan, err = scanSQL(`SELECT E'it\'s; BEGIN'; UPDATE t SET n = 1;`)
	if err != nil {
		t.Fatal(err)
	}
	if len(postgresScan.statements) != 2 || postgresScan.statements[1].command() != "UPDATE" {
		t.Fatalf("PostgreSQL escape string split incorrectly: %#v", postgresScan.statements)
	}
}

func TestScanSQLReportsUnterminatedConstructs(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "single quote", sql: "SELECT 'broken", want: "unterminated"},
		{name: "block comment", sql: "SELECT 1 /* broken", want: "unterminated block comment"},
		{name: "dollar quote", sql: "DO $tag$ broken", want: "unterminated dollar quote"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := scanSQL(tt.sql)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("scanSQL() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestValidateNoTransactionControl(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{name: "begin in string", sql: "SELECT 'BEGIN;';", wantErr: false},
		{name: "begin in comment", sql: "-- BEGIN;\nSELECT 1;", wantErr: false},
		{name: "begin in dollar quote", sql: "DO $$ BEGIN; END; $$;", wantErr: false},
		{name: "begin", sql: "BEGIN; SELECT 1;", wantErr: true},
		{name: "start transaction", sql: "START TRANSACTION;", wantErr: true},
		{name: "set transaction", sql: "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;", wantErr: true},
		{name: "release savepoint", sql: "RELEASE SAVEPOINT s;", wantErr: true},
		{name: "end alias", sql: "END;", wantErr: true},
		{name: "postgres abort alias", sql: "ABORT;", wantErr: true},
		{name: "mysql xa", sql: "XA START 'xid';", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan, err := scanSQL(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			err = validateNoTransactionControl(scan)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateNoTransactionControl() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateMySQLDryRun(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{name: "insert", sql: "INSERT INTO t VALUES (1);"},
		{name: "multiple dml", sql: "UPDATE t SET n = 1; DELETE FROM t WHERE n = 2;"},
		{name: "with update", sql: "WITH ids AS (SELECT id FROM source) UPDATE target SET n = 1;"},
		{name: "ddl", sql: "CREATE TABLE t (id INT);", wantErr: true},
		{name: "ddl after dml", sql: "INSERT INTO t VALUES (1); DROP TABLE t;", wantErr: true},
		{name: "unsupported select", sql: "SELECT 1;", wantErr: true},
		{name: "mysql executable comment", sql: "/*! CREATE TABLE unsafe (id INT) */;", wantErr: true},
		{name: "mariadb executable comment", sql: "/*M! DROP TABLE unsafe */;", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan, err := scanSQL(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			err = validateMySQLDryRun(scan)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateMySQLDryRun() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func FuzzScanSQL(f *testing.F) {
	seeds := []string{
		"SELECT 1;",
		"INSERT INTO t VALUES ('a;b'); UPDATE t SET n = 1;",
		"-- BEGIN;\nSELECT 1;",
		"/* nested /* comment */ ok */ DELETE FROM t;",
		"DO $tag$ BEGIN; PERFORM 1; END; $tag$;",
		"SELECT `semi;colon`, \"BEGIN\" FROM t;",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, sqlText string) {
		options := []sqlScannerOptions{
			{nestedBlockComments: true, dollarQuotes: true},
			{hashLineComments: true, dashDashRequiresSpace: true, backslashQuoteEscapes: true},
		}
		for _, option := range options {
			scan, err := scanSQLWithOptions(sqlText, option)
			if err != nil {
				continue
			}
			previousEnd := 0
			for _, statement := range scan.statements {
				if statement.start < previousEnd || statement.end < statement.start || statement.end > len(sqlText) {
					t.Fatalf("invalid statement span [%d,%d) after %d for input length %d", statement.start, statement.end, previousEnd, len(sqlText))
				}
				previousEnd = statement.end
			}
		}
	})
}
