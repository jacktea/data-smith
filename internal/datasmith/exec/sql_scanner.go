package exec

import (
	"fmt"
	"strings"
)

type sqlToken struct {
	value string
	depth int
}

type sqlStatement struct {
	start       int
	end         int
	startLine   int
	startColumn int
	tokens      []sqlToken
}

type sqlScan struct {
	statements                     []sqlStatement
	hasMySQLExecutableComment      bool
	hasModeDependentBackslashQuote bool
	diagnosticError                error
}

func scanSQL(sqlText string) (sqlScan, error) {
	return scanSQLWithOptions(sqlText, sqlScannerOptions{nestedBlockComments: true, dollarQuotes: true})
}

type sqlScannerOptions struct {
	hashLineComments      bool
	dashDashRequiresSpace bool
	nestedBlockComments   bool
	backslashQuoteEscapes bool
	dollarQuotes          bool
	modeDependentEscapes  bool
}

func scanSQLWithOptions(sqlText string, options sqlScannerOptions) (sqlScan, error) {
	var result sqlScan
	statementStart := -1
	statementLine := 0
	statementColumn := 0
	line, column := 1, 1
	depth := 0
	var tokens []sqlToken

	beginStatement := func(offset int) {
		if statementStart < 0 {
			statementStart = offset
			statementLine = line
			statementColumn = column
		}
	}
	finishStatement := func(end int) {
		if statementStart >= 0 {
			result.statements = append(result.statements, sqlStatement{
				start:       statementStart,
				end:         end,
				startLine:   statementLine,
				startColumn: statementColumn,
				tokens:      append([]sqlToken(nil), tokens...),
			})
		}
		statementStart = -1
		statementLine = 0
		statementColumn = 0
		tokens = tokens[:0]
		depth = 0
	}
	advance := func(text string) {
		for i := 0; i < len(text); i++ {
			if text[i] == '\n' {
				line++
				column = 1
			} else {
				column++
			}
		}
	}

	for i := 0; i < len(sqlText); {
		ch := sqlText[i]

		if isSQLSpace(ch) {
			advance(sqlText[i : i+1])
			i++
			continue
		}

		if ch == '-' && i+1 < len(sqlText) && sqlText[i+1] == '-' && (!options.dashDashRequiresSpace || i+2 == len(sqlText) || isSQLSpace(sqlText[i+2])) {
			start := i
			i += 2
			for i < len(sqlText) && sqlText[i] != '\n' {
				i++
			}
			advance(sqlText[start:i])
			continue
		}
		if ch == '#' && options.hashLineComments {
			start := i
			i++
			for i < len(sqlText) && sqlText[i] != '\n' {
				i++
			}
			advance(sqlText[start:i])
			continue
		}
		if ch == '/' && i+1 < len(sqlText) && sqlText[i+1] == '*' {
			start := i
			if (i+2 < len(sqlText) && sqlText[i+2] == '!') || (i+3 < len(sqlText) && (sqlText[i+2] == 'M' || sqlText[i+2] == 'm') && sqlText[i+3] == '!') {
				result.hasMySQLExecutableComment = true
			}
			i += 2
			commentDepth := 1
			for i < len(sqlText) && commentDepth > 0 {
				switch {
				case options.nestedBlockComments && i+1 < len(sqlText) && sqlText[i] == '/' && sqlText[i+1] == '*':
					commentDepth++
					i += 2
				case i+1 < len(sqlText) && sqlText[i] == '*' && sqlText[i+1] == '/':
					commentDepth--
					i += 2
				default:
					i++
				}
			}
			advance(sqlText[start:i])
			if commentDepth != 0 {
				return sqlScan{}, fmt.Errorf("unterminated block comment at line %d, column %d", line, column)
			}
			continue
		}

		if ch == ';' {
			advance(sqlText[i : i+1])
			i++
			finishStatement(i)
			continue
		}

		if ch == '(' {
			beginStatement(i)
			depth++
			advance(sqlText[i : i+1])
			i++
			continue
		}
		if ch == ')' {
			beginStatement(i)
			if depth > 0 {
				depth--
			}
			advance(sqlText[i : i+1])
			i++
			continue
		}

		if ch == '\'' || ch == '"' || ch == '`' {
			beginStatement(i)
			quote := ch
			postgresEscapeString := quote == '\'' && isPostgresEscapeStringPrefix(sqlText, i)
			backslashEscapes := options.backslashQuoteEscapes || postgresEscapeString
			modeDependentEscapes := options.modeDependentEscapes || quote == '\'' && !postgresEscapeString && !options.backslashQuoteEscapes
			startLine, startColumn := line, column
			start := i
			i++
			closed := false
			for i < len(sqlText) {
				if modeDependentEscapes && sqlText[i] == '\\' && i+1 < len(sqlText) && sqlText[i+1] == quote {
					result.hasModeDependentBackslashQuote = true
				}
				if backslashEscapes && sqlText[i] == '\\' && i+1 < len(sqlText) {
					i += 2
					continue
				}
				if sqlText[i] == quote {
					if i+1 < len(sqlText) && sqlText[i+1] == quote {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			advance(sqlText[start:i])
			if !closed {
				return sqlScan{}, fmt.Errorf("unterminated %q quote at line %d, column %d", quote, startLine, startColumn)
			}
			continue
		}

		if ch == '$' && options.dollarQuotes {
			if delimiter, ok := dollarQuoteDelimiter(sqlText, i); ok {
				beginStatement(i)
				startLine, startColumn := line, column
				start := i
				i += len(delimiter)
				end := strings.Index(sqlText[i:], delimiter)
				if end < 0 {
					advance(sqlText[start:])
					return sqlScan{}, fmt.Errorf("unterminated dollar quote %s at line %d, column %d", delimiter, startLine, startColumn)
				}
				i += end + len(delimiter)
				advance(sqlText[start:i])
				continue
			}
		}

		if isIdentifierStart(ch) {
			beginStatement(i)
			start := i
			for i < len(sqlText) && isIdentifierPart(sqlText[i]) {
				i++
			}
			tokens = append(tokens, sqlToken{value: strings.ToUpper(sqlText[start:i]), depth: depth})
			advance(sqlText[start:i])
			continue
		}

		beginStatement(i)
		advance(sqlText[i : i+1])
		i++
	}

	finishStatement(len(sqlText))
	return result, nil
}

func dollarQuoteDelimiter(sqlText string, offset int) (string, bool) {
	if offset > 0 && isIdentifierPart(sqlText[offset-1]) {
		return "", false
	}
	if offset+1 < len(sqlText) && sqlText[offset+1] == '$' {
		return "$$", true
	}
	if offset+1 >= len(sqlText) || !isIdentifierStart(sqlText[offset+1]) {
		return "", false
	}
	for i := offset + 2; i < len(sqlText); i++ {
		if sqlText[i] == '$' {
			return sqlText[offset : i+1], true
		}
		if !isIdentifierPart(sqlText[i]) {
			return "", false
		}
	}
	return "", false
}

func (s sqlStatement) topLevelTokens() []string {
	result := make([]string, 0, len(s.tokens))
	for _, token := range s.tokens {
		if token.depth == 0 {
			result = append(result, token.value)
		}
	}
	return result
}

func (s sqlStatement) command() string {
	tokens := s.topLevelTokens()
	if len(tokens) == 0 {
		return ""
	}
	if tokens[0] != "WITH" {
		return tokens[0]
	}
	for _, token := range tokens[1:] {
		switch token {
		case "INSERT", "UPDATE", "DELETE", "REPLACE", "SELECT":
			return token
		}
	}
	return "WITH"
}

func (s sqlStatement) transactionControl() string {
	tokens := s.topLevelTokens()
	if len(tokens) == 0 {
		return ""
	}
	switch tokens[0] {
	case "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "END", "ABORT":
		return tokens[0]
	case "START":
		if len(tokens) > 1 && tokens[1] == "TRANSACTION" {
			return "START TRANSACTION"
		}
	case "RELEASE":
		if len(tokens) > 1 && tokens[1] == "SAVEPOINT" {
			return "RELEASE SAVEPOINT"
		}
	case "SET":
		if len(tokens) > 1 && tokens[1] == "TRANSACTION" {
			return "SET TRANSACTION"
		}
	case "PREPARE":
		if len(tokens) > 1 && tokens[1] == "TRANSACTION" {
			return "PREPARE TRANSACTION"
		}
	case "XA":
		if len(tokens) > 1 {
			switch tokens[1] {
			case "START", "BEGIN", "END", "PREPARE", "COMMIT", "ROLLBACK":
				return "XA " + tokens[1]
			}
		}
	}
	return ""
}

func validateNoTransactionControl(scan sqlScan) error {
	for i, statement := range scan.statements {
		if command := statement.transactionControl(); command != "" {
			return fmt.Errorf("SQL contains explicit transaction control %q in statement %d (line %d, column %d)", command, i+1, statement.startLine, statement.startColumn)
		}
	}
	return nil
}

func validateMySQLDryRun(scan sqlScan) error {
	if scan.hasMySQLExecutableComment {
		return fmt.Errorf("MySQL dry-run rejects executable comments (/*! ... */ or /*M! ... */) before execution")
	}
	if len(scan.statements) == 0 {
		return fmt.Errorf("MySQL dry-run requires at least one transactional DML statement")
	}
	for i, statement := range scan.statements {
		command := statement.command()
		switch command {
		case "INSERT", "UPDATE", "DELETE", "REPLACE":
			continue
		case "CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME":
			return fmt.Errorf("MySQL dry-run rejects DDL command %q in statement %d (line %d, column %d) before execution", command, i+1, statement.startLine, statement.startColumn)
		default:
			return fmt.Errorf("MySQL dry-run permits only transactional DML (INSERT, UPDATE, DELETE, REPLACE); found %q in statement %d (line %d, column %d)", command, i+1, statement.startLine, statement.startColumn)
		}
	}
	return nil
}

func isSQLSpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}

func isIdentifierStart(ch byte) bool {
	return ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || ch >= '0' && ch <= '9' || ch == '$'
}

func isPostgresEscapeStringPrefix(sqlText string, quoteOffset int) bool {
	if quoteOffset == 0 || sqlText[quoteOffset-1] != 'E' && sqlText[quoteOffset-1] != 'e' {
		return false
	}
	return quoteOffset == 1 || !isIdentifierPart(sqlText[quoteOffset-2])
}
