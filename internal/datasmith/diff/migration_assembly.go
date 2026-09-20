package diff

import (
	"regexp"
	"strings"
)

// AssembleVersionScripts merges diff artifacts into one up/down migration
// pair with the fixed domain order: up = schema forward → data forward,
// down = data rollback → schema rollback. Each script carries a single
// leading EXECUTE-ON header (the first one found), labeled sections with
// duplicate headers stripped, and blank sections dropped. The generated
// scripts are artifacts only — executing them stays an explicit, separate
// step.
func AssembleVersionScripts(includeSchema, includeData bool, schemaForward, schemaRollback, dataForward, dataRollback string) (up, down string) {
	var upSegments, downSegments [][2]string
	if includeSchema {
		upSegments = append(upSegments, [2]string{"schema forward", schemaForward})
	}
	if includeData {
		upSegments = append(upSegments, [2]string{"data forward", dataForward})
	}
	if includeData {
		downSegments = append(downSegments, [2]string{"data rollback", dataRollback})
	}
	if includeSchema {
		downSegments = append(downSegments, [2]string{"schema rollback", schemaRollback})
	}
	return concatSQLSegments(upSegments), concatSQLSegments(downSegments)
}

// concatSQLSegments joins diff artifacts into one version script: a single
// leading EXECUTE-ON header (the first one found), then labeled sections with
// duplicate headers stripped and blank sections dropped.
func concatSQLSegments(segments [][2]string) string {
	header := ""
	for _, seg := range segments {
		if line := firstExecuteOnHeader(seg[1]); line != "" {
			header = line
			break
		}
	}
	var body strings.Builder
	for _, seg := range segments {
		content := stripExecuteOnHeaders(seg[1])
		if strings.TrimSpace(content) == "" {
			continue
		}
		body.WriteString("-- ==== " + seg[0] + " ====\n")
		body.WriteString(strings.TrimRight(content, "\n") + "\n\n")
	}
	if header == "" {
		return body.String()
	}
	return header + "\n" + body.String()
}

// executeOnHeaderRe matches a full-line DATASMITH EXECUTE-ON comment.
var executeOnHeaderRe = regexp.MustCompile(`(?im)^--\s*DATASMITH EXECUTE-ON:.*$`)

func firstExecuteOnHeader(content string) string {
	loc := executeOnHeaderRe.FindStringIndex(content)
	if loc == nil {
		return ""
	}
	return strings.TrimSpace(content[loc[0]:loc[1]])
}

func stripExecuteOnHeaders(content string) string {
	return executeOnHeaderRe.ReplaceAllString(content, "")
}
