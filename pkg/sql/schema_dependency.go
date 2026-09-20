package sql

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/jacktea/data-smith/pkg/conn"
)

var nextvalDependencyPattern = regexp.MustCompile(`(?i)nextval\s*\(\s*'([^']+)'\s*(?:::regclass)?\s*\)`)

type dependencyToken struct {
	text string
}

func inferTableDependencies(table *conn.Table, plan *schemaObjectPlan) ([]conn.SchemaObjectRef, error) {
	if table == nil {
		return nil, nil
	}
	var dependencies []conn.SchemaObjectRef
	for _, column := range table.Columns {
		if column == nil || column.Default == nil {
			continue
		}
		definition := *column.Default
		for _, match := range nextvalDependencyPattern.FindAllStringSubmatch(definition, -1) {
			schema, name := splitObjectName(strings.Trim(match[1], `"`), table.Schema)
			dependencies = append(dependencies, conn.SchemaObjectRef{Kind: conn.SchemaObjectSequence, Schema: schema, Name: name})
		}
		refs, err := inferReferences(definition, plan, objectID(conn.SchemaObjectTable, table.Schema, table.Name), false)
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, refs...)
	}
	if table.Type == conn.TableTypeView && table.ViewDefinition != nil {
		refs, err := inferReferences(table.ViewDefinition.SelectStatement, plan,
			objectID(conn.SchemaObjectView, table.Schema, table.Name), false)
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, refs...)
	}
	return uniqueObjectRefs(dependencies), nil
}

func inferRoutineDependencies(routine *conn.Routine, plan *schemaObjectPlan) ([]conn.SchemaObjectRef, error) {
	if routine == nil {
		return nil, nil
	}
	tokens := tokenizeDependencySQL(routine.Definition)
	language := ""
	for index := 0; index+1 < len(tokens); index++ {
		if strings.EqualFold(tokens[index].text, "language") {
			language = strings.ToLower(tokens[index+1].text)
			break
		}
	}
	if len(plan.operations) > 1 && language != "sql" && language != "plpgsql" {
		if language == "" {
			language = "unknown"
		}
		return nil, fmt.Errorf("%s -> unsupported dependency extraction language %s",
			objectID(conn.SchemaObjectRoutine, routine.Schema, routine.Identity()).display(), language)
	}
	return inferReferences(routine.Definition, plan,
		objectID(conn.SchemaObjectRoutine, routine.Schema, routine.Identity()), true)
}

func inferReferences(definition string, plan *schemaObjectPlan, owner schemaObjectID, rejectDynamic bool) ([]conn.SchemaObjectRef, error) {
	tokens := tokenizeDependencySQL(definition)
	if rejectDynamic {
		for _, token := range tokens {
			if strings.EqualFold(token.text, "execute") && len(plan.operations) > 1 {
				return nil, fmt.Errorf("%s -> unresolved dynamic SQL EXECUTE", owner.display())
			}
		}
	}
	candidates := planObjectCandidates(plan)
	var dependencies []conn.SchemaObjectRef
	for index := 0; index < len(tokens); index++ {
		keyword := strings.ToLower(tokens[index].text)
		if keyword == "returns" {
			next := index + 1
			if next < len(tokens) && strings.EqualFold(tokens[next].text, "setof") {
				next++
			}
			if ref, consumed := matchObjectReference(tokens, next, owner.schema, candidates, conn.SchemaObjectTable, conn.SchemaObjectView); consumed > 0 {
				dependencies = append(dependencies, ref)
			}
		}
		if keyword == "from" || keyword == "join" || keyword == "update" || keyword == "into" {
			if ref, consumed := matchObjectReference(tokens, index+1, owner.schema, candidates, conn.SchemaObjectTable, conn.SchemaObjectView); consumed > 0 {
				dependencies = append(dependencies, ref)
			}
		}
		if index+1 < len(tokens) && tokens[index+1].text == "(" {
			if isRoutineDeclarationName(tokens, index) {
				continue
			}
			name := tokens[index].text
			schema := owner.schema
			if index >= 2 && tokens[index-1].text == "." {
				schema = tokens[index-2].text
			}
			matches := matchRoutineCandidates(candidates, schema, name)
			if len(matches) > 1 {
				displays := make([]string, len(matches))
				for matchIndex, match := range matches {
					displays[matchIndex] = string(match.Kind) + ":" + match.Schema + "." + match.Name
				}
				return nil, fmt.Errorf("%s -> ambiguous routine call %s: %s", owner.display(), name, strings.Join(displays, ", "))
			}
			if len(matches) == 1 && !(owner.kind == conn.SchemaObjectRoutine && matches[0].Schema == owner.schema && matches[0].Name == owner.name) {
				dependencies = append(dependencies, matches[0])
			}
		}
	}
	return uniqueObjectRefs(dependencies), nil
}

func isRoutineDeclarationName(tokens []dependencyToken, index int) bool {
	isKind := func(value string) bool {
		return strings.EqualFold(value, "function") || strings.EqualFold(value, "procedure")
	}
	if index > 0 && isKind(tokens[index-1].text) {
		return true
	}
	return index >= 3 && tokens[index-1].text == "." && isKind(tokens[index-3].text)
}

func planObjectCandidates(plan *schemaObjectPlan) []conn.SchemaObjectRef {
	seen := make(map[string]conn.SchemaObjectRef)
	for _, operation := range plan.operations {
		if operation.id.step != "base" {
			continue
		}
		object := operation.id.object
		ref := conn.SchemaObjectRef{Kind: object.kind, Schema: object.schema, Name: object.name}
		seen[string(ref.Kind)+"\x00"+ref.Schema+"\x00"+ref.Name] = ref
	}
	result := make([]conn.SchemaObjectRef, 0, len(seen))
	for _, ref := range seen {
		result = append(result, ref)
	}
	sort.Slice(result, func(i, j int) bool {
		left := string(result[i].Kind) + "\x00" + result[i].Schema + "\x00" + result[i].Name
		right := string(result[j].Kind) + "\x00" + result[j].Schema + "\x00" + result[j].Name
		return left < right
	})
	return result
}

func matchObjectReference(tokens []dependencyToken, start int, defaultSchema string, candidates []conn.SchemaObjectRef, kinds ...conn.SchemaObjectKind) (conn.SchemaObjectRef, int) {
	if start >= len(tokens) || tokens[start].text == "(" {
		return conn.SchemaObjectRef{}, 0
	}
	schema, name, consumed := defaultSchema, tokens[start].text, 1
	if start+2 < len(tokens) && tokens[start+1].text == "." {
		schema, name, consumed = tokens[start].text, tokens[start+2].text, 3
	}
	for _, candidate := range candidates {
		if candidate.Schema != schema || candidate.Name != name {
			continue
		}
		for _, kind := range kinds {
			if candidate.Kind == kind {
				return candidate, consumed
			}
		}
	}
	return conn.SchemaObjectRef{}, 0
}

func matchRoutineCandidates(candidates []conn.SchemaObjectRef, schema, name string) []conn.SchemaObjectRef {
	var matches []conn.SchemaObjectRef
	for _, candidate := range candidates {
		if candidate.Kind != conn.SchemaObjectRoutine || candidate.Schema != schema {
			continue
		}
		identityName := candidate.Name
		if open := strings.IndexByte(identityName, '('); open >= 0 {
			identityName = identityName[:open]
		}
		if identityName == name {
			matches = append(matches, candidate)
		}
	}
	return matches
}

func uniqueObjectRefs(values []conn.SchemaObjectRef) []conn.SchemaObjectRef {
	seen := make(map[string]conn.SchemaObjectRef)
	for _, value := range values {
		if value.Name == "" {
			continue
		}
		key := string(value.Kind) + "\x00" + value.Schema + "\x00" + value.Name
		seen[key] = value
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]conn.SchemaObjectRef, 0, len(keys))
	for _, key := range keys {
		result = append(result, seen[key])
	}
	return result
}

func tokenizeDependencySQL(value string) []dependencyToken {
	var result []dependencyToken
	for index := 0; index < len(value); {
		r := rune(value[index])
		if unicode.IsSpace(r) {
			index++
			continue
		}
		if index+1 < len(value) && value[index:index+2] == "--" {
			if newline := strings.IndexByte(value[index+2:], '\n'); newline >= 0 {
				index += newline + 3
			} else {
				break
			}
			continue
		}
		if index+1 < len(value) && value[index:index+2] == "/*" {
			if end := strings.Index(value[index+2:], "*/"); end >= 0 {
				index += end + 4
			} else {
				break
			}
			continue
		}
		if value[index] == '\'' {
			index++
			for index < len(value) {
				if value[index] == '\'' {
					if index+1 < len(value) && value[index+1] == '\'' {
						index += 2
						continue
					}
					index++
					break
				}
				index++
			}
			continue
		}
		if value[index] == '$' {
			if end := strings.IndexByte(value[index+1:], '$'); end >= 0 {
				tagEnd := index + end + 2
				tag := value[index:tagEnd]
				tagName := tag[1 : len(tag)-1]
				valid := tagName == "" || unicode.IsLetter(rune(tagName[0])) || tagName[0] == '_'
				for _, tagRune := range tagName {
					if !unicode.IsLetter(tagRune) && !unicode.IsDigit(tagRune) && tagRune != '_' {
						valid = false
						break
					}
				}
				if valid {
					index = tagEnd
					continue
				}
			}
		}
		if value[index] == '"' {
			index++
			var identifier strings.Builder
			for index < len(value) {
				if value[index] == '"' {
					if index+1 < len(value) && value[index+1] == '"' {
						identifier.WriteByte('"')
						index += 2
						continue
					}
					index++
					break
				}
				identifier.WriteByte(value[index])
				index++
			}
			result = append(result, dependencyToken{text: identifier.String()})
			continue
		}
		if strings.ContainsRune("().,;", rune(value[index])) {
			result = append(result, dependencyToken{text: value[index : index+1]})
			index++
			continue
		}
		start := index
		for index < len(value) {
			current := rune(value[index])
			if unicode.IsSpace(current) || strings.ContainsRune("'\"$().,;", current) {
				break
			}
			index++
		}
		if start == index {
			index++
			continue
		}
		result = append(result, dependencyToken{text: value[start:index]})
	}
	return result
}
