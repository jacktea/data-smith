package sql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jacktea/data-smith/pkg/conn"
)

type schemaObjectID struct {
	kind   conn.SchemaObjectKind
	schema string
	name   string
}

func (id schemaObjectID) display() string {
	qualified := id.name
	if id.schema != "" {
		qualified = id.schema + "." + id.name
	}
	return string(id.kind) + ":" + qualified
}

type schemaOperationID struct {
	object schemaObjectID
	step   string
}

func (id schemaOperationID) key() string {
	return string(id.object.kind) + "\x00" + id.object.schema + "\x00" + id.object.name + "\x00" + id.step
}

func (id schemaOperationID) display() string {
	if id.step == "base" {
		return id.object.display()
	}
	return id.object.display() + "[" + id.step + "]"
}

type schemaOperation struct {
	id         schemaOperationID
	statements []string
	requires   map[string]struct{}
}

// schemaObjectPlan is the internal seam for deterministic schema operation
// ordering. Every edge is dependent -> prerequisite. Creation consumes the
// topological order; deletion builds the same edges and consumes its reverse.
type schemaObjectPlan struct {
	operations map[string]*schemaOperation
}

func newSchemaObjectPlan() *schemaObjectPlan {
	return &schemaObjectPlan{operations: make(map[string]*schemaOperation)}
}

func objectID(kind conn.SchemaObjectKind, schema, name string) schemaObjectID {
	return schemaObjectID{kind: kind, schema: schema, name: name}
}

func operationID(object schemaObjectID, step string) schemaOperationID {
	if step == "" {
		step = "base"
	}
	return schemaOperationID{object: object, step: step}
}

func (p *schemaObjectPlan) add(id schemaOperationID, statement string) {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return
	}
	key := id.key()
	op := p.operations[key]
	if op == nil {
		op = &schemaOperation{id: id, requires: make(map[string]struct{})}
		p.operations[key] = op
	}
	op.statements = append(op.statements, statement)
}

func (p *schemaObjectPlan) require(dependent, prerequisite schemaOperationID) {
	dependentOp := p.operations[dependent.key()]
	if dependentOp == nil || p.operations[prerequisite.key()] == nil || dependent.key() == prerequisite.key() {
		return
	}
	dependentOp.requires[prerequisite.key()] = struct{}{}
}

func (p *schemaObjectPlan) has(id schemaOperationID) bool {
	return p.operations[id.key()] != nil
}

func (p *schemaObjectPlan) baseFor(ref conn.SchemaObjectRef) (schemaOperationID, error) {
	direct := operationID(objectID(ref.Kind, ref.Schema, ref.Name), "base")
	if p.has(direct) {
		return direct, nil
	}
	var matches []schemaOperationID
	for _, operation := range p.operations {
		if operation.id.step != "base" || operation.id.object.kind != ref.Kind || operation.id.object.name != ref.Name {
			continue
		}
		if ref.Schema == "" || operation.id.object.schema == ref.Schema {
			matches = append(matches, operation.id)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].key() < matches[j].key() })
	if len(matches) > 1 {
		displays := make([]string, len(matches))
		for index, match := range matches {
			displays[index] = match.display()
		}
		return schemaOperationID{}, fmt.Errorf("ambiguous dependency %s:%s: %s",
			ref.Kind, ref.Name, strings.Join(displays, ", "))
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	return schemaOperationID{}, nil
}

func (p *schemaObjectPlan) ordered(reverse bool) ([]string, error) {
	dependencies := make(map[string]map[string]struct{}, len(p.operations))
	for key, operation := range p.operations {
		dependencies[key] = make(map[string]struct{}, len(operation.requires))
		for prerequisite := range operation.requires {
			dependencies[key][prerequisite] = struct{}{}
		}
	}
	order, cyclic := stableDependencyOrder(dependencies)
	if len(cyclic) > 0 {
		chain := p.cycleChain(cyclic)
		return nil, fmt.Errorf("schema object dependency cycle: %s", strings.Join(chain, " -> "))
	}
	if reverse {
		for left, right := 0, len(order)-1; left < right; left, right = left+1, right-1 {
			order[left], order[right] = order[right], order[left]
		}
	}
	var statements []string
	for _, key := range order {
		statements = append(statements, p.operations[key].statements...)
	}
	return statements, nil
}

func (p *schemaObjectPlan) cycleChain(cyclic []string) []string {
	inCycle := make(map[string]struct{}, len(cyclic))
	for _, key := range cyclic {
		inCycle[key] = struct{}{}
	}
	sort.Strings(cyclic)
	state := make(map[string]byte)
	stack := make([]string, 0, len(cyclic))
	positions := make(map[string]int)
	var found []string
	var visit func(string) bool
	visit = func(key string) bool {
		state[key] = 1
		positions[key] = len(stack)
		stack = append(stack, key)
		dependencies := make([]string, 0, len(p.operations[key].requires))
		for dependency := range p.operations[key].requires {
			if _, ok := inCycle[dependency]; ok {
				dependencies = append(dependencies, dependency)
			}
		}
		sort.Strings(dependencies)
		for _, dependency := range dependencies {
			if state[dependency] == 1 {
				start := positions[dependency]
				found = append(append([]string(nil), stack[start:]...), dependency)
				return true
			}
			if state[dependency] == 0 && visit(dependency) {
				return true
			}
		}
		stack = stack[:len(stack)-1]
		delete(positions, key)
		state[key] = 2
		return false
	}
	for _, key := range cyclic {
		if state[key] == 0 && visit(key) {
			break
		}
	}
	if len(found) == 0 {
		found = cyclic
	}
	display := make([]string, len(found))
	for index, key := range found {
		display[index] = p.operations[key].id.display()
	}
	return display
}
