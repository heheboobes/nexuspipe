package dag

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type ValidationError struct {
	Type    string
	Message string
	NodeID  uuid.UUID
	NodeIDs []uuid.UUID
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Type, e.Message)
}

type ValidationResult struct {
	Valid  bool
	Errors []*ValidationError
}

func (r *ValidationResult) Error() string {
	if r.Valid {
		return ""
	}
	msgs := make([]string, len(r.Errors))
	for i, err := range r.Errors {
		msgs[i] = err.Error()
	}
	return fmt.Sprintf("DAG validation failed (%d errors):\n%s", len(r.Errors), strings.Join(msgs, "\n"))
}

type Validator struct {
	maxDepth int
}

func NewValidator() *Validator {
	return &Validator{
		maxDepth: 100,
	}
}

func NewValidatorWithMaxDepth(maxDepth int) *Validator {
	if maxDepth < 1 {
		maxDepth = 1
	}
	return &Validator{
		maxDepth: maxDepth,
	}
}

func (v *Validator) ValidateDAG(graph *Graph) *ValidationResult {
	errors := make([]*ValidationError, 0)

	cycleErrors := v.validateCycles(graph)
	errors = append(errors, cycleErrors...)

	orphanErrors := v.validateOrphanNodes(graph)
	errors = append(errors, orphanErrors...)

	depErrors := v.validateDependencies(graph)
	errors = append(errors, depErrors...)

	depthErrors := v.validateMaxDepth(graph)
	errors = append(errors, depthErrors...)

	return &ValidationResult{
		Valid:  len(errors) == 0,
		Errors: errors,
	}
}

func (v *Validator) validateCycles(graph *Graph) []*ValidationError {
	sorted, hasCycle := graph.TopologicalSort()
	if !hasCycle {
		return nil
	}

	inCycle := make(map[uuid.UUID]bool)
	for _, n := range graph.Nodes() {
		inCycle[n.ID] = true
	}
	for _, n := range sorted {
		delete(inCycle, n.ID)
	}

	cycleNodeIDs := make([]uuid.UUID, 0, len(inCycle))
	for id := range inCycle {
		cycleNodeIDs = append(cycleNodeIDs, id)
	}

	cycleNames := make([]string, 0, len(cycleNodeIDs))
	for _, id := range cycleNodeIDs {
		if n := graph.Node(id); n != nil {
			cycleNames = append(cycleNames, n.Name)
		}
	}

	return []*ValidationError{
		{
			Type:    "CYCLE_DETECTED",
			Message: fmt.Sprintf("graph contains a cycle involving nodes: %s", strings.Join(cycleNames, ", ")),
			NodeIDs: cycleNodeIDs,
		},
	}
}

func (v *Validator) validateOrphanNodes(graph *Graph) []*ValidationError {
	var errors []*ValidationError

	for _, n := range graph.Nodes() {
		parents := graph.Parents(n.ID)
		children := graph.Children(n.ID)
		if len(parents) == 0 && len(children) == 0 {
			errors = append(errors, &ValidationError{
				Type:    "ORPHAN_NODE",
				Message: fmt.Sprintf("node %q has no dependencies and no dependents", n.Name),
				NodeID:  n.ID,
			})
		}
	}

	return errors
}

func (v *Validator) validateDependencies(graph *Graph) []*ValidationError {
	var errors []*ValidationError

	for _, e := range graph.Edges() {
		if graph.Node(e.From) == nil {
			errors = append(errors, &ValidationError{
				Type:    "MISSING_DEPENDENCY",
				Message: fmt.Sprintf("edge references non-existent source node %s", e.From),
				NodeID:  e.From,
			})
		}
		if graph.Node(e.To) == nil {
			errors = append(errors, &ValidationError{
				Type:    "MISSING_DEPENDENCY",
				Message: fmt.Sprintf("edge references non-existent target node %s", e.To),
				NodeID:  e.To,
			})
		}
	}

	return errors
}

func (v *Validator) validateMaxDepth(graph *Graph) []*ValidationError {
	levels, err := graph.Levels()
	if err != nil {
		return []*ValidationError{
			{
				Type:    "DEPTH_VALIDATION_FAILED",
				Message: fmt.Sprintf("cannot compute levels: %v", err),
			},
		}
	}

	if len(levels) > v.maxDepth {
		return []*ValidationError{
			{
				Type:    "MAX_DEPTH_EXCEEDED",
				Message: fmt.Sprintf("graph depth %d exceeds maximum allowed depth %d", len(levels), v.maxDepth),
			},
		}
	}

	return nil
}

func (v *Validator) ValidateNodeAddition(graph *Graph, name string, dependsOn []uuid.UUID) *ValidationResult {
	errors := make([]*ValidationError, 0)

	for _, depID := range dependsOn {
		if graph.Node(depID) == nil {
			errors = append(errors, &ValidationError{
				Type:    "DEPENDENCY_NOT_FOUND",
				Message: fmt.Sprintf("dependency node %s does not exist in graph", depID),
				NodeID:  depID,
			})
		}
	}

	for _, n := range graph.Nodes() {
		if n.Name == name {
			errors = append(errors, &ValidationError{
				Type:    "DUPLICATE_NODE_NAME",
				Message: fmt.Sprintf("node with name %q already exists", name),
				NodeID:  n.ID,
			})
			break
		}
	}

	return &ValidationResult{
		Valid:  len(errors) == 0,
		Errors: errors,
	}
}

func (v *Validator) ValidateEdgeAddition(graph *Graph, from, to uuid.UUID) *ValidationResult {
	errors := make([]*ValidationError, 0)

	if graph.Node(from) == nil {
		errors = append(errors, &ValidationError{
			Type:    "SOURCE_NOT_FOUND",
			Message: fmt.Sprintf("source node %s does not exist", from),
			NodeID:  from,
		})
	}

	if graph.Node(to) == nil {
		errors = append(errors, &ValidationError{
			Type:    "TARGET_NOT_FOUND",
			Message: fmt.Sprintf("target node %s does not exist", to),
			NodeID:  to,
		})
	}

	if len(errors) > 0 {
		return &ValidationResult{Valid: false, Errors: errors}
	}

	testGraph := NewGraph()
	for _, n := range graph.Nodes() {
		testGraph.nodes[n.ID] = n
	}
	testGraph.edges = append(testGraph.edges, graph.edges...)
	testGraph.edges = append(testGraph.edges, Edge{From: from, To: to})

	if testGraph.HasCycle() {
		fromNode := graph.Node(from)
		toNode := graph.Node(to)
		fromName := "unknown"
		toName := "unknown"
		if fromNode != nil {
			fromName = fromNode.Name
		}
		if toNode != nil {
			toName = toNode.Name
		}
		errors = append(errors, &ValidationError{
			Type:    "EDGE_CREATES_CYCLE",
			Message: fmt.Sprintf("adding edge from %q to %q would create a cycle", fromName, toName),
		})
	}

	return &ValidationResult{
		Valid:  len(errors) == 0,
		Errors: errors,
	}
}
