package dag

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type Node struct {
	ID       uuid.UUID
	Name     string
	Metadata map[string]interface{}
}

type Edge struct {
	From uuid.UUID
	To   uuid.UUID
}

type Graph struct {
	nodes map[uuid.UUID]*Node
	edges []Edge
}

func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[uuid.UUID]*Node),
		edges: make([]Edge, 0),
	}
}

func (g *Graph) AddNode(name string) *Node {
	id := uuid.New()
	n := &Node{
		ID:       id,
		Name:     name,
		Metadata: make(map[string]interface{}),
	}
	g.nodes[id] = n
	return n
}

func (g *Graph) AddNodeWithID(id uuid.UUID, name string) (*Node, error) {
	if _, exists := g.nodes[id]; exists {
		return nil, fmt.Errorf("node with id %s already exists", id)
	}
	n := &Node{
		ID:       id,
		Name:     name,
		Metadata: make(map[string]interface{}),
	}
	g.nodes[id] = n
	return n, nil
}

func (g *Graph) AddEdge(from, to uuid.UUID) error {
	if _, ok := g.nodes[from]; !ok {
		return fmt.Errorf("source node %s does not exist", from)
	}
	if _, ok := g.nodes[to]; !ok {
		return fmt.Errorf("target node %s does not exist", to)
	}
	for _, e := range g.edges {
		if e.From == from && e.To == to {
			return fmt.Errorf("edge from %s to %s already exists", from, to)
		}
	}
	g.edges = append(g.edges, Edge{From: from, To: to})
	return nil
}

func (g *Graph) RemoveNode(id uuid.UUID) bool {
	if _, ok := g.nodes[id]; !ok {
		return false
	}
	delete(g.nodes, id)
	filtered := make([]Edge, 0, len(g.edges))
	for _, e := range g.edges {
		if e.From != id && e.To != id {
			filtered = append(filtered, e)
		}
	}
	g.edges = filtered
	return true
}

func (g *Graph) RemoveEdge(from, to uuid.UUID) bool {
	for i, e := range g.edges {
		if e.From == from && e.To == to {
			g.edges = append(g.edges[:i], g.edges[i+1:]...)
			return true
		}
	}
	return false
}

func (g *Graph) Node(id uuid.UUID) *Node {
	return g.nodes[id]
}

func (g *Graph) Nodes() []*Node {
	result := make([]*Node, 0, len(g.nodes))
	for _, n := range g.nodes {
		result = append(result, n)
	}
	return result
}

func (g *Graph) Edges() []Edge {
	result := make([]Edge, len(g.edges))
	copy(result, g.edges)
	return result
}

func (g *Graph) NodeCount() int {
	return len(g.nodes)
}

func (g *Graph) EdgeCount() int {
	return len(g.edges)
}

func (g *Graph) Children(id uuid.UUID) []*Node {
	var children []*Node
	for _, e := range g.edges {
		if e.From == id {
			if n, ok := g.nodes[e.To]; ok {
				children = append(children, n)
			}
		}
	}
	return children
}

func (g *Graph) Parents(id uuid.UUID) []*Node {
	var parents []*Node
	for _, e := range g.edges {
		if e.To == id {
			if n, ok := g.nodes[e.From]; ok {
				parents = append(parents, n)
			}
		}
	}
	return parents
}

func (g *Graph) HasCycle() bool {
	_, hasCycle := g.TopologicalSort()
	return hasCycle
}

func (g *Graph) TopologicalSort() ([]*Node, bool) {
	inDegree := make(map[uuid.UUID]int)
	for _, n := range g.nodes {
		inDegree[n.ID] = 0
	}
	for _, e := range g.edges {
		inDegree[e.To]++
	}

	queue := make([]uuid.UUID, 0)
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	sorted := make([]*Node, 0, len(g.nodes))
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		sorted = append(sorted, g.nodes[current])

		for _, e := range g.edges {
			if e.From == current {
				inDegree[e.To]--
				if inDegree[e.To] == 0 {
					queue = append(queue, e.To)
				}
			}
		}
	}

	hasCycle := len(sorted) != len(g.nodes)
	return sorted, hasCycle
}

func (g *Graph) Levels() ([][]*Node, error) {
	sorted, hasCycle := g.TopologicalSort()
	if hasCycle {
		return nil, fmt.Errorf("graph contains a cycle, cannot compute levels")
	}

	inDegree := make(map[uuid.UUID]int)
	for _, n := range g.nodes {
		inDegree[n.ID] = 0
	}
	for _, e := range g.edges {
		inDegree[e.To]++
	}

	levelMap := make(map[uuid.UUID]int)
	queue := make([]uuid.UUID, 0)

	for id, deg := range inDegree {
		if deg == 0 {
			levelMap[id] = 0
			queue = append(queue, id)
		}
	}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		for _, e := range g.edges {
			if e.From == current {
				childLevel := levelMap[current] + 1
				if existing, ok := levelMap[e.To]; !ok || childLevel > existing {
					levelMap[e.To] = childLevel
				}
				inDegree[e.To]--
				if inDegree[e.To] == 0 {
					queue = append(queue, e.To)
				}
			}
		}
	}

	maxLevel := 0
	for _, l := range levelMap {
		if l > maxLevel {
			maxLevel = l
		}
	}

	levels := make([][]*Node, maxLevel+1)
	for i := range levels {
		levels[i] = make([]*Node, 0)
	}
	for _, n := range sorted {
		lvl := levelMap[n.ID]
		levels[lvl] = append(levels[lvl], n)
	}

	return levels, nil
}

func (g *Graph) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Graph (%d nodes, %d edges):\n", len(g.nodes), len(g.edges)))
	for _, n := range g.nodes {
		parents := g.Parents(n.ID)
		children := g.Children(n.ID)
		parentNames := make([]string, len(parents))
		for i, p := range parents {
			parentNames[i] = p.Name
		}
		childNames := make([]string, len(children))
		for i, c := range children {
			childNames[i] = c.Name
		}
		b.WriteString(fmt.Sprintf("  %s [parents: %s] [children: %s]\n",
			n.Name,
			strings.Join(parentNames, ", "),
			strings.Join(childNames, ", "),
		))
	}
	return b.String()
}

func (g *Graph) Clear() {
	g.nodes = make(map[uuid.UUID]*Node)
	g.edges = make([]Edge, 0)
}
