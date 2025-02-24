package dag

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type NodeStatus string

const (
	NodeStatusPending   NodeStatus = "pending"
	NodeStatusRunning   NodeStatus = "running"
	NodeStatusCompleted NodeStatus = "completed"
	NodeStatusFailed    NodeStatus = "failed"
	NodeStatusSkipped   NodeStatus = "skipped"
)

type NodeResult struct {
	NodeID   uuid.UUID
	NodeName string
	Status   NodeStatus
	Error    string
	Output   interface{}
	Duration time.Duration
}

type NodeHandler func(ctx context.Context, node *Node, inputs map[uuid.UUID]interface{}) (interface{}, error)

type ExecutionPolicy int

const (
	FailFast ExecutionPolicy = iota
	ContinueOnFailure
)

type DAGExecutor struct {
	logger         *zap.Logger
	handlers       map[string]NodeHandler
	mu             sync.RWMutex
	policy         ExecutionPolicy
	defaultTimeout time.Duration
}

func NewDAGExecutor(logger *zap.Logger) *DAGExecutor {
	return &DAGExecutor{
		logger:         logger.With(zap.String("component", "dag_executor")),
		handlers:       make(map[string]NodeHandler),
		policy:         FailFast,
		defaultTimeout: 5 * time.Minute,
	}
}

func (e *DAGExecutor) RegisterHandler(name string, handler NodeHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[name] = handler
}

func (e *DAGExecutor) SetExecutionPolicy(policy ExecutionPolicy) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policy = policy
}

func (e *DAGExecutor) SetDefaultTimeout(timeout time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.defaultTimeout = timeout
}

type ExecutionResult struct {
	GraphName string
	Results   map[uuid.UUID]*NodeResult
	Success   bool
	Duration  time.Duration
}

func (e *DAGExecutor) Execute(ctx context.Context, graph *Graph) (*ExecutionResult, error) {
	levels, err := graph.Levels()
	if err != nil {
		return nil, fmt.Errorf("failed to compute execution levels: %w", err)
	}

	start := time.Now()
	results := make(map[uuid.UUID]*NodeResult)
	nodeOutputs := make(map[uuid.UUID]interface{})
	var mu sync.Mutex

	e.logger.Info("starting DAG execution",
		zap.String("graph", graph.String()),
		zap.Int("levels", len(levels)),
	)

	for _, level := range levels {
		select {
		case <-ctx.Done():
			return e.buildResult(graph, results, start), ctx.Err()
		default:
		}

		levelResults := e.executeLevel(ctx, graph, level, nodeOutputs, &mu)

		for id, result := range levelResults {
			results[id] = result
			if result.Status == NodeStatusCompleted && result.Output != nil {
				nodeOutputs[id] = result.Output
			}
		}

		hasFailure := false
		hasCriticalFailure := false
		for _, result := range levelResults {
			if result.Status == NodeStatusFailed {
				hasFailure = true
				if graph.Node(result.NodeID) != nil {
					hasCriticalFailure = true
				}
			}
		}

		e.mu.RLock()
		policy := e.policy
		e.mu.RUnlock()

		if hasCriticalFailure && policy == FailFast {
			e.logger.Warn("critical failure in level, stopping execution",
				zap.Int("level", len(levelResults)),
			)
			return e.buildResult(graph, results, start), nil
		}

		if hasFailure && policy == FailFast {
			skipped := e.skipDependents(graph, level, results)
			for id, r := range skipped {
				results[id] = r
			}
			return e.buildResult(graph, results, start), nil
		}
	}

	totalSuccess := true
	for _, r := range results {
		if r.Status == NodeStatusFailed {
			totalSuccess = false
			break
		}
	}

	return &ExecutionResult{
		GraphName: graph.String(),
		Results:   results,
		Success:   totalSuccess,
		Duration:  time.Since(start),
	}, nil
}

func (e *DAGExecutor) executeLevel(ctx context.Context, graph *Graph, level []*Node, nodeOutputs map[uuid.UUID]interface{}, mu *sync.Mutex) map[uuid.UUID]*NodeResult {
	results := make(map[uuid.UUID]*NodeResult)
	var wg sync.WaitGroup
	var levelMu sync.Mutex

	for _, node := range level {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()

			inputs := e.collectInputs(n, nodeOutputs, mu)

			select {
			case <-ctx.Done():
				levelMu.Lock()
				results[n.ID] = &NodeResult{
					NodeID:   n.ID,
					NodeName: n.Name,
					Status:   NodeStatusSkipped,
					Error:    "execution cancelled",
				}
				levelMu.Unlock()
				return
			default:
			}

			result := e.executeNode(ctx, n, inputs)
			levelMu.Lock()
			results[n.ID] = result
			levelMu.Unlock()

			mu.Lock()
			if result.Status == NodeStatusCompleted && result.Output != nil {
				nodeOutputs[n.ID] = result.Output
			}
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	return results
}

func (e *DAGExecutor) executeNode(ctx context.Context, node *Node, inputs map[uuid.UUID]interface{}) *NodeResult {
	start := time.Now()

	e.logger.Info("executing node",
		zap.String("node", node.Name),
		zap.String("node_id", node.ID.String()),
	)

	e.mu.RLock()
	handler, ok := e.handlers[node.Name]
	e.mu.RUnlock()

	if !ok {
		e.logger.Warn("no handler registered for node, marking as completed",
			zap.String("node", node.Name),
		)
		return &NodeResult{
			NodeID:   node.ID,
			NodeName: node.Name,
			Status:   NodeStatusCompleted,
			Duration: time.Since(start),
		}
	}

	timeout := e.resolveTimeout(node)
	nodeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	output, err := handler(nodeCtx, node, inputs)
	duration := time.Since(start)

	if err != nil {
		e.logger.Error("node execution failed",
			zap.String("node", node.Name),
			zap.Error(err),
			zap.Duration("duration", duration),
		)
		return &NodeResult{
			NodeID:   node.ID,
			NodeName: node.Name,
			Status:   NodeStatusFailed,
			Error:    err.Error(),
			Duration: duration,
		}
	}

	e.logger.Info("node execution completed",
		zap.String("node", node.Name),
		zap.Duration("duration", duration),
	)
	return &NodeResult{
		NodeID:   node.ID,
		NodeName: node.Name,
		Status:   NodeStatusCompleted,
		Output:   output,
		Duration: duration,
	}
}

func (e *DAGExecutor) collectInputs(node *Node, nodeOutputs map[uuid.UUID]interface{}, mu *sync.Mutex) map[uuid.UUID]interface{} {
	mu.Lock()
	defer mu.Unlock()

	inputs := make(map[uuid.UUID]interface{})
	for _, parent := range parentsFromGraph(node, nil) {
		if output, ok := nodeOutputs[parent.ID]; ok {
			inputs[parent.ID] = output
		}
	}
	return inputs
}

func parentsFromGraph(node *Node, graph *Graph) []*Node {
	if graph == nil {
		return nil
	}
	return graph.Parents(node.ID)
}

func (e *DAGExecutor) skipDependents(graph *Graph, failedLevel []*Node, results map[uuid.UUID]*NodeResult) map[uuid.UUID]*NodeResult {
	skipped := make(map[uuid.UUID]*NodeResult)
	visited := make(map[uuid.UUID]bool)

	var markSkipped func(id uuid.UUID)
	markSkipped = func(id uuid.UUID) {
		if visited[id] {
			return
		}
		visited[id] = true
		for _, child := range graph.Children(id) {
			if _, exists := results[child.ID]; !exists {
				skipped[child.ID] = &NodeResult{
					NodeID:   child.ID,
					NodeName: child.Name,
					Status:   NodeStatusSkipped,
					Error:    "dependency failed",
				}
			}
			markSkipped(child.ID)
		}
	}

	for _, n := range failedLevel {
		result := results[n.ID]
		if result != nil && result.Status == NodeStatusFailed {
			markSkipped(n.ID)
		}
	}

	return skipped
}

func (e *DAGExecutor) resolveTimeout(node *Node) time.Duration {
	if timeout, ok := node.Metadata["timeout"].(time.Duration); ok {
		return timeout
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.defaultTimeout
}

func (e *DAGExecutor) buildResult(graph *Graph, results map[uuid.UUID]*NodeResult, start time.Time) *ExecutionResult {
	success := true
	for _, r := range results {
		if r.Status == NodeStatusFailed {
			success = false
			break
		}
	}
	return &ExecutionResult{
		GraphName: graph.String(),
		Results:   results,
		Success:   success,
		Duration:  time.Since(start),
	}
}
