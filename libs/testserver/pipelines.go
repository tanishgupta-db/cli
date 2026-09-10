package testserver

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/pipelines"
)

// dataflowGraphMaxPageSize is the per-leaf page-size cap the backend enforces: it rejects (does not
// clamp) a larger page_size with a 400, so the fake does the same.
var dataflowGraphMaxPageSize = map[string]int{"nodes": 50, "flows": 50, "diagnostics": 20}

// PipelineUpdate is a stored pipeline update. StartUpdate seeds one in a terminal COMPLETED state;
// tests seed failed or in-flight updates via SeedPipelineUpdate.
type PipelineUpdate struct {
	PipelineId   string
	UpdateId     string
	State        pipelines.UpdateInfoState
	ValidateOnly bool
	CreationTime int64
}

// DataflowGraph is the dry-run graph a pipeline exposes via the pipeline entities endpoints. It has
// no create API, so tests seed it with SetPipelineGraph.
type DataflowGraph struct {
	Nodes       []DataflowGraphNode
	Flows       []DataflowGraphFlow
	Diagnostics []DataflowGraphDiagnostic
}

type DataflowGraphNode struct {
	Dataset *DataflowGraphDataset `json:"dataset,omitempty"`
	Sink    *DataflowGraphSink    `json:"sink,omitempty"`
}

type DataflowGraphDataset struct {
	DatasetRef  string `json:"dataset_ref,omitempty"`
	Name        string `json:"name,omitempty"`
	FullName    string `json:"full_name,omitempty"`
	DatasetType string `json:"dataset_type,omitempty"`
}

type DataflowGraphSink struct {
	SinkRef   string `json:"sink_ref,omitempty"`
	Name      string `json:"name,omitempty"`
	TableName string `json:"table_name,omitempty"`
}

type DataflowGraphFlow struct {
	InputNodeRefs []string `json:"input_node_refs,omitempty"`
	OutputNodeRef string   `json:"output_node_ref,omitempty"`
}

type DataflowGraphDiagnostic struct {
	Severity     string                          `json:"severity,omitempty"`
	Code         string                          `json:"code,omitempty"`
	Message      string                          `json:"message,omitempty"`
	RelatedNodes []DataflowGraphDiagnosticNode   `json:"related_pipeline_nodes,omitempty"`
	Details      *DataflowGraphDiagnosticDetails `json:"details,omitempty"`
}

type DataflowGraphDiagnosticDetails struct {
	Exception *DataflowGraphDiagnosticException `json:"exception,omitempty"`
}

type DataflowGraphDiagnosticException struct {
	ErrorClass string `json:"error_class,omitempty"`
	SQLState   string `json:"sql_state,omitempty"`
}

type DataflowGraphDiagnosticNode struct {
	DatasetName string `json:"dataset_name,omitempty"`
	SinkName    string `json:"sink_name,omitempty"`
	FlowName    string `json:"flow_name,omitempty"`
}

func (s *FakeWorkspace) PipelineGet(pipelineId string) Response {
	defer s.LockUnlock()()

	value, ok := s.Pipelines[pipelineId]
	if !ok {
		return Response{
			StatusCode: 404,
			Body:       map[string]string{"message": fmt.Sprintf("The specified pipeline %s was not found.", pipelineId)},
		}
	}
	return Response{
		Body: value,
	}
}

func (s *FakeWorkspace) PipelineCreate(req Request) Response {
	defer s.LockUnlock()()

	spec := pipelines.PipelineSpec{}
	err := json.Unmarshal(req.Body, &spec)
	if err != nil {
		return Response{
			Body:       fmt.Sprintf("cannot unmarshal request body: %s", err),
			StatusCode: 400,
		}
	}

	// Unity Catalog requires target_schema_name to be a single schema segment, so a
	// dotted catalog.schema value is rejected; the catalog belongs in the separate
	// catalog field. Only the dot trips this check, but the backend's canned error
	// also lists dashes and other characters as invalid.
	if strings.Contains(spec.Target, ".") {
		return Response{
			StatusCode: 400,
			Body: map[string]string{
				"error_code": "INVALID_PARAMETER_VALUE",
				"message":    fmt.Sprintf("CreatePipeline target_schema_name %q is not a valid name. Valid names must contain only alphanumeric characters and underscores, and cannot contain spaces, periods, forward slashes, or control characters.", spec.Target),
			},
		}
	}

	var r pipelines.GetPipelineResponse
	r.Spec = &spec

	// parameters is not on PipelineSpec (only on CreatePipeline), so the decode above
	// drops it. The backend echoes it on GetPipelineResponse.Parameters; mirror that
	// here, else a re-read misses it and the CLI plans a perpetual update.
	var create pipelines.CreatePipeline
	if err := json.Unmarshal(req.Body, &create); err != nil {
		return Response{
			Body:       fmt.Sprintf("cannot unmarshal request body: %s", err),
			StatusCode: 400,
		}
	}
	r.Parameters = create.Parameters

	pipelineId := nextUUID()
	r.PipelineId = pipelineId
	r.CreatorUserName = "tester@databricks.com"
	r.LastModified = nowMilli()
	r.Name = r.Spec.Name
	// run_as is on CreatePipeline, not PipelineSpec, so the spec decode drops it. The backend
	// echoes it top-level on GetPipelineResponse.RunAs; mirror that so a re-read is faithful.
	if create.RunAs != nil {
		r.RunAs = create.RunAs
	}
	r.RunAsUserName = "tester@databricks.com"
	r.State = "IDLE"
	r.EffectivePublishingMode = pipelines.PublishingModeDefaultPublishingMode

	setSpecDefaults(&spec, pipelineId)
	s.Pipelines[pipelineId] = r

	return Response{
		Body: pipelines.CreatePipelineResponse{
			PipelineId: pipelineId,
		},
	}
}

func setSpecDefaults(spec *pipelines.PipelineSpec, pipelineId string) {
	spec.Id = pipelineId
	// If the pipeline definition does not specify a catalog, it switches to Hive metastore mode
	// and if the storage location is not specified, API automatically generates a storage location
	// (ref: https://docs.databricks.com/gcp/en/dlt/hive-metastore#specify-a-storage-location)
	if spec.Storage == "" && spec.Catalog == "" {
		spec.Storage = "dbfs:/pipelines/" + pipelineId
	}
}

func (s *FakeWorkspace) PipelineUpdate(req Request, pipelineId string) Response {
	defer s.LockUnlock()()

	var spec pipelines.PipelineSpec
	err := json.Unmarshal(req.Body, &spec)
	if err != nil {
		return Response{
			Body:       fmt.Sprintf("internal error: %s", err),
			StatusCode: 400,
		}
	}

	item, exists := s.Pipelines[pipelineId]
	if !exists {
		return Response{
			StatusCode: 404,
		}
	}

	// parameters is on EditPipeline, not PipelineSpec; round-trip it like
	// PipelineCreate does.
	var edit pipelines.EditPipeline
	if err := json.Unmarshal(req.Body, &edit); err != nil {
		return Response{
			Body:       fmt.Sprintf("internal error: %s", err),
			StatusCode: 400,
		}
	}

	item.Spec = &spec
	item.Parameters = edit.Parameters
	// The backend echoes the spec name on GetPipelineResponse.Name; mirror that so a
	// rename is reflected on the next read.
	item.Name = spec.Name
	// run_as is on EditPipeline, not PipelineSpec; keep it in sync like Parameters so an edit
	// that changes run_as is reflected on the next read (matches cloud top-level echo).
	if edit.RunAs != nil {
		item.RunAs = edit.RunAs
	}
	setSpecDefaults(&spec, pipelineId)
	s.Pipelines[pipelineId] = item

	return Response{}
}

func (s *FakeWorkspace) PipelineStartUpdate(req Request, pipelineId string) Response {
	defer s.LockUnlock()()

	_, exists := s.Pipelines[pipelineId]
	if !exists {
		return Response{
			StatusCode: 404,
			Body:       map[string]string{"message": fmt.Sprintf("The specified pipeline %s was not found.", pipelineId)},
		}
	}

	// The body is optional (a plain run omits validate_only), so ignore unmarshal errors.
	var body pipelines.StartUpdate
	_ = json.Unmarshal(req.Body, &body)

	updateId := nextUUID()
	// Default to a terminal COMPLETED state; tests seed other states via SeedPipelineUpdate. Stamp
	// creation_time so newestUpdate has a real ordering key rather than tying at zero.
	s.PipelineUpdates[updateId] = &PipelineUpdate{
		PipelineId:   pipelineId,
		UpdateId:     updateId,
		State:        pipelines.UpdateInfoStateCompleted,
		ValidateOnly: body.ValidateOnly,
		CreationTime: nowMilli(),
	}

	// Seed a deterministic graph so acceptance tests, which drive the API but can't call
	// SetPipelineGraph, read a non-empty one.
	if s.PipelineGraphs[pipelineId] == nil {
		s.PipelineGraphs[pipelineId] = defaultDataflowGraph()
	}

	return Response{
		Body: pipelines.StartUpdateResponse{
			UpdateId: updateId,
		},
	}
}

func (s *FakeWorkspace) PipelineEvents(pipelineId string) Response {
	defer s.LockUnlock()()

	_, exists := s.Pipelines[pipelineId]
	if !exists {
		return Response{
			StatusCode: 404,
			Body:       map[string]string{"message": fmt.Sprintf("The specified pipeline %s was not found.", pipelineId)},
		}
	}

	return Response{
		Body: map[string]any{
			"events": []pipelines.PipelineEvent{},
		},
	}
}

func (s *FakeWorkspace) PipelineGetUpdate(pipelineId, updateId string) Response {
	defer s.LockUnlock()()

	_, exists := s.Pipelines[pipelineId]
	if !exists {
		return Response{
			StatusCode: 404,
			Body:       map[string]string{"message": fmt.Sprintf("The specified pipeline %s was not found.", pipelineId)},
		}
	}

	// Check if the update exists
	update, updateExists := s.PipelineUpdates[updateId]
	if !updateExists {
		return Response{
			StatusCode: 404,
			Body:       map[string]string{"message": fmt.Sprintf("The specified update %s was not found.", updateId)},
		}
	}

	return Response{
		Body: pipelines.GetUpdateResponse{
			Update: &pipelines.UpdateInfo{
				UpdateId:     update.UpdateId,
				State:        update.State,
				ValidateOnly: update.ValidateOnly,
				CreationTime: update.CreationTime,
			},
		},
	}
}

func (s *FakeWorkspace) PipelineStop(pipelineId string) Response {
	defer s.LockUnlock()()

	_, exists := s.Pipelines[pipelineId]
	if !exists {
		return Response{
			StatusCode: 404,
			Body:       map[string]string{"message": fmt.Sprintf("The specified pipeline %s was not found.", pipelineId)},
		}
	}

	return Response{
		Body: pipelines.GetPipelineResponse{
			PipelineId: pipelineId,
			State:      pipelines.PipelineStateIdle,
		},
	}
}

// PipelineListUpdates lists a pipeline's updates, newest first, honoring max_results.
func (s *FakeWorkspace) PipelineListUpdates(req Request, pipelineId string) Response {
	defer s.LockUnlock()()

	_, exists := s.Pipelines[pipelineId]
	if !exists {
		return Response{
			StatusCode: 404,
			Body:       map[string]string{"message": fmt.Sprintf("The specified pipeline %s was not found.", pipelineId)},
		}
	}

	var updates []pipelines.UpdateInfo
	for _, u := range s.PipelineUpdates {
		if u.PipelineId != pipelineId {
			continue
		}
		updates = append(updates, pipelines.UpdateInfo{
			UpdateId:     u.UpdateId,
			State:        u.State,
			ValidateOnly: u.ValidateOnly,
			CreationTime: u.CreationTime,
		})
	}
	// Newest first: descending CreationTime, so flip the comparator operands.
	slices.SortFunc(updates, func(a, b pipelines.UpdateInfo) int { return cmp.Compare(b.CreationTime, a.CreationTime) })

	if raw := req.URL.Query().Get("max_results"); raw != "" {
		if maxResults, err := strconv.Atoi(raw); err == nil && maxResults > 0 && maxResults < len(updates) {
			updates = updates[:maxResults]
		}
	}

	return Response{
		Body: pipelines.ListUpdatesResponse{
			Updates: updates,
		},
	}
}

// PipelineDataflowGraph returns one page of the pipeline's seeded dataflow graph for leaf (nodes,
// flows, or diagnostics), driving the CLI's pagination loop.
func (s *FakeWorkspace) PipelineDataflowGraph(req Request, pipelineId, leaf string) Response {
	defer s.LockUnlock()()

	_, exists := s.Pipelines[pipelineId]
	if !exists {
		return Response{
			StatusCode: 404,
			Body:       map[string]string{"message": fmt.Sprintf("The specified pipeline %s was not found.", pipelineId)},
		}
	}

	if maxPageSize, ok := dataflowGraphMaxPageSize[leaf]; ok {
		if size, _ := strconv.Atoi(req.URL.Query().Get("page_size")); size > maxPageSize {
			return Response{StatusCode: 400, Body: map[string]string{
				"error_code": "INVALID_PARAMETER_VALUE",
				"message":    fmt.Sprintf("Invalid page size. The page size must be between 1 and %d", maxPageSize),
			}}
		}
	}

	graph := s.PipelineGraphs[pipelineId]
	if graph == nil {
		graph = &DataflowGraph{}
	}

	switch leaf {
	case "nodes":
		page, next := paginate(graph.Nodes, req)
		return Response{Body: map[string]any{"nodes": page, "next_page_token": next}}
	case "flows":
		page, next := paginate(graph.Flows, req)
		return Response{Body: map[string]any{"flows": page, "next_page_token": next}}
	case "diagnostics":
		page, next := paginate(graph.Diagnostics, req)
		return Response{Body: map[string]any{"diagnostics": page, "next_page_token": next}}
	default:
		return Response{StatusCode: 404}
	}
}

// paginate returns the page of items selected by the page_size and page_token query params, plus the
// next-page token (empty once the slice is exhausted).
func paginate[T any](items []T, req Request) ([]T, string) {
	start := 0
	if tok := req.URL.Query().Get("page_token"); tok != "" {
		start, _ = strconv.Atoi(tok)
	}
	if start > len(items) {
		start = len(items)
	}
	end := len(items)
	if size, err := strconv.Atoi(req.URL.Query().Get("page_size")); err == nil && size > 0 && start+size < end {
		end = start + size
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[start:end], next
}

// defaultDataflowGraph is the deterministic canned graph a dry-run produces for acceptance tests,
// which drive the API but can't call SetPipelineGraph. It mirrors the real backend's identifier
// shapes: a dataset's name is unquoted-dotted while its full_name is per-segment backtick-quoted; a
// view has a populated name but empty full_name; a sink carries its identifier in table_name.
func defaultDataflowGraph() *DataflowGraph {
	ds := func(ref, name, fullName, datasetType string) DataflowGraphNode {
		return DataflowGraphNode{Dataset: &DataflowGraphDataset{
			DatasetRef:  ref,
			Name:        name,
			FullName:    fullName,
			DatasetType: datasetType,
		}}
	}
	return &DataflowGraph{
		Nodes: []DataflowGraphNode{
			ds("n1", "main.demo.source", "`main`.`demo`.`source`", "MATERIALIZED_VIEW"),
			ds("n2", "main.demo.filtered", "`main`.`demo`.`filtered`", "MATERIALIZED_VIEW"),
			ds("n3", "main.demo.aggregated", "`main`.`demo`.`aggregated`", "MATERIALIZED_VIEW"),
			ds("n4", "recent", "", "VIEW"),
			{Sink: &DataflowGraphSink{SinkRef: "s1", TableName: "main.demo.archive"}},
		},
		Flows: []DataflowGraphFlow{
			{InputNodeRefs: []string{"n1"}, OutputNodeRef: "n2"},
			{InputNodeRefs: []string{"n1"}, OutputNodeRef: "n3"},
			{InputNodeRefs: []string{"n2"}, OutputNodeRef: "n4"},
			{InputNodeRefs: []string{"n3"}, OutputNodeRef: "s1"},
		},
	}
}

// SetPipelineGraph seeds the dataflow graph returned for a pipeline's dry-run.
func (s *FakeWorkspace) SetPipelineGraph(pipelineId string, graph *DataflowGraph) {
	defer s.LockUnlock()()
	s.PipelineGraphs[pipelineId] = graph
}

// SeedPipelineUpdate stores a pre-existing update that StartUpdate would not otherwise produce (a
// failed or in-flight one).
func (s *FakeWorkspace) SeedPipelineUpdate(update *PipelineUpdate) {
	defer s.LockUnlock()()
	s.PipelineUpdates[update.UpdateId] = update
}
