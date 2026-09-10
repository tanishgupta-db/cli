package pipelines

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/databricks/cli/libs/cmdio"
	"github.com/databricks/cli/libs/flags"
	"github.com/databricks/cli/libs/testserver"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests drive the real datasets/lineage command flow over HTTP against the testserver fake.
// The client and the seeding accessor share e2eToken so they hit the same fake workspace.
const e2eToken = "e2etoken"

func newDataflowServer(t *testing.T) (*testserver.Server, *databricks.WorkspaceClient) {
	t.Helper()
	server := testserver.New(t)
	testserver.AddDefaultHandlers(server)
	w, err := databricks.NewWorkspaceClient(&databricks.Config{Host: server.URL, Token: e2eToken})
	require.NoError(t, err)
	return server, w
}

func createDataflowPipeline(ctx context.Context, t *testing.T, w *databricks.WorkspaceClient, continuous bool) string {
	t.Helper()
	resp, err := w.Pipelines.Create(ctx, pipelines.CreatePipeline{
		Name:       "dag-test",
		Serverless: true,
		Continuous: continuous,
	})
	require.NoError(t, err)
	return resp.PipelineId
}

// sampleGraph is a small graph: main.s.orders feeds both main.s.orders_by_date and main.s.big_orders.
func sampleGraph() *testserver.DataflowGraph {
	ds := func(ref, fullName string) testserver.DataflowGraphNode {
		return testserver.DataflowGraphNode{Dataset: &testserver.DataflowGraphDataset{
			DatasetRef:  ref,
			FullName:    fullName,
			DatasetType: "MATERIALIZED_VIEW",
		}}
	}
	return &testserver.DataflowGraph{
		Nodes: []testserver.DataflowGraphNode{
			ds("n1", "main.s.orders"),
			ds("n2", "main.s.orders_by_date"),
			ds("n3", "main.s.big_orders"),
		},
		Flows: []testserver.DataflowGraphFlow{
			{InputNodeRefs: []string{"n1"}, OutputNodeRef: "n2"},
			{InputNodeRefs: []string{"n1"}, OutputNodeRef: "n3"},
		},
	}
}

func TestDatasetsE2EPaginates(t *testing.T) {
	ctx, cmd, buf := renderCmd(t, flags.OutputJSON)
	server, w := newDataflowServer(t)
	pipelineID := createDataflowPipeline(ctx, t, w, false)

	// More datasets than one page (nodesPageSize) so fetchAllPages must follow the page token.
	nodes := make([]testserver.DataflowGraphNode, 60)
	for i := range nodes {
		nodes[i] = testserver.DataflowGraphNode{Dataset: &testserver.DataflowGraphDataset{
			DatasetRef:  "n" + strconv.Itoa(i),
			FullName:    "main.s.t" + strconv.Itoa(i),
			DatasetType: "MATERIALIZED_VIEW",
		}}
	}
	server.Workspace(e2eToken).SetPipelineGraph(pipelineID, &testserver.DataflowGraph{Nodes: nodes})

	require.NoError(t, runDatasets(ctx, cmd, w, pipelineID, "key", dagRunOpts{}))

	var got []dagDataset
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	assert.Len(t, got, 60)
}

func TestDatasetsE2ERendersNamesCleanly(t *testing.T) {
	ctx, cmd, buf := renderCmd(t, flags.OutputText)
	server, w := newDataflowServer(t)
	pipelineID := createDataflowPipeline(ctx, t, w, false)
	// real backend shapes: backticked full_name dataset, empty-full_name view, nameless sink
	server.Workspace(e2eToken).SetPipelineGraph(pipelineID, &testserver.DataflowGraph{
		Nodes: []testserver.DataflowGraphNode{
			{Dataset: &testserver.DataflowGraphDataset{DatasetRef: "n1", Name: "main.s.orders", FullName: "`main`.`s`.`orders`", DatasetType: "MATERIALIZED_VIEW"}},
			{Dataset: &testserver.DataflowGraphDataset{DatasetRef: "n2", Name: "recent", DatasetType: "VIEW"}},
			{Sink: &testserver.DataflowGraphSink{SinkRef: "s1", TableName: "main.s.archive"}},
		},
	})

	require.NoError(t, runDatasets(ctx, cmd, w, pipelineID, "key", dagRunOpts{}))
	assert.Equal(t, "Name            Type\nmain.s.orders   MATERIALIZED_VIEW\nrecent          VIEW\nmain.s.archive  SINK\n", buf.String())
}

func TestDatasetsE2ETriggeredDryRunStreamsProgress(t *testing.T) {
	// A triggered dry-run streams the update URL and terminal Update ID to stderr, leaving the
	// dataset table on stdout.
	stderr := &bytes.Buffer{}
	cmd := &cobra.Command{}
	out := flags.OutputText
	cmd.Flags().Var(&out, "output", "")
	stdout := &bytes.Buffer{}
	cmd.SetOut(stdout)
	ctx := cmdio.InContext(t.Context(), cmdio.NewIO(t.Context(), flags.OutputText, nil, stdout, stderr, "", ""))

	server, w := newDataflowServer(t)
	pipelineID := createDataflowPipeline(ctx, t, w, false)
	server.Workspace(e2eToken).SetPipelineGraph(pipelineID, &testserver.DataflowGraph{
		Nodes: []testserver.DataflowGraphNode{
			{Dataset: &testserver.DataflowGraphDataset{DatasetRef: "n1", Name: "main.s.orders", DatasetType: "MATERIALIZED_VIEW"}},
		},
	})

	require.NoError(t, runDatasets(ctx, cmd, w, pipelineID, "key", dagRunOpts{forceDryRun: true}))
	progress := stderr.String()
	assert.Contains(t, progress, "Update URL: ")
	assert.Contains(t, progress, "/updates/")
	assert.Contains(t, progress, "Update ID: ")
	assert.Equal(t, "Name           Type\nmain.s.orders  MATERIALIZED_VIEW\n", stdout.String())
}

func TestDatasetsE2EFailedDryRunSurfacesDiagnostics(t *testing.T) {
	ctx, cmd, _ := renderCmd(t, flags.OutputText)
	server, w := newDataflowServer(t)
	pipelineID := createDataflowPipeline(ctx, t, w, false)
	ws := server.Workspace(e2eToken)
	ws.SeedPipelineUpdate(&testserver.PipelineUpdate{
		PipelineId:   pipelineID,
		UpdateId:     "failed-dry-run",
		State:        pipelines.UpdateInfoStateFailed,
		ValidateOnly: true,
		CreationTime: 1,
	})
	ws.SetPipelineGraph(pipelineID, &testserver.DataflowGraph{
		Diagnostics: []testserver.DataflowGraphDiagnostic{
			{
				Severity:     "ERROR",
				Code:         "TABLE_NOT_FOUND",
				Message:      "missing table",
				RelatedNodes: []testserver.DataflowGraphDiagnosticNode{{DatasetName: "main.s.missing"}},
			},
		},
	})

	err := runDatasets(ctx, cmd, w, pipelineID, "key", dagRunOpts{noDryRun: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dry-run failed")
	assert.Contains(t, err.Error(), "missing table")
	assert.Contains(t, err.Error(), "TABLE_NOT_FOUND")
}

func TestDatasetsE2EFailedDryRunSurfacesExceptionDetail(t *testing.T) {
	ctx, cmd, _ := renderCmd(t, flags.OutputText)
	server, w := newDataflowServer(t)
	pipelineID := createDataflowPipeline(ctx, t, w, false)
	ws := server.Workspace(e2eToken)
	ws.SeedPipelineUpdate(&testserver.PipelineUpdate{
		PipelineId:   pipelineID,
		UpdateId:     "failed-dry-run",
		State:        pipelines.UpdateInfoStateFailed,
		ValidateOnly: true,
		CreationTime: 1,
	})
	// The structured exception's error_class and sql_state must surface, since they pinpoint the
	// failure where the top-level message is generic.
	ws.SetPipelineGraph(pipelineID, &testserver.DataflowGraph{
		Diagnostics: []testserver.DataflowGraphDiagnostic{
			{
				Severity: "ERROR",
				Code:     "TABLE_NOT_FOUND",
				Message:  "query failed",
				Details: &testserver.DataflowGraphDiagnosticDetails{
					Exception: &testserver.DataflowGraphDiagnosticException{
						ErrorClass: "UNRESOLVED_COLUMN",
						SQLState:   "42703",
					},
				},
			},
		},
	})

	err := runDatasets(ctx, cmd, w, pipelineID, "key", dagRunOpts{noDryRun: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UNRESOLVED_COLUMN 42703")
}

func TestDatasetsE2EContinuousPipelineErrors(t *testing.T) {
	ctx, cmd, _ := renderCmd(t, flags.OutputText)
	server, w := newDataflowServer(t)
	pipelineID := createDataflowPipeline(ctx, t, w, true)
	server.Workspace(e2eToken).SetPipelineGraph(pipelineID, sampleGraph())

	err := runDatasets(ctx, cmd, w, pipelineID, "key", dagRunOpts{})
	assert.ErrorIs(t, err, errContinuous)
}

func TestDatasetsE2EActiveUpdateErrors(t *testing.T) {
	ctx, cmd, _ := renderCmd(t, flags.OutputText)
	server, w := newDataflowServer(t)
	pipelineID := createDataflowPipeline(ctx, t, w, false)
	server.Workspace(e2eToken).SeedPipelineUpdate(&testserver.PipelineUpdate{
		PipelineId:   pipelineID,
		UpdateId:     "running",
		State:        pipelines.UpdateInfoStateRunning,
		ValidateOnly: false,
		CreationTime: 1,
	})

	err := runDatasets(ctx, cmd, w, pipelineID, "key", dagRunOpts{})
	require.ErrorIs(t, err, errActiveUpdate)
	assert.Contains(t, err.Error(), "an update is running")
}

func TestDatasetsE2ENoDryRunInProgressErrors(t *testing.T) {
	ctx, cmd, _ := renderCmd(t, flags.OutputText)
	server, w := newDataflowServer(t)
	pipelineID := createDataflowPipeline(ctx, t, w, false)
	// The newest validate-only update is still running; --no-dry-run must report it as in-progress,
	// not fetch diagnostics and render "dry-run failed".
	server.Workspace(e2eToken).SeedPipelineUpdate(&testserver.PipelineUpdate{
		PipelineId:   pipelineID,
		UpdateId:     "running-dry-run",
		State:        pipelines.UpdateInfoStateRunning,
		ValidateOnly: true,
		CreationTime: 1,
	})

	err := runDatasets(ctx, cmd, w, pipelineID, "key", dagRunOpts{noDryRun: true})
	require.ErrorIs(t, err, errDryRunInProgress)
	assert.NotContains(t, err.Error(), "failed")
}
