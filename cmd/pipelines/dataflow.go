package pipelines

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/databricks/cli/libs/cmdio"
	"github.com/databricks/cli/libs/log"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/client"
	"github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/spf13/cobra"
)

const (
	// gap between update-status polls while a triggered dry-run runs
	pollInterval = time.Second
	// grace for completed updated but no results
	graceDelay = 2 * time.Second
	// page size per entity endpoint
	nodesPageSize       = 20
	flowsPageSize       = 20
	diagnosticsPageSize = 20
	// window for an existing dry-run
	listUpdatesPageSize = 100

	errCodeInvalidStateTransition = "INVALID_STATE_TRANSITION"

	severityError = "ERROR"
	// messages for dry-run triggers
	triggerNotice      = "No readable dry-run found; triggering one (this starts a billed cluster). Press Ctrl-C to cancel."
	forceTriggerNotice = "Triggering a fresh dry-run (this starts a billed cluster). Press Ctrl-C to cancel."
)

var (
	errContinuous       = errors.New("the dataflow graph is unavailable for continuous pipelines")
	errNoDryRun         = errors.New("no dry-run found; re-run without --no-dry-run to trigger one")
	errActiveUpdate     = errors.New("an update is already active for this pipeline")
	errDryRunInProgress = errors.New("the latest dry-run is still in progress")
)

// hand-rolled response shapes
type dagDataset struct {
	Ref         string `json:"dataset_ref"`
	Name        string `json:"name"`
	FullName    string `json:"full_name"`
	DatasetType string `json:"dataset_type"`
}

type dagSink struct {
	Ref       string `json:"sink_ref"`
	Name      string `json:"name"`
	TableName string `json:"table_name"`
}

// graph vertex: either Dataset or Sink
type dagNode struct {
	Dataset *dagDataset `json:"dataset"`
	Sink    *dagSink    `json:"sink"`
}

type listNodesResponse struct {
	Nodes         []dagNode `json:"nodes"`
	NextPageToken string    `json:"next_page_token"`
}

// node a diagnostic relates to; exactly one name field is set
type dagDiagnosticNode struct {
	DatasetName string `json:"dataset_name"`
	SinkName    string `json:"sink_name"`
	FlowName    string `json:"flow_name"`
}

type dagDiagnostic struct {
	Severity    string `json:"severity"`
	Code        string `json:"code"`
	Message     string `json:"message"`
	DocumentURI string `json:"document_uri"`
	Range       struct {
		Start struct {
			Line int `json:"line"`
		} `json:"start"`
	} `json:"range"`
	RelatedNodes []dagDiagnosticNode `json:"related_pipeline_nodes"`
	Details      struct {
		// structured JVM error (error_class, sql_state)
		Exception struct {
			ErrorClass string `json:"error_class"`
			SQLState   string `json:"sql_state"`
		} `json:"exception"`
	} `json:"details"`
}

type listDiagnosticsResponse struct {
	Diagnostics   []dagDiagnostic `json:"diagnostics"`
	NextPageToken string          `json:"next_page_token"`
}

// dry-run mode flags shared by the pipeline-entities commands
type dagRunOpts struct {
	forceDryRun bool
	noDryRun    bool
}

// workspace operations resolveDryRun orchestrates
type updateDeps struct {
	continuous  func(ctx context.Context) (bool, error)
	listUpdates func(ctx context.Context) ([]pipelines.UpdateInfo, error)
	trigger     func(ctx context.Context) (updateID string, err error)
	poll        func(ctx context.Context, updateID string) (pipelines.UpdateInfoState, error)
	stop        func(ctx context.Context)
}

func isTerminal(s pipelines.UpdateInfoState) bool {
	switch s {
	case pipelines.UpdateInfoStateCompleted, pipelines.UpdateInfoStateFailed, pipelines.UpdateInfoStateCanceled:
		return true
	default:
		return false
	}
}

// most recent update (by creation time) matching filter, if any
// nil filter matches all updates
func newestUpdate(updates []pipelines.UpdateInfo, filter func(pipelines.UpdateInfo) bool) (pipelines.UpdateInfo, bool) {
	var best pipelines.UpdateInfo
	found := false
	for _, u := range updates {
		if filter != nil && !filter(u) {
			continue
		}
		if !found || u.CreationTime > best.CreationTime {
			best, found = u, true
		}
	}
	return best, found
}

func activeUpdateError(u pipelines.UpdateInfo) error {
	if u.ValidateOnly {
		return fmt.Errorf("%w: a dry-run is in progress (update %s); re-run once it finishes", errActiveUpdate, u.UpdateId)
	}
	return fmt.Errorf("%w: an update is running (update %s); a dry-run cannot start while an update is active, retry once it completes", errActiveUpdate, u.UpdateId)
}

// resolves validate-only update whose graph to read, triggers a fresh dry-run when needed
// returns the update id and terminal state. continuous/active-update return errors.
func resolveDryRun(ctx context.Context, d updateDeps, opts dagRunOpts) (id string, state pipelines.UpdateInfoState, err error) {
	// stop a running dry-run if the command is interrupted/erorr etc.
	signalCtx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	triggered := false
	defer func() {
		if triggered && err != nil {
			d.stop(ctx)
		}
	}()

	triggerAndPoll := func(notice string) (string, pipelines.UpdateInfoState, error) {
		// continuous pipeline cannot be dry-run
		if continuous, err := d.continuous(signalCtx); err != nil {
			return "", "", err
		} else if continuous {
			return "", "", errContinuous
		}
		cmdio.LogString(signalCtx, notice)
		newID, err := d.trigger(signalCtx)
		if err != nil {
			return "", "", mapTriggerError(signalCtx, d, err)
		}
		triggered = true
		st, err := d.poll(signalCtx, newID)
		return newID, st, err
	}

	if opts.forceDryRun {
		return triggerAndPoll(forceTriggerNotice)
	}

	updates, err := d.listUpdates(signalCtx)
	if err != nil {
		return "", "", err
	}

	latest, found := newestUpdate(updates, func(u pipelines.UpdateInfo) bool { return u.ValidateOnly })

	if found && latest.State == pipelines.UpdateInfoStateCompleted {
		return latest.UpdateId, latest.State, nil
	}

	if opts.noDryRun {
		switch {
		case found && !isTerminal(latest.State):
			// in progress update
			return "", "", fmt.Errorf("%w (update %s); re-run once it finishes", errDryRunInProgress, latest.UpdateId)
		case found:
			// failed/canceled dry-run: surface its diagnostics
			return latest.UpdateId, latest.State, nil
		default:
			return "", "", errNoDryRun
		}
	}

	// active update
	if u, ok := newestUpdate(updates, nil); ok && !isTerminal(u.State) {
		return "", "", activeUpdateError(u)
	}
	// trigger fresh dry-run
	return triggerAndPoll(triggerNotice)
}

func mapTriggerError(ctx context.Context, d updateDeps, err error) error {
	apiErr, ok := errors.AsType[*apierr.APIError](err)
	if !ok || apiErr.ErrorCode != errCodeInvalidStateTransition {
		return err
	}
	if updates, e := d.listUpdates(ctx); e == nil {
		if u, ok := newestUpdate(updates, nil); ok && !isTerminal(u.State) {
			return activeUpdateError(u)
		}
	}
	return err
}

// wires updateDeps to the workspace client for the given pipeline
func newUpdateDeps(w *databricks.WorkspaceClient, pipelineID string) updateDeps {
	return updateDeps{
		continuous: func(ctx context.Context) (bool, error) {
			resp, err := w.Pipelines.GetByPipelineId(ctx, pipelineID)
			if err != nil {
				return false, err
			}
			return resp.Spec != nil && resp.Spec.Continuous, nil
		},
		listUpdates: func(ctx context.Context) ([]pipelines.UpdateInfo, error) {
			resp, err := w.Pipelines.ListUpdates(ctx, pipelines.ListUpdatesRequest{
				PipelineId: pipelineID,
				MaxResults: listUpdatesPageSize,
			})
			if err != nil {
				return nil, err
			}
			return resp.Updates, nil
		},
		trigger: func(ctx context.Context) (string, error) {
			res, err := w.Pipelines.StartUpdate(ctx, pipelines.StartUpdate{
				PipelineId:   pipelineID,
				ValidateOnly: true,
			})
			if err != nil {
				return "", err
			}
			return res.UpdateId, nil
		},
		poll: func(ctx context.Context, updateID string) (pipelines.UpdateInfoState, error) {
			return pollUpdate(ctx, w, pipelineID, updateID)
		},
		stop: func(ctx context.Context) { stopUpdate(ctx, w, pipelineID) },
	}
}

// polls the update until it reaches a terminal state or ctx ends
func pollUpdate(ctx context.Context, w *databricks.WorkspaceClient, pipelineID, updateID string) (pipelines.UpdateInfoState, error) {
	for {
		resp, err := w.Pipelines.GetUpdateByPipelineIdAndUpdateId(ctx, pipelineID, updateID)
		if err != nil {
			return "", err
		}
		if resp.Update != nil && isTerminal(resp.Update.State) {
			return resp.Update.State, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// best-effort cancels a triggered dry-run
func stopUpdate(ctx context.Context, w *databricks.WorkspaceClient, pipelineID string) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	cmdio.LogString(cleanupCtx, "Stopping the dry-run that was triggered...")
	if _, err := w.Pipelines.Stop(cleanupCtx, pipelines.StopRequest{PipelineId: pipelineID}); err != nil {
		log.Warnf(cleanupCtx, "failed to stop the dry-run for %s (a billed cluster may still be running): %v", pipelineID, err)
	}
}

// issues GET path with page_size + page_token until the response has no next token
func fetchAllPages[R, T any](ctx context.Context, c *client.DatabricksClient, headers map[string]string, path, updateID string, pageSize int, items func(*R) []T, next func(*R) string) ([]T, error) {
	var out []T
	token := ""
	for {
		query := map[string]string{
			"update_id": updateID,
			"page_size": strconv.Itoa(pageSize),
		}
		if token != "" {
			query["page_token"] = token
		}
		var resp R
		if err := c.Do(ctx, http.MethodGet, path, headers, nil, query, &resp); err != nil {
			return nil, err
		}
		out = append(out, items(&resp)...)
		token = next(&resp)
		if token == "" {
			return out, nil
		}
	}
}

func graphPath(pipelineID, leaf string) string {
	return fmt.Sprintf("/api/2.0/pipelines/%s/entities/%s", pipelineID, leaf)
}

// returns the pipeline's datasets
func fetchDatasets(ctx context.Context, c *client.DatabricksClient, headers map[string]string, pipelineID, updateID string) ([]dagDataset, error) {
	nodes, err := fetchNodes(ctx, c, headers, pipelineID, updateID)
	if err != nil {
		return nil, err
	}
	datasets := datasetsFromNodes(nodes)
	if len(datasets) > 0 {
		return datasets, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(graceDelay):
	}
	nodes, err = fetchNodes(ctx, c, headers, pipelineID, updateID)
	if err != nil {
		return nil, err
	}
	return datasetsFromNodes(nodes), nil
}

// returns the pipeline's graph nodes (datasets and sinks) for the update
func fetchNodes(ctx context.Context, c *client.DatabricksClient, headers map[string]string, pipelineID, updateID string) ([]dagNode, error) {
	return fetchAllPages(ctx, c, headers, graphPath(pipelineID, "nodes"), updateID, nodesPageSize,
		func(r *listNodesResponse) []dagNode { return r.Nodes },
		func(r *listNodesResponse) string { return r.NextPageToken })
}

// keeps only the dataset nodes, dropping sinks
func datasetsFromNodes(nodes []dagNode) []dagDataset {
	datasets := make([]dagDataset, 0, len(nodes))
	for _, n := range nodes {
		if n.Dataset != nil {
			datasets = append(datasets, *n.Dataset)
		}
	}
	return datasets
}

func fetchDiagnostics(ctx context.Context, c *client.DatabricksClient, headers map[string]string, pipelineID, updateID string) ([]dagDiagnostic, error) {
	return fetchAllPages(ctx, c, headers, graphPath(pipelineID, "diagnostics"), updateID, diagnosticsPageSize,
		func(r *listDiagnosticsResponse) []dagDiagnostic { return r.Diagnostics },
		func(r *listDiagnosticsResponse) string { return r.NextPageToken })
}

// gets the diagnostics for a non-completed update
func nonCompletedError(ctx context.Context, c *client.DatabricksClient, headers map[string]string, pipelineID, updateID string, state pipelines.UpdateInfoState) error {
	if state == pipelines.UpdateInfoStateCanceled {
		return errors.New("dry-run was canceled")
	}
	diagnostics, err := fetchDiagnostics(ctx, c, headers, pipelineID, updateID)
	if err != nil {
		return fmt.Errorf("dry-run failed; could not fetch diagnostics: %w", err)
	}
	return fmt.Errorf("dry-run failed: %s", diagnosticsSummary(diagnostics))
}

// joins the error-severity diagnostics into a single line
func diagnosticsSummary(diagnostics []dagDiagnostic) string {
	var parts []string
	for _, d := range diagnostics {
		if d.Severity != severityError {
			continue
		}
		parts = append(parts, formatDiagnostic(d))
	}
	if len(parts) == 0 {
		return "no error diagnostics returned"
	}
	return strings.Join(parts, "; ")
}

func formatDiagnostic(d dagDiagnostic) string {
	msg := d.Message
	if target := diagnosticTarget(d.RelatedNodes); target != "" {
		msg = target + ": " + msg
	}
	// prefer the structured error identifiers (error_class [sql_state]) over the bare code
	if id := exceptionID(d); id != "" {
		msg = fmt.Sprintf("%s [%s]", msg, id)
	} else if d.Code != "" {
		msg = fmt.Sprintf("%s (%s)", msg, d.Code)
	}
	if d.DocumentURI != "" {
		// the API reports zero-based lines; add 1 so it matches the user's 1-based editor
		msg = fmt.Sprintf("%s at %s:%d", msg, d.DocumentURI, d.Range.Start.Line+1)
	}
	return msg
}

// joins the structured exception's error_class and sql_state (either may be absent) into a single identifier
func exceptionID(d dagDiagnostic) string {
	ex := d.Details.Exception
	switch {
	case ex.ErrorClass != "" && ex.SQLState != "":
		return fmt.Sprintf("%s %s", ex.ErrorClass, ex.SQLState)
	case ex.ErrorClass != "":
		return ex.ErrorClass
	case ex.SQLState != "":
		return ex.SQLState
	default:
		return ""
	}
}

func diagnosticTarget(nodes []dagDiagnosticNode) string {
	for _, n := range nodes {
		switch {
		case n.DatasetName != "":
			return n.DatasetName
		case n.SinkName != "":
			return n.SinkName
		case n.FlowName != "":
			return n.FlowName
		}
	}
	return ""
}

// writes v as indented JSON to the command's stdout with a trailing newline
func renderJSON(cmd *cobra.Command, v any) error {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = cmd.OutOrStdout().Write(append(out, '\n'))
	return err
}

// returns a non-nil slice so JSON output renders [] rather than null
func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
