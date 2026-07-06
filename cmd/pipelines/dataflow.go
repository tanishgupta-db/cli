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
	// pollInterval is the gap between update-status polls while a triggered dry-run runs.
	pollInterval = time.Second
	// graceDelay is the single re-poll wait when a COMPLETED update reads back an empty graph
	// (entity aggregation can finalize just after the terminal commit).
	graceDelay = 2 * time.Second
	// graphPageSize is the page size for the entities list endpoints. It MUST be sent: the
	// server treats an omitted or zero page_size as zero rows.
	graphPageSize = 50
	// listUpdatesPageSize bounds how far back we look for the latest dry-run. A dry-run older than
	// this many updates is effectively stale, so default mode triggers a fresh one instead.
	listUpdatesPageSize = 100

	errCodeInvalidStateTransition = "INVALID_STATE_TRANSITION"
	// triggerNotice is shown when default mode collapses a missing/failed dry-run into a fresh one;
	// forceTriggerNotice when --force-dry-run always triggers. Both announce the billed cluster.
	triggerNotice      = "No readable dry-run found; triggering one (this starts a billed cluster). Press Ctrl-C to cancel."
	forceTriggerNotice = "Triggering a fresh dry-run (this starts a billed cluster). Press Ctrl-C to cancel."
)

var (
	errContinuous       = errors.New("the dataflow graph is unavailable for continuous pipelines")
	errNoDryRun         = errors.New("no dry-run found; re-run without --no-dry-run to trigger one")
	errActiveUpdate     = errors.New("an update is already active for this pipeline")
	errDryRunInProgress = errors.New("the latest dry-run is still in progress")
)

// Response shapes for the entities list endpoints, hand-rolled because the endpoints are
// PUBLIC_UNDOCUMENTED during the preview and not in the generated SDK yet.
type dagDataset struct {
	Ref         string `json:"dataset_ref"`
	Name        string `json:"name"`
	FullName    string `json:"full_name"`
	DatasetType string `json:"dataset_type"`
}

// dagSink is a terminal write target (e.g. a Delta/Kafka sink). Unlike a dataset it has no
// dataset_type; for delta sinks table_name is the target table, otherwise name identifies it.
type dagSink struct {
	Ref       string `json:"sink_ref"`
	Name      string `json:"name"`
	TableName string `json:"table_name"`
}

// dagNode is a graph vertex: exactly one of Dataset or Sink is set (mirrors the server's oneof).
type dagNode struct {
	Dataset *dagDataset `json:"dataset"`
	Sink    *dagSink    `json:"sink"`
}

type listNodesResponse struct {
	Nodes         []dagNode `json:"nodes"`
	NextPageToken string    `json:"next_page_token"`
}

// dagDiagnosticNode is the node a diagnostic relates to; exactly one name field is set.
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
		// exception carries the structured JVM error (error_class, sql_state) behind an ERROR,
		// which is often more diagnostic than the generic top-level message.
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

// dagRunOpts are the dry-run mode flags shared by the pipeline-entities commands.
type dagRunOpts struct {
	forceDryRun bool
	noDryRun    bool
}

// updateDeps are the workspace operations resolveDryRun orchestrates. Injecting them (rather than
// calling w.Pipelines directly) keeps the resolution state machine unit-testable.
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

// newestUpdate returns the most recent update (by creation time) matching filter, if any.
// A nil filter matches all updates.
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

// resolveDryRun resolves the validate-only update whose graph a command should read, triggering a
// fresh dry-run when needed. It returns the update id and terminal state (COMPLETED = fetch the
// graph, FAILED/CANCELED = render diagnostics); continuous/active-update/no-dry-run return errors.
func resolveDryRun(ctx context.Context, d updateDeps, opts dagRunOpts) (id string, state pipelines.UpdateInfoState, err error) {
	// If we triggered a dry-run but never confirmed a terminal state (err != nil, from Ctrl-C, the
	// timeout, or an API error), it may still be running, so stop it to avoid leaking a billed cluster.
	signalCtx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	triggered := false
	defer func() {
		if triggered && err != nil {
			d.stop(ctx)
		}
	}()

	triggerAndPoll := func(notice string) (string, pipelines.UpdateInfoState, error) {
		// A continuous pipeline cannot be dry-run; check only on the trigger path so reading an
		// existing dry-run does not pay for an extra getPipeline call.
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

	// The active-update guard allows at most one non-terminal update, and it is always the most
	// recent; --no-dry-run never triggers, so an active update is not a blocker for it.
	if !opts.noDryRun {
		if u, ok := newestUpdate(updates, nil); ok && !isTerminal(u.State) {
			return "", "", activeUpdateError(u)
		}
	}

	latest, found := newestUpdate(updates, func(u pipelines.UpdateInfo) bool { return u.ValidateOnly })
	switch {
	case found && latest.State == pipelines.UpdateInfoStateCompleted:
		return latest.UpdateId, latest.State, nil
	case opts.noDryRun && found && !isTerminal(latest.State):
		// The newest dry-run is still running; --no-dry-run never triggers, so report that rather
		// than mislabeling an in-progress update as a failure.
		return "", "", fmt.Errorf("%w (update %s); re-run once it finishes", errDryRunInProgress, latest.UpdateId)
	case opts.noDryRun && found:
		// A failed/canceled dry-run: surface its diagnostics rather than triggering one.
		return latest.UpdateId, latest.State, nil
	case opts.noDryRun:
		return "", "", errNoDryRun
	default:
		// Default mode collapses a failed/canceled/absent dry-run into one fresh trigger.
		return triggerAndPoll(triggerNotice)
	}
}

// mapTriggerError translates a StartUpdate rejection caused by a concurrently-active update into the
// same targeted message the pre-trigger check produces; other errors pass through.
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

// newUpdateDeps wires updateDeps to the workspace client for the given pipeline.
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

// pollUpdate polls the update until it reaches a terminal state or ctx ends.
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

// stopUpdate best-effort cancels a triggered dry-run so an interrupted command does not leave a
// billed cluster running. It derives a fresh context since the command's is already cancelled.
func stopUpdate(ctx context.Context, w *databricks.WorkspaceClient, pipelineID string) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	cmdio.LogString(cleanupCtx, "Stopping the dry-run that was triggered...")
	if _, err := w.Pipelines.Stop(cleanupCtx, pipelines.StopRequest{PipelineId: pipelineID}); err != nil {
		log.Warnf(cleanupCtx, "failed to stop the dry-run for %s (a billed cluster may still be running): %v", pipelineID, err)
	}
}

// fetchAllPages issues GET path with page_size + page_token until the response has no next token,
// accumulating items.
func fetchAllPages[R, T any](ctx context.Context, c *client.DatabricksClient, headers map[string]string, path, updateID string, items func(*R) []T, next func(*R) string) ([]T, error) {
	var out []T
	token := ""
	for {
		query := map[string]string{
			"update_id": updateID,
			"page_size": strconv.Itoa(graphPageSize),
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

// fetchDatasets returns the pipeline's datasets (sink nodes excluded), re-polling once if a
// COMPLETED update reads back empty while aggregation finalizes.
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

// fetchNodes returns the pipeline's graph nodes (datasets and sinks) for the update.
func fetchNodes(ctx context.Context, c *client.DatabricksClient, headers map[string]string, pipelineID, updateID string) ([]dagNode, error) {
	return fetchAllPages(ctx, c, headers, graphPath(pipelineID, "nodes"), updateID,
		func(r *listNodesResponse) []dagNode { return r.Nodes },
		func(r *listNodesResponse) string { return r.NextPageToken })
}

// datasetsFromNodes keeps only the dataset nodes, dropping sinks.
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
	return fetchAllPages(ctx, c, headers, graphPath(pipelineID, "diagnostics"), updateID,
		func(r *listDiagnosticsResponse) []dagDiagnostic { return r.Diagnostics },
		func(r *listDiagnosticsResponse) string { return r.NextPageToken })
}

// failureError fetches a failed update's diagnostics and renders them as a single error. Used by
// both commands when the resolved update is FAILED/CANCELED.
func failureError(ctx context.Context, c *client.DatabricksClient, headers map[string]string, pipelineID, updateID string) error {
	diagnostics, err := fetchDiagnostics(ctx, c, headers, pipelineID, updateID)
	if err != nil {
		return fmt.Errorf("dry-run failed; could not fetch diagnostics: %w", err)
	}
	return fmt.Errorf("dry-run failed: %s", diagnosticsSummary(diagnostics))
}

// diagnosticsSummary joins the error-severity diagnostics into a single line.
func diagnosticsSummary(diagnostics []dagDiagnostic) string {
	var parts []string
	for _, d := range diagnostics {
		if strings.TrimPrefix(d.Severity, "DIAGNOSTIC_SEVERITY_") != "ERROR" {
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
	// Prefer the structured error identifiers (error_class [sql_state]) over the bare code: they
	// pinpoint the failure where the top-level message is often generic.
	if id := exceptionID(d); id != "" {
		msg = fmt.Sprintf("%s [%s]", msg, id)
	} else if d.Code != "" {
		msg = fmt.Sprintf("%s (%s)", msg, d.Code)
	}
	if d.DocumentURI != "" {
		// The API reports zero-based lines; add 1 so it matches the user's 1-based editor.
		msg = fmt.Sprintf("%s at %s:%d", msg, d.DocumentURI, d.Range.Start.Line+1)
	}
	return msg
}

// exceptionID joins the structured exception's error_class and sql_state (either may be absent) into
// a single identifier, empty when the diagnostic carries no exception detail.
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

// renderJSON writes v as indented JSON to the command's stdout with a trailing newline.
func renderJSON(cmd *cobra.Command, v any) error {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = cmd.OutOrStdout().Write(append(out, '\n'))
	return err
}

// orEmpty returns a non-nil slice so JSON output renders [] rather than null.
func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
