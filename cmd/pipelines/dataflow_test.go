package pipelines

import (
	"context"
	"errors"
	"testing"

	"github.com/databricks/cli/libs/cmdio"
	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDeps is an in-memory updateDeps for testing resolveDryRun without a workspace.
type fakeDeps struct {
	continuous      bool
	continuousErr   error
	updates         []pipelines.UpdateInfo
	updatesErr      error
	triggerID       string
	triggerErr      error
	pollState       pipelines.UpdateInfoState
	pollErr         error
	continuousCalls int
	listCalls       int
	triggered       bool
	stopped         bool
}

func (f *fakeDeps) deps() updateDeps {
	return updateDeps{
		continuous: func(ctx context.Context) (bool, error) {
			f.continuousCalls++
			return f.continuous, f.continuousErr
		},
		listUpdates: func(ctx context.Context) ([]pipelines.UpdateInfo, error) {
			f.listCalls++
			return f.updates, f.updatesErr
		},
		trigger: func(ctx context.Context) (string, error) {
			f.triggered = true
			return f.triggerID, f.triggerErr
		},
		poll: func(ctx context.Context, updateID string) (pipelines.UpdateInfoState, error) {
			return f.pollState, f.pollErr
		},
		stop: func(ctx context.Context) { f.stopped = true },
	}
}

func upd(id string, state pipelines.UpdateInfoState, validateOnly bool, created int64) pipelines.UpdateInfo {
	return pipelines.UpdateInfo{UpdateId: id, State: state, ValidateOnly: validateOnly, CreationTime: created}
}

func TestResolveDryRun(t *testing.T) {
	completed := pipelines.UpdateInfoStateCompleted
	cases := []struct {
		name          string
		opts          dagRunOpts
		fake          fakeDeps
		wantID        string
		wantState     pipelines.UpdateInfoState
		wantErrIs     error
		wantErrSub    string
		wantTriggered bool
	}{
		{
			name:      "default reads the latest completed dry-run",
			fake:      fakeDeps{updates: []pipelines.UpdateInfo{upd("u2", completed, true, 2), upd("u1", completed, false, 1)}},
			wantID:    "u2",
			wantState: completed,
		},
		{
			name:          "default triggers when no dry-run exists",
			fake:          fakeDeps{updates: []pipelines.UpdateInfo{upd("u1", completed, false, 1)}, triggerID: "u9", pollState: completed},
			wantID:        "u9",
			wantState:     completed,
			wantTriggered: true,
		},
		{
			name:          "default triggers when the update list is empty",
			fake:          fakeDeps{updates: nil, triggerID: "u9", pollState: completed},
			wantID:        "u9",
			wantState:     completed,
			wantTriggered: true,
		},
		{
			name:          "default collapses a failed dry-run into a fresh trigger",
			fake:          fakeDeps{updates: []pipelines.UpdateInfo{upd("u2", pipelines.UpdateInfoStateFailed, true, 2)}, triggerID: "u9", pollState: completed},
			wantID:        "u9",
			wantState:     completed,
			wantTriggered: true,
		},
		{
			name:       "default rejects an in-flight dry-run",
			fake:       fakeDeps{updates: []pipelines.UpdateInfo{upd("u3", pipelines.UpdateInfoStateRunning, true, 3), upd("u1", completed, true, 1)}},
			wantErrIs:  errActiveUpdate,
			wantErrSub: "dry-run is in progress",
		},
		{
			name:       "default rejects an in-flight real run",
			fake:       fakeDeps{updates: []pipelines.UpdateInfo{upd("u3", pipelines.UpdateInfoStateRunning, false, 3)}},
			wantErrIs:  errActiveUpdate,
			wantErrSub: "an update is running",
		},
		{
			name:      "default reads a completed dry-run even when a newer real run is active",
			fake:      fakeDeps{updates: []pipelines.UpdateInfo{upd("u3", pipelines.UpdateInfoStateRunning, false, 3), upd("u2", completed, true, 2)}},
			wantID:    "u2",
			wantState: completed,
		},
		{
			name:      "default rejects a continuous pipeline",
			fake:      fakeDeps{continuous: true},
			wantErrIs: errContinuous,
		},
		{
			name:      "no-dry-run reads the latest completed dry-run",
			opts:      dagRunOpts{noDryRun: true},
			fake:      fakeDeps{updates: []pipelines.UpdateInfo{upd("u2", completed, true, 2)}},
			wantID:    "u2",
			wantState: completed,
		},
		{
			name:      "no-dry-run returns a failed dry-run for diagnostics",
			opts:      dagRunOpts{noDryRun: true},
			fake:      fakeDeps{updates: []pipelines.UpdateInfo{upd("u2", pipelines.UpdateInfoStateFailed, true, 2)}},
			wantID:    "u2",
			wantState: pipelines.UpdateInfoStateFailed,
		},
		{
			name:       "no-dry-run reports an in-progress dry-run instead of mislabeling it failed",
			opts:       dagRunOpts{noDryRun: true},
			fake:       fakeDeps{updates: []pipelines.UpdateInfo{upd("u3", pipelines.UpdateInfoStateRunning, true, 3)}},
			wantErrIs:  errDryRunInProgress,
			wantErrSub: "still in progress",
		},
		{
			name:      "no-dry-run errors when no dry-run exists",
			opts:      dagRunOpts{noDryRun: true},
			fake:      fakeDeps{updates: []pipelines.UpdateInfo{upd("u1", completed, false, 1)}},
			wantErrIs: errNoDryRun,
		},
		{
			name:      "no-dry-run is exempt from the active-update check",
			opts:      dagRunOpts{noDryRun: true},
			fake:      fakeDeps{updates: []pipelines.UpdateInfo{upd("u3", pipelines.UpdateInfoStateRunning, false, 3), upd("u2", completed, true, 2)}},
			wantID:    "u2",
			wantState: completed,
		},
		{
			name:          "force triggers a fresh dry-run",
			opts:          dagRunOpts{forceDryRun: true},
			fake:          fakeDeps{triggerID: "u9", pollState: completed},
			wantID:        "u9",
			wantState:     completed,
			wantTriggered: true,
		},
		{
			name:      "force rejects a continuous pipeline",
			opts:      dagRunOpts{forceDryRun: true},
			fake:      fakeDeps{continuous: true},
			wantErrIs: errContinuous,
		},
		{
			name:          "force returns a failed state for diagnostics",
			opts:          dagRunOpts{forceDryRun: true},
			fake:          fakeDeps{triggerID: "u9", pollState: pipelines.UpdateInfoStateFailed},
			wantID:        "u9",
			wantState:     pipelines.UpdateInfoStateFailed,
			wantTriggered: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, _ := cmdio.NewTestContextWithStdout(t.Context())
			f := c.fake
			id, state, err := resolveDryRun(ctx, f.deps(), c.opts)

			if c.wantErrIs != nil || c.wantErrSub != "" {
				require.Error(t, err)
				if c.wantErrIs != nil {
					assert.ErrorIs(t, err, c.wantErrIs)
				}
				if c.wantErrSub != "" {
					assert.Contains(t, err.Error(), c.wantErrSub)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, c.wantID, id)
				assert.Equal(t, c.wantState, state)
			}
			assert.Equal(t, c.wantTriggered, f.triggered)
		})
	}
}

func TestResolveDryRunForceSkipsListButChecksContinuous(t *testing.T) {
	ctx, _ := cmdio.NewTestContextWithStdout(t.Context())
	f := fakeDeps{triggerID: "u9", pollState: pipelines.UpdateInfoStateCompleted}
	_, _, err := resolveDryRun(ctx, f.deps(), dagRunOpts{forceDryRun: true})
	require.NoError(t, err)
	assert.Equal(t, 0, f.listCalls, "force must not list updates")
	assert.Equal(t, 1, f.continuousCalls, "force must still reject continuous pipelines")
}

func TestResolveDryRunNoDryRunSkipsContinuousCheck(t *testing.T) {
	ctx, _ := cmdio.NewTestContextWithStdout(t.Context())
	// continuous is true but must be ignored: --no-dry-run never triggers.
	f := fakeDeps{continuous: true, updates: []pipelines.UpdateInfo{upd("u2", pipelines.UpdateInfoStateCompleted, true, 2)}}
	id, _, err := resolveDryRun(ctx, f.deps(), dagRunOpts{noDryRun: true})
	require.NoError(t, err)
	assert.Equal(t, "u2", id)
	assert.Equal(t, 0, f.continuousCalls)
}

func TestResolveDryRunStopsTriggeredDryRunOnInterrupt(t *testing.T) {
	base, _ := cmdio.NewTestContextWithStdout(t.Context())
	ctx, cancel := context.WithCancel(base)
	cancel() // simulate Ctrl-C / timeout

	f := fakeDeps{triggerID: "u9", pollErr: context.Canceled}
	_, _, err := resolveDryRun(ctx, f.deps(), dagRunOpts{forceDryRun: true})
	require.Error(t, err)
	assert.True(t, f.triggered)
	assert.True(t, f.stopped, "an interrupted, triggered dry-run must be stopped")
}

func TestResolveDryRunStopsTriggeredDryRunOnPollError(t *testing.T) {
	ctx, _ := cmdio.NewTestContextWithStdout(t.Context())
	// Polling fails with a non-cancellation error (e.g. a 500 or auth expiry) after we triggered:
	// we never confirmed a terminal state, so the billed cluster must still be stopped.
	f := fakeDeps{triggerID: "u9", pollErr: errors.New("boom")}
	_, _, err := resolveDryRun(ctx, f.deps(), dagRunOpts{forceDryRun: true})
	require.Error(t, err)
	assert.True(t, f.triggered)
	assert.True(t, f.stopped, "a triggered dry-run must be stopped when polling errors out")
}

func TestResolveDryRunDoesNotStopOnTerminalFailure(t *testing.T) {
	ctx, _ := cmdio.NewTestContextWithStdout(t.Context())
	// A FAILED dry-run is terminal: its cluster already stopped, so we must NOT issue a stop.
	f := fakeDeps{triggerID: "u9", pollState: pipelines.UpdateInfoStateFailed}
	_, state, err := resolveDryRun(ctx, f.deps(), dagRunOpts{forceDryRun: true})
	require.NoError(t, err)
	assert.Equal(t, pipelines.UpdateInfoStateFailed, state)
	assert.True(t, f.triggered)
	assert.False(t, f.stopped, "a terminal (FAILED) dry-run must not be stopped")
}

func TestResolveDryRunMapsActiveUpdateTriggerError(t *testing.T) {
	ctx, _ := cmdio.NewTestContextWithStdout(t.Context())
	// StartUpdate is rejected because a real run started in the race window.
	f := fakeDeps{
		triggerErr: &apierr.APIError{ErrorCode: errCodeInvalidStateTransition, Message: "active update"},
		updates:    []pipelines.UpdateInfo{upd("u3", pipelines.UpdateInfoStateRunning, false, 3)},
	}
	_, _, err := resolveDryRun(ctx, f.deps(), dagRunOpts{forceDryRun: true})
	require.Error(t, err)
	assert.ErrorIs(t, err, errActiveUpdate)
	assert.Contains(t, err.Error(), "an update is running")
}

func TestResolveDryRunPassesThroughNonActiveTriggerError(t *testing.T) {
	ctx, _ := cmdio.NewTestContextWithStdout(t.Context())
	boom := errors.New("boom")
	f := fakeDeps{triggerErr: boom}
	_, _, err := resolveDryRun(ctx, f.deps(), dagRunOpts{forceDryRun: true})
	assert.ErrorIs(t, err, boom)
}

func TestIsTerminal(t *testing.T) {
	for _, s := range []pipelines.UpdateInfoState{pipelines.UpdateInfoStateCompleted, pipelines.UpdateInfoStateFailed, pipelines.UpdateInfoStateCanceled} {
		assert.True(t, isTerminal(s), string(s))
	}
	for _, s := range []pipelines.UpdateInfoState{pipelines.UpdateInfoStateRunning, pipelines.UpdateInfoStateWaitingForResources, pipelines.UpdateInfoStateQueued, ""} {
		assert.False(t, isTerminal(s), string(s))
	}
}

func TestNewestUpdate(t *testing.T) {
	updates := []pipelines.UpdateInfo{
		upd("u1", pipelines.UpdateInfoStateCompleted, false, 1),
		upd("u3", pipelines.UpdateInfoStateCompleted, true, 3),
		upd("u2", pipelines.UpdateInfoStateCompleted, true, 2),
	}
	newest, ok := newestUpdate(updates, nil)
	require.True(t, ok)
	assert.Equal(t, "u3", newest.UpdateId)

	dryRun, ok := newestUpdate(updates, func(u pipelines.UpdateInfo) bool { return u.ValidateOnly })
	require.True(t, ok)
	assert.Equal(t, "u3", dryRun.UpdateId)

	_, ok = newestUpdate(nil, nil)
	assert.False(t, ok)
}

func TestActiveUpdateError(t *testing.T) {
	assert.Contains(t, activeUpdateError(upd("u1", pipelines.UpdateInfoStateRunning, true, 1)).Error(), "dry-run is in progress")
	assert.Contains(t, activeUpdateError(upd("u1", pipelines.UpdateInfoStateRunning, false, 1)).Error(), "an update is running")
}

func TestDiagnosticsSummary(t *testing.T) {
	assert.Equal(t, "no error diagnostics returned", diagnosticsSummary(nil))

	// Only ERROR-severity diagnostics are summarized; WARNING and INFORMATION are dropped.
	diags := []dagDiagnostic{
		{Severity: "WARNING", Message: "ignored warning"},
		{Severity: "INFORMATION", Message: "ignored insight"},
		{Severity: severityError, Code: "TABLE_NOT_FOUND", Message: "missing table", RelatedNodes: []dagDiagnosticNode{{DatasetName: "main.s.t"}}},
	}
	diags[2].Range.Start.Line = 12
	diags[2].DocumentURI = "file:///a.py"
	// The zero-based line 12 renders as 1-based 13.
	assert.Equal(t, "main.s.t: missing table (TABLE_NOT_FOUND) at file:///a.py:13", diagnosticsSummary(diags))
}

func TestFormatDiagnosticExceptionDetail(t *testing.T) {
	// error_class + sql_state take precedence over the bare code and pinpoint the failure.
	var d dagDiagnostic
	d.Message = "boom"
	d.Code = "TABLE_NOT_FOUND"
	d.Details.Exception.ErrorClass = "UNRESOLVED_COLUMN"
	d.Details.Exception.SQLState = "42703"
	assert.Equal(t, "boom [UNRESOLVED_COLUMN 42703]", formatDiagnostic(d))

	// Either field alone still surfaces; empty when neither is set.
	var classOnly dagDiagnostic
	classOnly.Details.Exception.ErrorClass = "UNRESOLVED_COLUMN"
	assert.Equal(t, "UNRESOLVED_COLUMN", exceptionID(classOnly))
	var stateOnly dagDiagnostic
	stateOnly.Details.Exception.SQLState = "42703"
	assert.Equal(t, "42703", exceptionID(stateOnly))
	assert.Empty(t, exceptionID(dagDiagnostic{}))
}

func TestNonCompletedErrorCanceled(t *testing.T) {
	// A canceled dry-run has no diagnostics, so it must report as canceled (not "failed"). It
	// short-circuits before any diagnostics fetch, so a nil client is fine here.
	err := nonCompletedError(t.Context(), nil, nil, "pid", "uid", pipelines.UpdateInfoStateCanceled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "canceled")
	assert.NotContains(t, err.Error(), "failed")
}

func TestDiagnosticTarget(t *testing.T) {
	assert.Equal(t, "ds", diagnosticTarget([]dagDiagnosticNode{{DatasetName: "ds"}}))
	assert.Equal(t, "sink", diagnosticTarget([]dagDiagnosticNode{{SinkName: "sink"}}))
	assert.Equal(t, "flow", diagnosticTarget([]dagDiagnosticNode{{FlowName: "flow"}}))
	assert.Empty(t, diagnosticTarget(nil))
}
