package pipelines

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/databricks/cli/cmd/bundle/utils"
	"github.com/databricks/cli/cmd/root"
	"github.com/databricks/cli/libs/auth"
	"github.com/databricks/cli/libs/cmdio"
	"github.com/databricks/cli/libs/flags"
	"github.com/databricks/cli/libs/logdiag"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/client"
	"github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/spf13/cobra"
)

type lineageOutput struct {
	Upstream   []dagDataset `json:"upstream"`
	Downstream []dagDataset `json:"downstream"`
	// refs not present as nodes (e.g. external sources); omitempty keeps the happy-path JSON clean.
	UnresolvedRefs int `json:"unresolved_refs,omitempty"`
}

func lineageCommand() *cobra.Command {
	var forceDryRun bool
	var noDryRun bool
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "lineage [KEY] TABLE",
		Short: "Show the upstream and downstream datasets for a table",
		Long: `Show the datasets that feed TABLE (upstream) and that TABLE feeds (downstream)
within a pipeline's entities.

By default this reads the latest dry-run's already-computed graph; if no dry-run exists yet it
triggers one (which spins up a billed cluster) and waits for it. Use --force-dry-run to always
trigger a fresh dry-run, or --no-dry-run to never trigger one (reporting lineage as unavailable
when none exists).

KEY is the pipeline's key in the bundle; it is optional if the bundle defines a
single pipeline. TABLE is the dataset to inspect.`,
		Args:   cobra.RangeArgs(1, 2),
		Hidden: true,
	}
	cmd.Flags().BoolVar(&forceDryRun, "force-dry-run", false, "Always trigger a fresh dry-run (spins up a billed cluster) instead of reading the latest.")
	cmd.Flags().BoolVar(&noDryRun, "no-dry-run", false, "Never trigger a dry-run; report lineage as unavailable if none exists.")
	cmd.Flags().DurationVar(&timeout, "timeout", 20*time.Minute, "Maximum time to wait for the graph.")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := logdiag.InitContext(cmd.Context())
		cmd.SetContext(ctx)

		if forceDryRun && noDryRun {
			return errors.New("--force-dry-run and --no-dry-run cannot be used together")
		}

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		cmd.SetContext(ctx)

		b, err := utils.ProcessBundle(cmd, utils.ProcessOptions{ErrorOnEmptyState: true, SkipInitContext: true})
		if err != nil {
			return err
		}
		suggestPipelineDeploy(ctx, cmd)

		// trailing positional is always TABLE; a leading positional, when present, is the pipeline KEY
		var keyArgs []string
		var table string
		if len(args) == 2 {
			keyArgs = args[:1]
			table = args[1]
		} else {
			table = args[0]
		}

		key, _, err := resolveRunArgument(ctx, b, keyArgs)
		if err != nil {
			return err
		}
		pipelineID, err := resolvePipelineIdFromKey(ctx, b, key)
		if err != nil {
			return err
		}

		w := b.WorkspaceClient(ctx)
		return runLineage(ctx, cmd, w, pipelineID, key, table, dagRunOpts{forceDryRun: forceDryRun, noDryRun: noDryRun})
	}
	return cmd
}

// runLineage resolves the dry-run graph for the pipeline and renders TABLE's upstream and
// downstream datasets
func runLineage(ctx context.Context, cmd *cobra.Command, w *databricks.WorkspaceClient, pipelineID, key, table string, opts dagRunOpts) error {
	apiClient, err := client.New(w.Config)
	if err != nil {
		return fmt.Errorf("create API client: %w", err)
	}
	headers := auth.WorkspaceIDHeaders(w.Config)

	updateID, state, err := resolveDryRun(ctx, newUpdateDeps(w, pipelineID), opts)
	if err != nil {
		return fmt.Errorf("lineage for %s: %w", table, err)
	}
	if state != pipelines.UpdateInfoStateCompleted {
		return nonCompletedError(ctx, apiClient, headers, pipelineID, updateID, state)
	}

	nodes, err := fetchNodes(ctx, apiClient, headers, pipelineID, updateID)
	if err != nil {
		return fmt.Errorf("fetch nodes for %s: %w", key, err)
	}
	flows, err := fetchFlows(ctx, apiClient, headers, pipelineID, updateID)
	if err != nil {
		return fmt.Errorf("fetch flows for %s: %w", key, err)
	}

	upstream, downstream, unresolved, err := computeLineage(table, nodes, flows)
	if err != nil {
		return err
	}
	return renderLineage(ctx, cmd, table, upstream, downstream, unresolved)
}

// resolves the upstream and downstream nodes of table from the node and flow lists
func computeLineage(table string, nodes []dagNode, flows []dagFlow) (upstream, downstream []dagDataset, unresolved int, err error) {
	refToDataset := make(map[string]dagDataset, len(nodes))
	nameToRef := make(map[string]string, len(nodes))
	for _, n := range nodes {
		ds, ok := nodeToDataset(n)
		if !ok {
			continue
		}
		refToDataset[ds.Ref] = ds
		// key on display name so the clean identifier matches; skip empty names (e.g. an empty arg)
		if name := displayName(ds); name != "" {
			nameToRef[name] = ds.Ref
		}
	}

	targetRef, ok := nameToRef[table]
	if !ok {
		return nil, nil, 0, fmt.Errorf("table %s not found in pipeline graph", table)
	}

	upRefs := map[string]struct{}{}
	downRefs := map[string]struct{}{}
	addRef := func(refs map[string]struct{}, ref string) {
		if ref != "" {
			refs[ref] = struct{}{}
		}
	}
	for _, f := range flows {
		if f.OutputNodeRef == targetRef {
			for _, in := range f.InputNodeRefs {
				addRef(upRefs, in)
			}
		}
		for _, in := range f.InputNodeRefs {
			if in == targetRef {
				addRef(downRefs, f.OutputNodeRef)
			}
		}
	}

	upstream = resolveRefs(upRefs, refToDataset)
	downstream = resolveRefs(downRefs, refToDataset)
	// avoid double-counting unresolved refs
	unresolvedRefs := map[string]struct{}{}
	for _, refs := range []map[string]struct{}{upRefs, downRefs} {
		for ref := range refs {
			if _, ok := refToDataset[ref]; !ok {
				unresolvedRefs[ref] = struct{}{}
			}
		}
	}
	return upstream, downstream, len(unresolvedRefs), nil
}

// maps the resolvable node refs to datasets, sorted by display name to match render order
func resolveRefs(refs map[string]struct{}, refToDataset map[string]dagDataset) []dagDataset {
	out := make([]dagDataset, 0, len(refs))
	for ref := range refs {
		if d, ok := refToDataset[ref]; ok {
			out = append(out, d)
		}
	}
	slices.SortFunc(out, func(a, b dagDataset) int { return strings.Compare(displayName(a), displayName(b)) })
	return out
}

// lineageView is the display projection rendered by lineageTemplate.
type lineageView struct {
	Table      string
	Upstream   []datasetRow
	Downstream []datasetRow
	Unresolved int
}

func renderLineage(ctx context.Context, cmd *cobra.Command, table string, upstream, downstream []dagDataset, unresolved int) error {
	switch root.OutputType(cmd) {
	case flags.OutputText:
		view := lineageView{
			Table:      table,
			Upstream:   datasetRows(upstream),
			Downstream: datasetRows(downstream),
			Unresolved: unresolved,
		}
		return cmdio.RenderWithTemplate(ctx, view, "", lineageTemplate)
	case flags.OutputJSON:
		return renderJSON(cmd, lineageOutput{Upstream: orEmpty(upstream), Downstream: orEmpty(downstream), UnresolvedRefs: unresolved})
	default:
		return fmt.Errorf("unknown output type %s", root.OutputType(cmd))
	}
}
