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
	"github.com/databricks/cli/libs/flags"
	"github.com/databricks/cli/libs/log"
	"github.com/databricks/cli/libs/logdiag"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/client"
	"github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/spf13/cobra"
)

type lineageOutput struct {
	Upstream   []dagDataset `json:"upstream"`
	Downstream []dagDataset `json:"downstream"`
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
		Args: cobra.RangeArgs(1, 2),
		// Hidden until the backing endpoint is generally available; it is served as
		// PUBLIC_UNDOCUMENTED during the preview.
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

		// Bound the whole command so a never-ready pipeline cannot hang the CLI. ProcessBundle reads
		// the context off the command, so push the bounded one onto cmd before the state read too.
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		cmd.SetContext(ctx)

		b, err := utils.ProcessBundle(cmd, utils.ProcessOptions{ErrorOnEmptyState: true, SkipInitContext: true})
		if err != nil {
			return err
		}

		// The trailing positional is always TABLE; a leading positional, when present, is the
		// pipeline KEY. RangeArgs(1, 2) guarantees one or two.
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
// downstream datasets. It is the testable seam RunE calls once it has a workspace client and
// resolved pipeline id.
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
		return failureError(ctx, apiClient, headers, pipelineID, updateID)
	}

	nodes, err := fetchNodes(ctx, apiClient, headers, pipelineID, updateID)
	if err != nil {
		return fmt.Errorf("fetch datasets for %s: %w", key, err)
	}
	flows, err := fetchFlows(ctx, apiClient, headers, pipelineID, updateID)
	if err != nil {
		return fmt.Errorf("fetch flows for %s: %w", key, err)
	}

	upstream, downstream, skipped, err := computeLineage(table, nodes, flows)
	if err != nil {
		return err
	}
	if skipped > 0 {
		log.Warnf(ctx, "skipped %d lineage edge(s) referencing unknown nodes", skipped)
	}
	return renderLineage(cmd, table, upstream, downstream)
}

// lineageNode flattens a graph node into the dagDataset shape lineage renders. Datasets pass through
// unchanged; sinks become a synthetic "SINK" dataset (named by table_name, else name) so a table's
// sink outputs show up in lineage instead of being dropped as unknown refs.
func lineageNode(n dagNode) (dagDataset, bool) {
	switch {
	case n.Dataset != nil:
		return *n.Dataset, true
	case n.Sink != nil:
		fullName := n.Sink.TableName
		if fullName == "" {
			fullName = n.Sink.Name
		}
		return dagDataset{Ref: n.Sink.Ref, FullName: fullName, DatasetType: sinkNodeType}, true
	default:
		return dagDataset{}, false
	}
}

// computeLineage resolves the upstream and downstream nodes of table from the node and flow lists.
// skipped counts edges referencing a node absent from the list (a leaked/stale ref); those are
// dropped rather than rendered as a blank name.
func computeLineage(table string, nodes []dagNode, flows []dagFlow) (upstream, downstream []dagDataset, skipped int, err error) {
	refToDataset := make(map[string]dagDataset, len(nodes))
	nameToRef := make(map[string]string, len(nodes))
	for _, n := range nodes {
		ds, ok := lineageNode(n)
		if !ok {
			continue
		}
		refToDataset[ds.Ref] = ds
		nameToRef[ds.FullName] = ds.Ref
	}

	targetRef, ok := nameToRef[table]
	if !ok {
		return nil, nil, 0, fmt.Errorf("table %s not found in pipeline graph", table)
	}

	upRefs := map[string]struct{}{}
	downRefs := map[string]struct{}{}
	for _, f := range flows {
		if f.OutputNodeRef == targetRef {
			for _, in := range f.InputNodeRefs {
				upRefs[in] = struct{}{}
			}
		}
		for _, in := range f.InputNodeRefs {
			if in == targetRef {
				downRefs[f.OutputNodeRef] = struct{}{}
			}
		}
	}

	upstream, upSkipped := resolveRefs(upRefs, refToDataset)
	downstream, downSkipped := resolveRefs(downRefs, refToDataset)
	return upstream, downstream, upSkipped + downSkipped, nil
}

// resolveRefs maps node refs to datasets, sorted by full name for deterministic output, and counts
// refs with no matching node.
func resolveRefs(refs map[string]struct{}, refToDataset map[string]dagDataset) ([]dagDataset, int) {
	out := make([]dagDataset, 0, len(refs))
	skipped := 0
	for ref := range refs {
		if d, ok := refToDataset[ref]; ok {
			out = append(out, d)
		} else {
			skipped++
		}
	}
	slices.SortFunc(out, func(a, b dagDataset) int { return strings.Compare(a.FullName, b.FullName) })
	return out, skipped
}

func renderLineage(cmd *cobra.Command, table string, upstream, downstream []dagDataset) error {
	switch root.OutputType(cmd) {
	case flags.OutputText:
		var sb strings.Builder
		fmt.Fprintf(&sb, "Lineage for %s\n\nUpstream:\n", table)
		writeDatasetLines(&sb, upstream)
		sb.WriteString("\nDownstream:\n")
		writeDatasetLines(&sb, downstream)
		_, err := cmd.OutOrStdout().Write([]byte(sb.String()))
		return err
	case flags.OutputJSON:
		return renderJSON(cmd, lineageOutput{Upstream: orEmpty(upstream), Downstream: orEmpty(downstream)})
	default:
		return fmt.Errorf("unknown output type %s", root.OutputType(cmd))
	}
}

func writeDatasetLines(sb *strings.Builder, datasets []dagDataset) {
	if len(datasets) == 0 {
		sb.WriteString("  (none)\n")
		return
	}
	for _, d := range datasets {
		fmt.Fprintf(sb, "  %s\t%s\n", d.FullName, d.DatasetType)
	}
}
