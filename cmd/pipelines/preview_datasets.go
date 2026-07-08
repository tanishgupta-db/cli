package pipelines

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/databricks/cli/cmd/bundle/utils"
	"github.com/databricks/cli/cmd/root"
	"github.com/databricks/cli/libs/auth"
	"github.com/databricks/cli/libs/flags"
	"github.com/databricks/cli/libs/logdiag"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/client"
	"github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/spf13/cobra"
)

func previewDatasetsCommand() *cobra.Command {
	var forceDryRun bool
	var noDryRun bool
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "preview-datasets [KEY]",
		Short: "List the datasets a pipeline defines",
		Long: `List the datasets a pipeline defines.

By default this reads the latest dry-run's already-computed graph; if no dry-run exists yet it
triggers one and waits for it. Use --force-dry-run to always
trigger a fresh dry-run, or --no-dry-run to never trigger one (reports the graph as unavailable
when none exists).

KEY is the pipeline's key in the bundle; it is optional if the bundle defines a
single pipeline.`,
		Args:   root.MaximumNArgs(1),
		Hidden: true,
	}
	cmd.Flags().BoolVar(&forceDryRun, "force-dry-run", false, "Always trigger a fresh dry-run (spins up a billed cluster) instead of reading the latest.")
	cmd.Flags().BoolVar(&noDryRun, "no-dry-run", false, "Never trigger a dry-run; report the graph as unavailable if none exists.")
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
		key, _, err := resolveRunArgument(ctx, b, args)
		if err != nil {
			return err
		}
		pipelineID, err := resolvePipelineIdFromKey(ctx, b, key)
		if err != nil {
			return err
		}

		w := b.WorkspaceClient(ctx)
		return runPreviewDatasets(ctx, cmd, w, pipelineID, key, dagRunOpts{forceDryRun: forceDryRun, noDryRun: noDryRun})
	}
	return cmd
}

// resolves the dry-run graph for the pipeline and renders its datasets
func runPreviewDatasets(ctx context.Context, cmd *cobra.Command, w *databricks.WorkspaceClient, pipelineID, key string, opts dagRunOpts) error {
	apiClient, err := client.New(w.Config)
	if err != nil {
		return fmt.Errorf("create API client: %w", err)
	}
	headers := auth.WorkspaceIDHeaders(w.Config)

	updateID, state, err := resolveDryRun(ctx, newUpdateDeps(w, pipelineID), opts)
	if err != nil {
		return fmt.Errorf("datasets for %s: %w", key, err)
	}
	if state != pipelines.UpdateInfoStateCompleted {
		return nonCompletedError(ctx, apiClient, headers, pipelineID, updateID, state)
	}

	datasets, err := fetchDatasets(ctx, apiClient, headers, pipelineID, updateID)
	if err != nil {
		return fmt.Errorf("fetch datasets for %s: %w", key, err)
	}
	return renderPreviewDatasets(cmd, datasets)
}

func renderPreviewDatasets(cmd *cobra.Command, datasets []dagDataset) error {
	switch root.OutputType(cmd) {
	case flags.OutputText:
		if len(datasets) == 0 {
			_, err := cmd.OutOrStdout().Write([]byte("(none)\n"))
			return err
		}
		var sb strings.Builder
		for _, d := range datasets {
			fmt.Fprintf(&sb, "%s\t%s\n", d.FullName, d.DatasetType)
		}
		_, err := cmd.OutOrStdout().Write([]byte(sb.String()))
		return err
	case flags.OutputJSON:
		return renderJSON(cmd, orEmpty(datasets))
	default:
		return fmt.Errorf("unknown output type %s", root.OutputType(cmd))
	}
}
