package pipelines

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/databricks/cli/libs/cmdio"
	"github.com/databricks/cli/libs/flags"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// builds a command plus a cmdio context that captures stdout (results) to the returned
// buffer, mirroring how root wires cmdio to the command in production.
func renderCmd(t *testing.T, out flags.Output) (context.Context, *cobra.Command, *bytes.Buffer) {
	t.Helper()
	cmd := &cobra.Command{}
	value := out
	cmd.Flags().Var(&value, "output", "")
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	ctx := cmdio.InContext(t.Context(), cmdio.NewIO(t.Context(), out, nil, buf, io.Discard, "", ""))
	return ctx, cmd, buf
}

func TestRenderDatasets(t *testing.T) {
	datasets := []dagDataset{
		// dataset renders by name (not backticked full_name); view by name; sink by full_name
		{Name: "main.s.a", FullName: "`main`.`s`.`a`", DatasetType: "MATERIALIZED_VIEW"},
		{Name: "recent", DatasetType: "VIEW"},
		{FullName: "main.s.archive", DatasetType: "SINK"},
	}

	t.Run("text", func(t *testing.T) {
		ctx, cmd, buf := renderCmd(t, flags.OutputText)
		require.NoError(t, renderDatasets(ctx, cmd, datasets))
		assert.Equal(t, "Name            Type\nmain.s.a        MATERIALIZED_VIEW\nrecent          VIEW\nmain.s.archive  SINK\n", buf.String())
	})
	t.Run("text empty", func(t *testing.T) {
		ctx, cmd, buf := renderCmd(t, flags.OutputText)
		require.NoError(t, renderDatasets(ctx, cmd, nil))
		assert.Equal(t, "(none)\n", buf.String())
	})
	t.Run("json empty renders an array", func(t *testing.T) {
		ctx, cmd, buf := renderCmd(t, flags.OutputJSON)
		require.NoError(t, renderDatasets(ctx, cmd, nil))
		assert.Equal(t, "[]\n", buf.String())
	})
}

func TestDatasetsFromNodes(t *testing.T) {
	nodes := []dagNode{
		{Dataset: &dagDataset{Ref: "n1", Name: "main.s.a", FullName: "`main`.`s`.`a`", DatasetType: "STREAMING_TABLE"}},
		{Sink: &dagSink{Ref: "s1", TableName: "main.s.archive"}},
		// a Kafka sink has no table_name, so its identifier arrives in name
		{Sink: &dagSink{Ref: "s2", Name: "events_kafka_sink"}},
	}
	// datasets and sinks are both listed; a sink becomes a SINK-typed row
	got := datasetsFromNodes(nodes)
	require.Len(t, got, 3)
	assert.Equal(t, "main.s.a", got[0].Name)
	assert.Equal(t, dagDataset{Ref: "s1", FullName: "main.s.archive", DatasetType: sinkNodeType}, got[1])
	assert.Equal(t, dagDataset{Ref: "s2", FullName: "events_kafka_sink", DatasetType: sinkNodeType}, got[2])
}

func TestDisplayName(t *testing.T) {
	assert.Equal(t, "main.s.a", displayName(dagDataset{Name: "main.s.a", FullName: "`main`.`s`.`a`"}))
	assert.Equal(t, "recent", displayName(dagDataset{Name: "recent"}))
	// a sink has no name, so it falls back to full_name
	assert.Equal(t, "main.s.archive", displayName(dagDataset{FullName: "main.s.archive"}))
}
