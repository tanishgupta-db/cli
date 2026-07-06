package pipelines

import (
	"bytes"
	"testing"

	"github.com/databricks/cli/libs/flags"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// renderCmd builds a command wired with the output flag set to out and stdout captured to buf.
func renderCmd(t *testing.T, out flags.Output) (*cobra.Command, *bytes.Buffer) {
	t.Helper()
	cmd := &cobra.Command{}
	value := out
	cmd.Flags().Var(&value, "output", "")
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	return cmd, buf
}

func TestRenderDatasets(t *testing.T) {
	datasets := []dagDataset{
		{FullName: "main.s.a", DatasetType: "MATERIALIZED_VIEW"},
		{FullName: "main.s.b", DatasetType: "STREAMING_TABLE"},
	}

	t.Run("text", func(t *testing.T) {
		cmd, buf := renderCmd(t, flags.OutputText)
		require.NoError(t, renderDatasets(cmd, datasets))
		assert.Equal(t, "main.s.a\tMATERIALIZED_VIEW\nmain.s.b\tSTREAMING_TABLE\n", buf.String())
	})
	t.Run("text empty", func(t *testing.T) {
		cmd, buf := renderCmd(t, flags.OutputText)
		require.NoError(t, renderDatasets(cmd, nil))
		assert.Empty(t, buf.String())
	})
	t.Run("json empty renders an array", func(t *testing.T) {
		cmd, buf := renderCmd(t, flags.OutputJSON)
		require.NoError(t, renderDatasets(cmd, nil))
		assert.Equal(t, "[]\n", buf.String())
	})
}
