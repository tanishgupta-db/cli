package pipelines

import (
	"testing"

	"github.com/databricks/cli/libs/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func names(ds []dagDataset) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.FullName
	}
	return out
}

func datasetNode(ref, fullName, datasetType string) dagNode {
	return dagNode{Dataset: &dagDataset{Ref: ref, FullName: fullName, DatasetType: datasetType}}
}

func TestComputeLineage(t *testing.T) {
	// raw -> orders -> report ; orders -> audit
	nodes := []dagNode{
		datasetNode("r1", "main.s.raw", "STREAMING_TABLE"),
		datasetNode("r2", "main.s.orders", "MATERIALIZED_VIEW"),
		datasetNode("r3", "main.s.report", "MATERIALIZED_VIEW"),
		datasetNode("r4", "main.s.audit", "MATERIALIZED_VIEW"),
	}
	flows := []dagFlow{
		{InputNodeRefs: []string{"r1"}, OutputNodeRef: "r2"},
		{InputNodeRefs: []string{"r2"}, OutputNodeRef: "r3"},
		{InputNodeRefs: []string{"r2"}, OutputNodeRef: "r4"},
	}

	up, down, skipped, err := computeLineage("main.s.orders", nodes, flows)
	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	assert.Equal(t, []string{"main.s.raw"}, names(up))
	assert.Equal(t, []string{"main.s.audit", "main.s.report"}, names(down)) // sorted by full name
}

func TestComputeLineageMatchesCleanName(t *testing.T) {
	// target matches the clean name (not backticked full_name); both fields survive for JSON
	nodes := []dagNode{
		{Dataset: &dagDataset{Ref: "r1", Name: "main.s.raw", FullName: "`main`.`s`.`raw`", DatasetType: "STREAMING_TABLE"}},
		{Dataset: &dagDataset{Ref: "r2", Name: "main.s.orders", FullName: "`main`.`s`.`orders`", DatasetType: "MATERIALIZED_VIEW"}},
	}
	flows := []dagFlow{{InputNodeRefs: []string{"r1"}, OutputNodeRef: "r2"}}

	up, _, _, err := computeLineage("main.s.orders", nodes, flows)
	require.NoError(t, err)
	require.Len(t, up, 1)
	assert.Equal(t, "main.s.raw", up[0].Name)
	assert.Equal(t, "`main`.`s`.`raw`", up[0].FullName)
}

func TestComputeLineageRejectsEmptyTarget(t *testing.T) {
	// a view has an empty full_name; keying on name keeps an empty arg from matching it
	nodes := []dagNode{
		{Dataset: &dagDataset{Ref: "v1", Name: "recent", FullName: "", DatasetType: "VIEW"}},
	}
	_, _, _, err := computeLineage("", nodes, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in pipeline graph")
}

func TestComputeLineageMultiInput(t *testing.T) {
	// a, b -> joined (a flow with multiple input refs)
	nodes := []dagNode{
		datasetNode("ra", "main.s.a", "MATERIALIZED_VIEW"),
		datasetNode("rb", "main.s.b", "MATERIALIZED_VIEW"),
		datasetNode("rj", "main.s.joined", "MATERIALIZED_VIEW"),
	}
	flows := []dagFlow{{InputNodeRefs: []string{"ra", "rb"}, OutputNodeRef: "rj"}}

	up, down, skipped, err := computeLineage("main.s.joined", nodes, flows)
	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	assert.Equal(t, []string{"main.s.a", "main.s.b"}, names(up))
	assert.Empty(t, down)
}

func TestComputeLineageIncludesSinkDownstream(t *testing.T) {
	// orders -> a delta sink (table_name) and a non-delta sink (name). Both must appear downstream
	// as SINK-typed entries rather than being dropped as unknown refs.
	nodes := []dagNode{
		datasetNode("r2", "main.s.orders", "MATERIALIZED_VIEW"),
		{Sink: &dagSink{Ref: "s1", TableName: "main.s.orders_sink"}},
		{Sink: &dagSink{Ref: "s2", Name: "kafka_out"}},
	}
	flows := []dagFlow{
		{InputNodeRefs: []string{"r2"}, OutputNodeRef: "s1"},
		{InputNodeRefs: []string{"r2"}, OutputNodeRef: "s2"},
	}

	up, down, skipped, err := computeLineage("main.s.orders", nodes, flows)
	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	assert.Empty(t, up)
	assert.Equal(t, []string{"kafka_out", "main.s.orders_sink"}, names(down)) // sorted by display name
	for _, d := range down {
		assert.Equal(t, sinkNodeType, d.DatasetType)
	}
}

func TestComputeLineageTargetNotFound(t *testing.T) {
	_, _, _, err := computeLineage("main.s.missing", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in pipeline graph")
}

func TestComputeLineageCountsUnresolvedRefs(t *testing.T) {
	// A flow feeds the target from a ref with no matching node (an external source the pipeline reads).
	nodes := []dagNode{datasetNode("r2", "main.s.orders", "")}
	flows := []dagFlow{{InputNodeRefs: []string{"ghost"}, OutputNodeRef: "r2"}}
	up, _, unresolved, err := computeLineage("main.s.orders", nodes, flows)
	require.NoError(t, err)
	assert.Empty(t, up)
	assert.Equal(t, 1, unresolved)
}

func TestComputeLineageCountsEachUnresolvedRefOnce(t *testing.T) {
	// "ghost" is both an upstream input and a downstream output of the target; it must count once,
	// and an empty output ref must not inflate the count.
	nodes := []dagNode{datasetNode("r2", "main.s.orders", "")}
	flows := []dagFlow{
		{InputNodeRefs: []string{"ghost"}, OutputNodeRef: "r2"},
		{InputNodeRefs: []string{"r2"}, OutputNodeRef: "ghost"},
		{InputNodeRefs: []string{"r2"}, OutputNodeRef: ""},
	}
	_, _, unresolved, err := computeLineage("main.s.orders", nodes, flows)
	require.NoError(t, err)
	assert.Equal(t, 1, unresolved)
}

func TestRenderLineage(t *testing.T) {
	up := []dagDataset{{FullName: "main.s.raw", DatasetType: "STREAMING_TABLE"}}
	down := []dagDataset{{FullName: "main.s.report", DatasetType: "MATERIALIZED_VIEW"}}

	t.Run("text", func(t *testing.T) {
		cmd, buf := renderCmd(t, flags.OutputText)
		require.NoError(t, renderLineage(cmd, "main.s.orders", up, down, 0))
		want := "Lineage for main.s.orders\n\nUpstream:\n  main.s.raw\tSTREAMING_TABLE\n\nDownstream:\n  main.s.report\tMATERIALIZED_VIEW\n"
		assert.Equal(t, want, buf.String())
	})
	t.Run("text empty sides", func(t *testing.T) {
		cmd, buf := renderCmd(t, flags.OutputText)
		require.NoError(t, renderLineage(cmd, "main.s.orders", nil, nil, 0))
		assert.Contains(t, buf.String(), "Upstream:\n  (none)\n")
		assert.Contains(t, buf.String(), "Downstream:\n  (none)\n")
	})
	t.Run("text notes unresolved refs", func(t *testing.T) {
		cmd, buf := renderCmd(t, flags.OutputText)
		require.NoError(t, renderLineage(cmd, "main.s.orders", up, nil, 2))
		assert.Contains(t, buf.String(), "2 referenced node(s) are not defined in this pipeline")
	})
	t.Run("json includes unresolved refs", func(t *testing.T) {
		cmd, buf := renderCmd(t, flags.OutputJSON)
		require.NoError(t, renderLineage(cmd, "main.s.orders", up, nil, 3))
		assert.Contains(t, buf.String(), `"unresolved_refs": 3`)
	})
	t.Run("json omits zero unresolved refs", func(t *testing.T) {
		cmd, buf := renderCmd(t, flags.OutputJSON)
		require.NoError(t, renderLineage(cmd, "main.s.orders", up, nil, 0))
		assert.NotContains(t, buf.String(), "unresolved_refs")
	})
}
