package testserver

import (
	"net/url"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestPipeline(t *testing.T, workspace *FakeWorkspace) string {
	createReq := Request{
		Body: []byte(`{
			"name": "Test Pipeline",
			"storage": "dbfs:/pipelines/test-pipeline"
		}`),
	}

	createResponse := workspace.PipelineCreate(createReq)
	// StatusCode 0 gets converted to 200 by normalizeResponse in the server
	require.Equal(t, 0, createResponse.StatusCode)

	createPipelineResponse, ok := createResponse.Body.(pipelines.CreatePipelineResponse)
	require.True(t, ok)
	return createPipelineResponse.PipelineId
}

func TestPipelineCreate_RejectsDottedTargetSchemaName(t *testing.T) {
	workspace := NewFakeWorkspace("http://test", "dbapi123")

	response := workspace.PipelineCreate(Request{
		Body: []byte(`{"name": "p", "catalog": "main", "target": "main.test_schema"}`),
	})
	assert.Equal(t, 400, response.StatusCode)

	body, ok := response.Body.(map[string]string)
	require.True(t, ok)
	assert.Equal(t, "INVALID_PARAMETER_VALUE", body["error_code"])
	assert.Contains(t, body["message"], `target_schema_name "main.test_schema"`)
}

func TestPipelineCreate_AllowsSingleSegmentTargetSchemaName(t *testing.T) {
	workspace := NewFakeWorkspace("http://test", "dbapi123")

	response := workspace.PipelineCreate(Request{
		Body: []byte(`{"name": "p", "catalog": "main", "target": "test_schema"}`),
	})
	assert.Equal(t, 0, response.StatusCode)
}

func TestPipelineStartUpdate_HandlesNonExistentPipeline(t *testing.T) {
	workspace := NewFakeWorkspace("http://test", "dbapi123")

	response := workspace.PipelineStartUpdate(Request{}, "non-existent-pipeline")
	assert.Equal(t, 404, response.StatusCode)
	assert.Contains(t, response.Body.(map[string]string)["message"], "The specified pipeline non-existent-pipeline was not found")
}

func TestPipelineGetUpdate_HandlesNonExistent(t *testing.T) {
	workspace := NewFakeWorkspace("http://test", "dbapi123")

	response := workspace.PipelineGetUpdate("non-existent-pipeline", "some-update-id")
	assert.Equal(t, 404, response.StatusCode)

	pipelineId := createTestPipeline(t, workspace)

	response = workspace.PipelineGetUpdate(pipelineId, "non-existent-update")
	assert.Equal(t, 404, response.StatusCode)
	assert.Contains(t, response.Body.(map[string]string)["message"], "The specified update non-existent-update was not found")
}

func TestPipelineDataflowGraph_RejectsPageSizeAboveLeafMax(t *testing.T) {
	workspace := NewFakeWorkspace("http://test", "dbapi123")
	pipelineId := createTestPipeline(t, workspace)

	graphReq := func(pageSize string) Request {
		return Request{URL: &url.URL{RawQuery: url.Values{"page_size": {pageSize}}.Encode()}}
	}

	// Diagnostics max is 20; nodes/flows max is 50. The backend rejects (does not clamp) an
	// oversized page_size with a 400, so the fake must too.
	resp := workspace.PipelineDataflowGraph(graphReq("50"), pipelineId, "diagnostics")
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "INVALID_PARAMETER_VALUE", resp.Body.(map[string]string)["error_code"])

	assert.Equal(t, 400, workspace.PipelineDataflowGraph(graphReq("51"), pipelineId, "nodes").StatusCode)
	assert.Equal(t, 0, workspace.PipelineDataflowGraph(graphReq("20"), pipelineId, "diagnostics").StatusCode)
	assert.Equal(t, 0, workspace.PipelineDataflowGraph(graphReq("50"), pipelineId, "nodes").StatusCode)
}

func TestPipelineStop_AfterUpdate(t *testing.T) {
	workspace := NewFakeWorkspace("http://test", "dbapi123")

	pipelineId := createTestPipeline(t, workspace)

	startResponse := workspace.PipelineStartUpdate(Request{}, pipelineId)
	assert.Equal(t, 0, startResponse.StatusCode)

	stopResponse := workspace.PipelineStop(pipelineId)
	assert.Equal(t, 0, stopResponse.StatusCode)

	stopBody, ok := stopResponse.Body.(pipelines.GetPipelineResponse)
	require.True(t, ok)
	assert.Equal(t, pipelineId, stopBody.PipelineId)
	assert.Equal(t, pipelines.PipelineStateIdle, stopBody.State)
}
