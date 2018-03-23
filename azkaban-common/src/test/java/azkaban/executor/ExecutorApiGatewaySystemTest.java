package azkaban.executor;

import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Requires azkaban server running -> run AzkabanSingleServer first")
public class ExecutorApiGatewaySystemTest {

  private ExecutorApiGateway apiGateway;

  @Before
  public void setUp() throws Exception {
    ExecutorApiClient client = new ExecutorApiClient();
    apiGateway = new ExecutorApiGateway(client);
  }

  @Test
  public void update100Executions() throws Exception {
    updateExecutions(100);
  }

  @Test
  public void update300Executions() throws Exception {
    // used to fail because the URL is too long
    // works after switching to HTTP POST
    updateExecutions(300);
  }

  @Test
  public void update100kExecutions() throws Exception {
    // used to fail because the URL is too long
    // works after switching to HTTP POST
    updateExecutions(100_000);
  }

  private void updateExecutions(int count) throws ExecutorManagerException {
    final List<Integer> executionIdsList = new ArrayList<>();
    final List<Long> updateTimesList = new ArrayList<>();

    for (int i = 100000; i < 100000 + count; i++) {
      executionIdsList.add(i);
      updateTimesList.add(0L);
    }

    final Pair<String, String> executionIds =
        new Pair<>(ConnectorParams.EXEC_ID_LIST_PARAM,
            JSONUtils.toJSON(executionIdsList));

    final Pair<String, String> updateTimes =
        new Pair<>(
            ConnectorParams.UPDATE_TIME_LIST_PARAM,
            JSONUtils.toJSON(updateTimesList));

    Map<String, Object> results = apiGateway.callWithExecutionId("localhost", 12321,
        ConnectorParams.UPDATE_ACTION, null, null, executionIds, updateTimes);

    Assert.assertTrue(results != null);
    final List<Map<String, Object>> executionUpdates =
        (List<Map<String, Object>>) results
            .get(ConnectorParams.RESPONSE_UPDATED_FLOWS);
    Assert.assertEquals(count, executionUpdates.size());
    System.out.println("executionUpdates.get(count - 1): " + executionUpdates.get(count - 1));
  }

}
