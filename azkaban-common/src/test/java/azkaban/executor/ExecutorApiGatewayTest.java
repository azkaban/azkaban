package azkaban.executor;

import static org.mockito.Mockito.when;

import azkaban.utils.JSONUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ExecutorApiGatewayTest {

  private ExecutorApiGateway gateway;
  private ExecutorApiClient client;

  @Before
  public void setUp() throws Exception {
    this.client = Mockito.mock(ExecutorApiClient.class);
    this.gateway = new ExecutorApiGateway(this.client);
  }

  @Test
  public void testExecutorInfoJsonParser() throws Exception {
    final ExecutorInfo exeInfo = new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 89,
        10);
    final String json = JSONUtils.toJSON(exeInfo);
    when(this.client.httpGet(Mockito.any(), Mockito.any())).thenReturn(json);
    final ExecutorInfo exeInfo2 = this.gateway
        .callExecutorForJsonType("localhost", 1234, "executor", null, ExecutorInfo.class);
    Assert.assertTrue(exeInfo.equals(exeInfo2));
  }

}
