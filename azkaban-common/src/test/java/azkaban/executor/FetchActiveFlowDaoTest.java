package azkaban.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import azkaban.db.EncodingType;
import azkaban.executor.FetchActiveFlowDao.FetchActiveExecutableFlows;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.TestUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Also @see ExecutionFlowDaoTest - DB operations of FetchActiveFlowDao are tested there.
 */
public class FetchActiveFlowDaoTest {

  private ResultSet rs;

  @Before
  public void setUp() throws Exception {
    this.rs = Mockito.mock(ResultSet.class);
  }

  @Test
  public void handleResultMissingExecutor() throws Exception {
    final FetchActiveExecutableFlows resultHandler = new FetchActiveExecutableFlows();
    mockResultWithData();
    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> result = resultHandler
        .handle(this.rs);
    assertThat(result.containsKey(1)).isTrue();
    assertThat(result.get(1).getFirst().getExecutor().isPresent()).isFalse();
  }

  @Test
  public void handleResultNullData() throws Exception {
    final FetchActiveExecutableFlows resultHandler = new FetchActiveExecutableFlows();
    mockResultWithNullData();
    final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> result = resultHandler
        .handle(this.rs);
    assertThat(result).isEmpty();
  }

  private void mockResultWithData() throws Exception {
    final ExecutableFlow flow = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    final String json = JSONUtils.toJSON(flow.toObject());
    final byte[] data = json.getBytes("UTF-8");
    mockExecution(EncodingType.PLAIN.getNumVal(), data);
  }

  private void mockResultWithNullData() throws SQLException {
    mockExecution(0, null);
  }

  private void mockExecution(final int encodingType, final byte[] flowData) throws SQLException {
    when(this.rs.next()).thenReturn(true).thenReturn(false);
    // execution id
    when(this.rs.getInt(1)).thenReturn(1);
    // encodingType
    when(this.rs.getInt(2)).thenReturn(encodingType);
    // data
    when(this.rs.getBytes(3)).thenReturn(flowData);
    // executor host
    when(this.rs.getString(4)).thenReturn(null);
    // executor port
    when(this.rs.getInt(5)).thenReturn(0);
    // executorId
    when(this.rs.getInt(6)).thenReturn(1);
    // executorStatus
    when(this.rs.getBoolean(7)).thenReturn(false);
  }

}
