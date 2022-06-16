package azkaban.webapp;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;

import azkaban.Constants.ConfigurationKeys;
import azkaban.logs.JdbcExecutionLogsLoader;
import azkaban.utils.Props;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ExecutionLogsCleanerTest {
  private Props props;
  private JdbcExecutionLogsLoader loader;
  private ExecutionLogsCleaner executionLogsCleaner;

  @Before
  public void setUp() {
    this.props = new Props();
    /* This config will set the thread to run every 2 seconds */
    this.props.put(ConfigurationKeys.EXECUTION_LOGS_CLEANUP_INTERVAL_SECONDS, 2);
    this.loader = mock(JdbcExecutionLogsLoader.class);
    this.executionLogsCleaner = new ExecutionLogsCleaner(this.props, this.loader);
  }

  @Test
  public void checkIfExecutionCleanerGetsTriggered() throws Exception {
    executionLogsCleaner.start();
    TimeUnit.SECONDS.sleep(5);
    Mockito.verify(this.loader, atLeast(2)).removeExecutionLogsByTime(anyLong(), anyInt());
  }
}
