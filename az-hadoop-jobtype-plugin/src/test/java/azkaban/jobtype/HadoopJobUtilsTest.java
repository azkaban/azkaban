package azkaban.jobtype;

import static azkaban.Constants.FlowProperties.AZKABAN_FLOW_EXEC_ID;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.powermock.api.mockito.PowerMockito.when;

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;
import azkaban.utils.UndefinedPropertyException;
import azkaban.utils.YarnUtils;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HadoopJobUtils.class, YarnUtils.class})
public class HadoopJobUtilsTest {

  final private Logger log = Logger.getLogger(HadoopJobUtilsTest.class);

  @Test(expected = UndefinedPropertyException.class)
  public void testGetApplicationIDsToKillNoExecID() throws IOException, YarnException {
    Props props = new Props();
    // property missing
    // props.put(AZKABAN_FLOW_EXEC_ID, "dummy-id-1");
    YarnClient mockClient = Mockito.mock(YarnClient.class);

    // invoke
    Set<String> actual = HadoopJobUtils.getApplicationIDsToKill(mockClient, props, log);
  }

  @Test
  public void testGetApplicationIDsToKillYarnApiCallSucceed() throws IOException, YarnException {
    Props props = new Props();
    props.put(AZKABAN_FLOW_EXEC_ID, "dummy-id-1");
    YarnClient mockClient = Mockito.mock(YarnClient.class);

    PowerMockito.mockStatic(YarnUtils.class);
    when(YarnUtils.getAllAliveAppIDsByExecID(any(), any(), eq(log))).thenReturn(
        ImmutableSet.of("application_1234_1234", "application_4321_4321"));

    // invoke
    Set<String> actual = HadoopJobUtils.getApplicationIDsToKill(mockClient, props, log);
    assertTrue(
        actual.containsAll(ImmutableSet.of("application_1234_1234", "application_4321_4321")));
  }

  @Test(expected = UndefinedPropertyException.class)
  public void testGetApplicationIDsToKillYarnApiCallFailNoLogFilePath() throws IOException,
      YarnException {
    Props props = new Props();
    props.put(AZKABAN_FLOW_EXEC_ID, "dummy-id-1");
    // property missing
    // props.put(CommonJobProperties.JOB_LOG_FILE, "dummy/file/path");

    YarnClient mockClient = Mockito.mock(YarnClient.class);

    PowerMockito.mockStatic(YarnUtils.class);
    when(YarnUtils.getAllAliveAppIDsByExecID(any(), any(), eq(log))).thenThrow(
        new YarnException("ops"));

    // invoke
    HadoopJobUtils.getApplicationIDsToKill(mockClient, props, log);
  }

  @Test
  public void testGetApplicationIDsToKillYarnApiCallFailReadLogSucceed() throws IOException,
      YarnException {
    Props props = new Props();
    props.put(AZKABAN_FLOW_EXEC_ID, "dummy-id-1");
    props.put(CommonJobProperties.JOB_LOG_FILE, "dummy/file/path");

    YarnClient mockClient = Mockito.mock(YarnClient.class);

    PowerMockito.mockStatic(YarnUtils.class);
    when(YarnUtils.getAllAliveAppIDsByExecID(any(), any(), eq(log))).thenThrow(
        new YarnException("ops"));

    PowerMockito.mockStatic(HadoopJobUtils.class, invocation -> {
      if (invocation.getMethod().getName().equals("findApplicationIdFromLog")) {
        return ImmutableSet.of("application_6789_6789", "application_9876_9876");
      }
      return invocation.callRealMethod();
    });

    // invoke
    Set<String> actual = HadoopJobUtils.getApplicationIDsToKill(mockClient, props, log);
    assertTrue(
        actual.containsAll(ImmutableSet.of("application_6789_6789", "application_9876_9876")));
  }

  @Test(expected = Exception.class)
  public void testGetApplicationIDsToKillBothFail() throws IOException,
      YarnException {
    Props props = new Props();
    props.put(AZKABAN_FLOW_EXEC_ID, "dummy-id-1");
    props.put(CommonJobProperties.JOB_LOG_FILE, "dummy/file/path");

    YarnClient mockClient = Mockito.mock(YarnClient.class);

    PowerMockito.mockStatic(YarnUtils.class);
    when(YarnUtils.getAllAliveAppIDsByExecID(any(), any(), eq(log))).thenThrow(
        new YarnException("ops"));

    PowerMockito.mockStatic(HadoopJobUtils.class, invocation -> {
      if (invocation.getMethod().getName().equals("findApplicationIdFromLog")) {
        throw new Exception("Ops");
      }
      return invocation.callRealMethod();
    });

    // invoke
    HadoopJobUtils.getApplicationIDsToKill(mockClient, props, log);
  }
}