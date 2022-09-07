package azkaban.utils;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;
import org.assertj.core.util.Sets;
import org.junit.Test;

public class YarnUtilsTest {

  final private Logger log = Logger.getLogger(YarnUtilsTest.class);

  @Test
  public void testKillAppOnCluster() throws IOException, YarnException {
    YarnClient mockClient = mock(YarnClient.class);
    doNothing().when(mockClient).killApplication(any());
    YarnUtils.killAppOnCluster(mockClient, "application_123_456", log);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testKillAppOnClusterInvalidAppID() throws IOException, YarnException {
    YarnClient mockClient = mock(YarnClient.class);
    doNothing().when(mockClient).killApplication(any());
    YarnUtils.killAppOnCluster(mockClient, "application+++123===456", log);
  }

  @Test(expected = YarnException.class)
  public void testKillAppOnClusterYarnFail() throws IOException, YarnException {
    YarnClient mockClient = mock(YarnClient.class);
    doThrow(new YarnException("ops")).when(mockClient).killApplication(any());
    YarnUtils.killAppOnCluster(mockClient, "application_123_456", log);
  }

  @Test
  public void testGetAllAliveAppIDsByExecID() throws IOException, YarnException {
    YarnClient mockClient = mock(YarnClient.class);

    ApplicationReport report1 = mock(ApplicationReport.class);
    ApplicationReport report2 = mock(ApplicationReport.class);
    doReturn(ApplicationId.newInstance(1234, 1234)).when(report1).getApplicationId();
    doReturn(ApplicationId.newInstance(6789, 6789)).when(report2).getApplicationId();

    List<ApplicationReport> applicationReports = Lists.newArrayList(report1, report2);
    doReturn(applicationReports).when(mockClient).getApplications(eq(null), any(), any());
    Set<String> actual = YarnUtils.getAllAliveAppIDsByExecID(
        mockClient, "dummy-exec-id-123", log);

    assertEquals(2, actual.size());
    assertTrue(actual.contains("application_1234_1234"));
    assertTrue(actual.contains("application_6789_6789"));
  }

  @Test(expected = YarnException.class)
  public void testGetAllAliveAppIDsByExecIDYarnFai() throws IOException, YarnException {
    YarnClient mockClient = mock(YarnClient.class);
    doThrow(new YarnException("ops")).when(mockClient).getApplications(eq(null), any(), any());
    Set<String> actual = YarnUtils.getAllAliveAppIDsByExecID(
        mockClient, "dummy-exec-id-123", log);
  }
}
