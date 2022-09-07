package azkaban.utils;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;
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
}
