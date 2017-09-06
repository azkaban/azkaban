package azkaban.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;


public class SystemMemoryInfoTest {

  private static final long GB_UNIT = 1024L * 1024L;

  @Test
  public void grantedIfFreeMemoryAvailable() throws Exception {
    final OsMemoryUtil memUtil = mock(OsMemoryUtil.class);
    final long availableFreeMem = 10L * 1024L * 1024L; //10 GB
    when(memUtil.getOsTotalFreeMemorySize()).thenReturn(availableFreeMem);
    final SystemMemoryInfo memInfo = new SystemMemoryInfo(memUtil);
    final boolean isGranted = memInfo.canSystemGrantMemory(1);
    assertTrue(isGranted);
  }

  @Test
  public void notGrantedIfFreeMemoryAvailableLessThanMinimal() throws Exception {
    final OsMemoryUtil memUtil = mock(OsMemoryUtil.class);
    final long availableFreeMem = 4L * 1024L * 1024L; //4 GB
    when(memUtil.getOsTotalFreeMemorySize()).thenReturn(availableFreeMem);
    final SystemMemoryInfo memInfo = new SystemMemoryInfo(memUtil);
    final long xmx = 2 * GB_UNIT; //2 GB
    final boolean isGranted = memInfo.canSystemGrantMemory(xmx);
    assertFalse(isGranted);
  }

  @Test
  public void grantedIfFreeMemoryCheckReturnsZero() throws Exception {
    final OsMemoryUtil memUtil = mock(OsMemoryUtil.class);
    final long availableFreeMem = 0;
    when(memUtil.getOsTotalFreeMemorySize()).thenReturn(availableFreeMem);
    final SystemMemoryInfo memInfo = new SystemMemoryInfo(memUtil);
    final long xmx = 0;
    final boolean isGranted = memInfo.canSystemGrantMemory(xmx);
    assertTrue("Memory check failed. Should fail open", isGranted);
  }
}
