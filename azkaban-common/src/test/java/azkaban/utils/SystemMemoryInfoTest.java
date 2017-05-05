package azkaban.utils;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class SystemMemoryInfoTest {
  private static final long GB_UNIT = 1024L * 1024L;

  @Test
  public void grantedIfFreeMemoryAvailable() throws Exception {
    OsMemoryUtil memUtil = mock(OsMemoryUtil.class);
    long availableFreeMem = 10L * 1024L * 1024L; //10 GB
    when(memUtil.getOsTotalFreeMemorySize()).thenReturn(availableFreeMem);
    SystemMemoryInfo memInfo = new SystemMemoryInfo(memUtil);
    boolean isGranted = memInfo.canSystemGrantMemory(1);
    assertTrue(isGranted);
  }

  @Test
  public void notGrantedIfFreeMemoryAvailableLessThanMinimal() throws Exception {
    OsMemoryUtil memUtil = mock(OsMemoryUtil.class);
    long availableFreeMem = 4L * 1024L * 1024L; //4 GB
    when(memUtil.getOsTotalFreeMemorySize()).thenReturn(availableFreeMem);
    SystemMemoryInfo memInfo = new SystemMemoryInfo(memUtil);
    long xmx = 2 * GB_UNIT; //2 GB
    boolean isGranted = memInfo.canSystemGrantMemory(xmx);
    assertFalse(isGranted);
  }

  @Test
  public void grantedIfFreeMemoryCheckReturnsZero() throws Exception {
    OsMemoryUtil memUtil = mock(OsMemoryUtil.class);
    long availableFreeMem = 0;
    when(memUtil.getOsTotalFreeMemorySize()).thenReturn(availableFreeMem);
    SystemMemoryInfo memInfo = new SystemMemoryInfo(memUtil);
    long xmx = 0;
    boolean isGranted = memInfo.canSystemGrantMemory(xmx);
    assertTrue("Memory check failed. Should fail open", isGranted);
  }
}
