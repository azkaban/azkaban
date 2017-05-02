package azkaban.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;


public class OsMemoryUtilTest {
  private OsMemoryUtil util = new OsMemoryUtil();

  @Test
  public void canReadMemInfoFileIfExists() {
    if (util.doesMemInfoFileExist()) {
      util.getOsTotalFreeMemorySize();
    }
  }

  @Test
  public void getOsTotalFreeMemorySize() {
    List<String> lines =
        Arrays.asList("MemFree:        1 kB", "Buffers:          2 kB", "Cached:          3 kB", "SwapFree:    4 kB",
            "Foo: 10 kB");

    long size = util.getOsTotalFreeMemorySizeFromStrings(lines);
    assertEquals(10, size);
  }

  @Test
  public void getOsTotalFreeMemorySizeMissingEntry() {
    List<String> lines = Arrays.asList("MemFree:        1 kB", "Foo: 10 kB");

    long size = util.getOsTotalFreeMemorySizeFromStrings(lines);
    assertEquals(0, size);
  }

  @Test
  public void getOsTotalFreeMemorySizeWrongEntry() {
    List<String> lines = Collections.singletonList("MemFree:        foo kB");

    long size = util.getOsTotalFreeMemorySizeFromStrings(lines);
    assertEquals(0, size);
  }

  @Test
  public void parseMemoryLine() {
    String line = "MemFree:        500 kB";
    long size = util.parseMemoryLine(line);
    assertEquals(500, size);
  }

  @Test
  public void parseIncorrectMemoryLine() {
    String line = "MemFree:        ab kB";
    long size = util.parseMemoryLine(line);
    assertEquals(0, size);
  }
}
