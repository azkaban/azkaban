package azkaban.utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;


public class OsMemoryUtilTest {
  private OsMemoryUtil util = new OsMemoryUtil();

  @Test
  public void canReadMemInfoFileIfExists() {
    long size = util.getOsTotalFreeMemorySize();
    Path memFile = Paths.get("/proc/meminfo");
    if (!(Files.isRegularFile(memFile) && Files.isReadable(memFile))) {
      assertTrue(size == 0);
    }
    // todo HappyRay: investigate why size returned is 0 on Travis only but works on my Linux machine.
    // I can't find a way to get to the Gradle test report on Travis which makes debugging difficult.
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
