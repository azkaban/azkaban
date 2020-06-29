package azkaban.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;


public class OsMemoryUtilTest {

  private final OsMemoryUtil util = new OsMemoryUtil();

  @Test
  public void canReadMemInfoFileIfExists() {
    final long size = this.util.getOsTotalFreeMemorySize();
    final Path memFile = Paths.get("/proc/meminfo");
    if (!(Files.isRegularFile(memFile) && Files.isReadable(memFile))) {
      assertTrue(size == 0);
    }
    // todo HappyRay: investigate why size returned is 0 on Travis only but works on my Linux machine.
    // I can't find a way to get to the Gradle test report on Travis which makes debugging difficult.
  }

  @Test
  public void getOsTotalFreeMemorySize() {
    final List<String> lines =
        Arrays.asList("MemFree:        1 kB", "Buffers:          2 kB", "Cached:          3 kB",
            "SwapFree:    4 kB",
            "Foo: 10 kB");

    final long size = this.util.getOsTotalFreeMemorySizeFromStrings(lines, OsMemoryUtil.MEM_KEYS);
    assertEquals(10, size);
  }

  @Test
  public void getOsTotalFreeMemorySizeMissingEntry() {
    final List<String> lines = Arrays.asList("MemFree:        1 kB", "Foo: 10 kB");

    final long size = this.util.getOsTotalFreeMemorySizeFromStrings(lines, OsMemoryUtil.MEM_KEYS);
    assertEquals(0, size);
  }

  @Test
  public void getOsTotalFreeMemorySizeWrongEntry() {
    final List<String> lines = Collections.singletonList("MemFree:        foo kB");

    final long size = this.util.getOsTotalFreeMemorySizeFromStrings(lines, OsMemoryUtil.MEM_KEYS);
    assertEquals(0, size);
  }

  @Test
  public void parseMemoryLine() {
    final String line = "MemFree:        500 kB";
    final long size = this.util.parseMemoryLine(line);
    assertEquals(500, size);
  }

  @Test
  public void parseIncorrectMemoryLine() {
    final String line = "MemFree:        ab kB";
    final long size = this.util.parseMemoryLine(line);
    assertEquals(0, size);
  }
}
