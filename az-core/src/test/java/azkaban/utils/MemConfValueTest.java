package azkaban.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;

import azkaban.Constants;
import org.junit.Test;

public class MemConfValueTest {

  @Test
  public void parseMaxXmX() {
    assertXmx(Props.of(Constants.JobProperties.JOB_MAX_XMX, "1K"), "1K", 1L);
  }

  @Test
  public void parseMaxXmXDefault() {
    assertXmx(new Props(), Constants.JobProperties.MAX_XMX_DEFAULT, 2097152L);
  }

  @Test
  public void parseMaxXms() {
    assertXms(Props.of(Constants.JobProperties.JOB_MAX_XMS, "1K"), "1K", 1L);
  }

  @Test
  public void parseMaxXmsDefault() {
    assertXms(new Props(), Constants.JobProperties.MAX_XMS_DEFAULT, 1048576L);
  }

  @Test
  public void parseEmptyThrows() {
    final Throwable thrown = catchThrowable(() ->
        MemConfValue.parseMaxXmx(Props.of(Constants.JobProperties.JOB_MAX_XMX, "")));
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown).hasMessage("job.max.Xmx must not have an empty value. "
        + "Remove the property to use default or specify a valid value.");
  }

  private static void assertXmx(final Props props, final String expectedString, final long expectedSize) {
    assertMemConf(MemConfValue.parseMaxXmx(props), expectedString, expectedSize);
  }

  private static void assertXms(final Props props, final String expectedString, final long expectedSize) {
    assertMemConf(MemConfValue.parseMaxXms(props), expectedString, expectedSize);
  }

  private static void assertMemConf(final MemConfValue memConf, final String expectedString,
      final long expectedSize) {
    assertEquals(expectedString, memConf.getString());
    assertEquals(expectedSize, memConf.getSize());
  }

}
