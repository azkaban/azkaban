package azkaban.jobtype;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.utils.Props;

import org.junit.Before;
import org.junit.Test;

/**
 * Test class for constructHadoopTags method in HadoopJobUtils
 */
public class TestHadoopJobUtilsConstructHadoopTags {

  private Props props;

  @Before
  public void beforeMethod() {
    props = new Props();
  }

  @Test
  public void testNoTags() {
    String[] tags = new String[0];
    assertThat(HadoopJobUtils.constructHadoopTags(props, tags)).isEqualTo("");
  }

  @Test
  public void testWithTags() {
    String tag0 = "tag0";
    String tag1 = "tag1";
    props.put(tag0, "val0");
    props.put(tag1, "val1");
    String[] tags = new String[] { tag0, tag1 };
    assertThat(HadoopJobUtils.constructHadoopTags(props, tags))
        .isEqualTo("tag0:val0,tag1:val1");
  }

  @Test
  public void testWithNonExistentTags() {
    String tag0 = "tag0";
    String tag1 = "tag1";
    String tag2 = "tag2";
    props.put(tag0, "val0");
    props.put(tag2, "val2");
    String[] tags = new String[] { tag0, tag1, tag2 };
    assertThat(HadoopJobUtils.constructHadoopTags(props, tags))
        .isEqualTo("tag0:val0,tag2:val2");
  }
}
