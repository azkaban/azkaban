package azkaban.jobtype;

import azkaban.utils.Props;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

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
    Assert.assertEquals("", HadoopJobUtils.constructHadoopTags(props, tags));
  }

  @Test
  public void testWithTags() {
    String tag0 = "tag0";
    String tag1 = "tag1";
    props.put(tag0, "val0");
    props.put(tag1, "val1");
    String[] tags = new String[] { tag0, tag1 };
    Assert.assertEquals("tag0:val0,tag1:val1", HadoopJobUtils.constructHadoopTags(props, tags));
  }

  @Test
  public void testWithNonExistentTags() {
    String tag0 = "tag0";
    String tag1 = "tag1";
    String tag2 = "tag2";
    props.put(tag0, "val0");
    props.put(tag2, "val2");
    String[] tags = new String[] { tag0, tag1, tag2 };
    Assert.assertEquals("tag0:val0,tag2:val2", HadoopJobUtils.constructHadoopTags(props, tags));
  }
}
