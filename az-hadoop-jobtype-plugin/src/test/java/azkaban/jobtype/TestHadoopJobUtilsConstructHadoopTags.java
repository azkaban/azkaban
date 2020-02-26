package azkaban.jobtype;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;

import java.util.Set;
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

  @Test
  public void testWorkflowIdTag() {
    String tag0 = "tag0";
    props.put(tag0, "val0");
    props.put(CommonJobProperties.PROJECT_NAME, "project-name");
    props.put(CommonJobProperties.FLOW_ID, "flow-id");
    String[] tags = new String[] { tag0 };
    assertThat(HadoopJobUtils.constructHadoopTags(props, tags))
        .isEqualTo("tag0:val0,workflowid:project-name$flow-id");
  }

  @Test
  public void testLongWorkflowIdTag() {
    String tag0 = "tag0";
    props.put(tag0, "val0");
    StringBuffer flowIdBuffer = new StringBuffer(150);
    for (int i = 0; i < 150; i++) {
      flowIdBuffer.append("f");
    }
    props.put(CommonJobProperties.PROJECT_NAME, "project-name");
    props.put(CommonJobProperties.FLOW_ID, flowIdBuffer.toString());
    String[] tags = new String[] { tag0 };
    String[] actualTags = HadoopJobUtils.constructHadoopTags(props, tags).split(",");
    assertThat(actualTags[0]).isEqualTo("tag0:val0");
    assertThat(actualTags[1].length()).isLessThanOrEqualTo(HadoopJobUtils.APPLICATION_TAG_MAX_LENGTH);
    String prefix = "workflowid:project-name$";
    StringBuffer expectedTagBuffer = new StringBuffer();
    expectedTagBuffer.append(prefix);
    for (int i = prefix.length(); i < HadoopJobUtils.APPLICATION_TAG_MAX_LENGTH; i++) {
      expectedTagBuffer.append("f");
    }
    assertThat(actualTags[1]).isEqualTo(expectedTagBuffer);
  }
}
