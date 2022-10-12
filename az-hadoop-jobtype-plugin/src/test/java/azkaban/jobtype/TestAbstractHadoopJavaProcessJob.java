package azkaban.jobtype;

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;


public class TestAbstractHadoopJavaProcessJob {

  private static Logger logger = Logger.getLogger(TestAbstractHadoopJavaProcessJob.class);

  @BeforeClass
  public static void setUpClass() {
    azkaban.test.Utils.initServiceProvider();
  }

  @Test
  public void testWorkflowIdTag() {
    Props props = new Props();
    props.put(CommonJobProperties.EXEC_ID, "123");
    props.put(CommonJobProperties.PROJECT_NAME, "project-name");
    props.put(CommonJobProperties.FLOW_ID, "flow-id");
    AbstractHadoopJavaProcessJob job = new AbstractHadoopJavaProcessJob("test", new Props(), props, logger) {};
    job.setupHadoopJobProperties();
    assertThat(job.getJobProps().get(HadoopConfigurationInjector.INJECT_PREFIX + HadoopJobUtils.MAPREDUCE_JOB_TAGS))
        .isEqualTo("azkaban.flow.execid:123,azkaban.flow.flowid:flow-id"
            + ",azkaban.flow.projectname:project-name,workflowid:project-name$flow-id");
  }

  @Test
  public void testLongWorkflowIdTag() {
    Props props = new Props();
    StringBuffer flowIdBuffer = new StringBuffer("flow-id");
    for (int i = 0; i < 150; i++) {
      flowIdBuffer.append("f");
    }
    props.put(CommonJobProperties.EXEC_ID, "123");
    props.put(CommonJobProperties.PROJECT_NAME, "project-name");
    props.put(CommonJobProperties.FLOW_ID, flowIdBuffer.toString());
    AbstractHadoopJavaProcessJob job = new AbstractHadoopJavaProcessJob("test2", new Props(),
        props, logger) {};
    job.setupHadoopJobProperties();
    String[] actualTags = job.getJobProps().get(
        HadoopConfigurationInjector.INJECT_PREFIX + HadoopJobUtils.MAPREDUCE_JOB_TAGS)
        .split(",");
    assertThat(actualTags.length).isEqualTo(4);
    assertThat(actualTags[0]).isEqualTo("azkaban.flow.execid:123");
    String flowPrefix = "azkaban.flow.flowid:flow-id";
    StringBuffer expectedFlowTagBuffer = new StringBuffer(flowPrefix);
    for (int i = flowPrefix.length(); i < HadoopJobUtils.APPLICATION_TAG_MAX_LENGTH; i++) {
      expectedFlowTagBuffer.append("f");
    }
    assertThat(actualTags[1]).isEqualTo(expectedFlowTagBuffer.toString());
    assertThat(actualTags[2]).isEqualTo("azkaban.flow.projectname:project-name");
    assertThat(actualTags[3].length()).isLessThanOrEqualTo(HadoopJobUtils.APPLICATION_TAG_MAX_LENGTH);
    String workflowPrefix = "workflowid:project-name$flow-id";
    StringBuffer expectedTagBuffer = new StringBuffer();
    expectedTagBuffer.append(workflowPrefix);
    for (int i = workflowPrefix.length(); i < HadoopJobUtils.APPLICATION_TAG_MAX_LENGTH; i++) {
      expectedTagBuffer.append("f");
    }
    assertThat(actualTags[3]).isEqualTo(expectedTagBuffer.toString());
  }
}
