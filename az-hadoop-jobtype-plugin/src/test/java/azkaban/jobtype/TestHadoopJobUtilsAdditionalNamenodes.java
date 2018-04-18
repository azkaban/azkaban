package azkaban.jobtype;

import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Test;


public class TestHadoopJobUtilsAdditionalNamenodes {

  @Test
  public void testAdditionalNamenodes() {
    Props testProps = new Props();
    HadoopJobUtils.addAdditionalNamenodesToProps(testProps, "hdfs://testNN:9000");
    Assert.assertEquals("hdfs://testNN:9000", testProps.get("other_namenodes"));

    testProps = new Props();
    testProps.put("other_namenodes", "hdfs://testNN1:9000,hdfs://testNN2:9000");
    HadoopJobUtils.addAdditionalNamenodesToProps(testProps, "hdfs://testNN:9000");
    Assert.assertEquals("hdfs://testNN1:9000,hdfs://testNN2:9000,hdfs://testNN:9000",
        testProps.get("other_namenodes"));
  }

}
