package azkaban.jobtype;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


@SuppressWarnings("DefaultCharset")
public class TestHadoopJobUtilsFindApplicationIdFromLog {

  File tempFile = null;

  BufferedWriter bw = null;

  Logger logger = Logger.getRootLogger();

  @Before
  public void beforeMethod() throws IOException {
    tempFile = File.createTempFile("test_hadoop_job_utils_find_application_id_from_log", null);
    bw = new BufferedWriter(new FileWriter(tempFile));

  }

  @Test
  public void testNoApplicationId() throws IOException {
    bw.write("28-08-2015 14:05:24 PDT spark INFO - 15/08/28 21:05:24 INFO client.RMProxy: Connecting to ResourceManager at eat1-nertzrm02.grid.linkedin.com/172.20.158.95:8032\n");
    bw.write("28-08-2015 14:05:24 PDT spark INFO - 15/08/28 21:05:24 INFO yarn.Client: Requesting a new application from cluster with 134 NodeManagers\n");
    bw.write("28-08-2015 14:05:24 PDT spark INFO - 15/08/28 21:05:24 INFO yarn.Client: Verifying our application has not requested more than the maximum memory capability of the cluster (55296 MB per container)\n");
    bw.write("28-08-2015 14:05:24 PDT spark INFO - 15/08/28 21:05:24 INFO yarn.Client: Will allocate AM container, with 4505 MB memory including 409 MB overhead\n");
    bw.write("28-08-2015 14:05:24 PDT spark INFO - 15/08/28 21:05:24 INFO yarn.Client: Setting up container launch context for our AM\n");
    bw.write("28-08-2015 14:05:24 PDT spark INFO - 15/08/28 21:05:24 INFO yarn.Client: Preparing resources for our AM container\n");
    bw.close();

    Set<String> appId = HadoopJobUtils.findApplicationIdFromLog(tempFile.toString(), logger);

    Assert.assertEquals(0, appId.size());

  }

  @Test
  public void testOneApplicationId() throws IOException {
    bw.write("28-08-2015 14:05:32 PDT spark INFO - 15/08/28 21:05:32 INFO spark.SecurityManager: SecurityManager: authentication enabled; ui acls enabled; users with view permissions: Set(*); users with modify permissions: Set(azkaban, jyu)\n");
    bw.write("28-08-2015 14:05:32 PDT spark INFO - 15/08/28 21:05:32 INFO yarn.Client: Submitting application 3099 to ResourceManager\n");
    bw.write("28-08-2015 14:05:33 PDT spark INFO - 15/08/28 21:05:33 INFO impl.YarnClientImpl: Submitted application application_1440264346270_3099\n");
    bw.close();

    Set<String> appId = HadoopJobUtils.findApplicationIdFromLog(tempFile.toString(), logger);

    Assert.assertEquals(1, appId.size());
    Assert.assertTrue(appId.contains("application_1440264346270_3099"));
  }

  @Test
  public void testMultipleSameApplicationIdWhenSparkStarts() throws IOException {
    bw.write("28-08-2015 14:05:34 PDT spark INFO - 15/08/28 21:05:34 INFO yarn.Client: Application report for application_1440264346270_3099 (state: ACCEPTED)\n");
    bw.write("28-08-2015 14:05:34 PDT spark INFO - 15/08/28 21:05:34 INFO yarn.Client: \n");
    bw.write("28-08-2015 14:05:34 PDT spark INFO -   client token: Token { kind: YARN_CLIENT_TOKEN, service:  }\n");
    bw.write("28-08-2015 14:05:34 PDT spark INFO -   diagnostics: N/A\n");
    bw.write("28-08-2015 14:05:34 PDT spark INFO -   ApplicationMaster host: N/A\n");
    bw.write("28-08-2015 14:05:34 PDT spark INFO -   ApplicationMaster RPC port: -1\n");
    bw.write("28-08-2015 14:05:34 PDT spark INFO -   queue: default\n");
    bw.write("28-08-2015 14:05:34 PDT spark INFO -   start time: 1440795932813\n");
    bw.write("28-08-2015 14:05:34 PDT spark INFO -   final status: UNDEFINED\n");
    bw.write("28-08-2015 14:05:34 PDT spark INFO -   tracking URL: http://eat1-nertzwp02.grid.linkedin.com:8080/proxy/application_1440264346270_3099/\n");
    bw.write("28-08-2015 14:05:34 PDT spark INFO -   user: jyu\n");
    bw.write("28-08-2015 14:05:35 PDT spark INFO - 15/08/28 21:05:35 INFO yarn.Client: Application report for application_1440264346270_3099 (state: ACCEPTED)\n");
    bw.close();

    Set<String> appId = HadoopJobUtils.findApplicationIdFromLog(tempFile.toString(), logger);

    Assert.assertEquals(1, appId.size());
    Assert.assertTrue(appId.contains("application_1440264346270_3099"));
  }

  @Test
  public void testMultipleSameApplicationIdForSparkAfterRunningFor17Hours() throws IOException {
    bw.write("28-08-2015 14:11:50 PDT spark INFO - 15/08/28 21:11:50 INFO yarn.Client: Application report for application_1440264346270_3099 (state: RUNNING)\n");
    bw.write("28-08-2015 14:11:51 PDT spark INFO - 15/08/28 21:11:51 INFO yarn.Client: Application report for application_1440264346270_3099 (state: RUNNING)\n");
    bw.write("28-08-2015 14:11:52 PDT spark INFO - 15/08/28 21:11:52 INFO yarn.Client: Application report for application_1440264346270_3099 (state: RUNNING)\n");
    bw.write("28-08-2015 14:11:53 PDT spark INFO - 15/08/28 21:11:53 INFO yarn.Client: Application report for application_1440264346270_3099 (state: RUNNING)\n");
    bw.write("28-08-2015 14:11:54 PDT spark INFO - 15/08/28 21:11:54 INFO yarn.Client: Application report for application_1440264346270_3099 (state: RUNNING)\n");
    bw.close();

    Set<String> appId = HadoopJobUtils.findApplicationIdFromLog(tempFile.toString(), logger);

    Assert.assertEquals(1, appId.size());
    Assert.assertTrue(appId.contains("application_1440264346270_3099"));
  }

  @Test
  public void testLogWithMultipleApplicationIdsAppearingMultipleTimes() throws IOException {
    bw.write("28-08-2015 12:29:38 PDT Training_clickSelectFeatures INFO - INFO Submitted application application_1440264346270_3044\n");
    bw.write("28-08-2015 12:29:38 PDT Training_clickSelectFeatures INFO - INFO The url to track the job: http://eat1-nertzwp02.grid.linkedin.com:8080/proxy/application_1440264346270_3044/\n");
    bw.write("28-08-2015 12:29:38 PDT Training_clickSelectFeatures INFO - INFO See http://eat1-nertzwp02.grid.linkedin.com:8080/proxy/application_1440264346270_3044/ for details.\n");
    bw.write("28-08-2015 12:29:38 PDT Training_clickSelectFeatures INFO - INFO Running job: job_1440264346270_3044\n");
    bw.write("28-08-2015 12:30:21 PDT Training_clickSelectFeatures INFO - INFO Closing idle connection Socket[addr=eat1-hcl5481.grid.linkedin.com/172.20.138.228,port=42492,localport=42382] to server eat1-hcl5481.grid.linkedin.com/172.20.138.228:42492\n");
    bw.write("28-08-2015 12:30:37 PDT Training_clickSelectFeatures INFO - INFO Closing idle connection Socket[addr=eat1-nertznn01.grid.linkedin.com/172.20.158.57,port=9000,localport=30453] to server eat1-nertznn01.grid.linkedin.com/172.20.158.57:9000\n");
    bw.write("28-08-2015 12:31:09 PDT Training_clickSelectFeatures INFO - INFO Job job_1440264346270_3044 running in uber mode : false\n");
    bw.write("28-08-2015 12:29:38 PDT Training_clickSelectFeatures INFO - INFO Submitted application application_1440264346270_3088\n");
    bw.write("28-08-2015 12:29:38 PDT Training_clickSelectFeatures INFO - INFO The url to track the job: http://eat1-nertzwp02.grid.linkedin.com:8080/proxy/application_1440264346270_3088/\n");
    bw.write("28-08-2015 12:29:38 PDT Training_clickSelectFeatures INFO - INFO See http://eat1-nertzwp02.grid.linkedin.com:8080/proxy/application_1440264346270_3088/ for details.\n");
    bw.write("28-08-2015 12:29:38 PDT Training_clickSelectFeatures INFO - INFO Running job: job_1440264346270_3088\n");
    bw.write("28-08-2015 12:30:21 PDT Training_clickSelectFeatures INFO - INFO Closing idle connection Socket[addr=eat1-hcl5481.grid.linkedin.com/172.20.138.228,port=42492,localport=42382] to server eat1-hcl5481.grid.linkedin.com/172.20.138.228:42492\n");
    bw.write("28-08-2015 12:30:37 PDT Training_clickSelectFeatures INFO - INFO Closing idle connection Socket[addr=eat1-nertznn01.grid.linkedin.com/172.20.158.57,port=9000,localport=30453] to server eat1-nertznn01.grid.linkedin.com/172.20.158.57:9000\n");
    bw.write("28-08-2015 12:31:09 PDT Training_clickSelectFeatures INFO - INFO Job job_1440264346270_3088 running in uber mode : false\n");
    bw.close();

    Set<String> appId = HadoopJobUtils.findApplicationIdFromLog(tempFile.toString(), logger);

    Assert.assertEquals(2, appId.size());
    Assert.assertTrue(appId.contains("application_1440264346270_3044"));
    Assert.assertTrue(appId.contains("application_1440264346270_3088"));
  }

}
