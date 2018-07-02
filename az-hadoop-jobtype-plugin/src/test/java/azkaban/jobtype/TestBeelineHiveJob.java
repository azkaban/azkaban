package azkaban.jobtype;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import azkaban.AzkabanCommonModule;
import azkaban.utils.Props;
import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestBeelineHiveJob {

  private static final String WORKING_DIR =
      Files.createTempDir().getAbsolutePath() + "/TestBeelineHiveJob";
  Logger testLogger;

  @Before
  public void setUp() throws Exception {
    this.testLogger = Logger.getLogger("testLogger");
    final File workingDirFile = new File(WORKING_DIR);
    workingDirFile.mkdirs();
  }

  @Test
  public void testMainArguments() throws Exception {

    final Props sysProps = new Props();
    sysProps.put("database.type", "h2");
    sysProps.put("h2.path", "./h2");

    SERVICE_PROVIDER.unsetInjector();
        /* Initialize Guice Injector */
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(sysProps)
    );
    SERVICE_PROVIDER.setInjector(injector);

    final String jobId = "test_job";
    final Props jobProps = new Props();
    jobProps.put("type", "beelinehive");
    jobProps.put("hive.script", "hivescript.sql");
    jobProps.put("hive.url", "localhost");

    final BeelineHiveJob job = new BeelineHiveJob(jobId, sysProps, jobProps, this.testLogger);
    final String jobArguments = job.getMainArguments();
    Assert
        .assertTrue(jobArguments.endsWith("-d org.apache.hive.jdbc.HiveDriver -f hivescript.sql "));
    Assert.assertTrue(jobArguments.startsWith("-u localhost"));
    Assert.assertFalse(jobArguments.contains(" -a "));
    System.out.println(job.getMainArguments());

  }


}

