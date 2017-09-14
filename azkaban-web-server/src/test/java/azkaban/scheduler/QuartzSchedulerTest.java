package azkaban.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.db.DatabaseOperator;
import azkaban.test.Utils;
import azkaban.utils.Props;
import java.io.File;
import java.sql.SQLException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class QuartzSchedulerTest {

  private static DatabaseOperator dbOperator;
  private static QuartzScheduler scheduler;

  @BeforeClass
  public static void setUpQUartz() throws Exception {
    dbOperator = Utils.initQuartzDB();
    final String quartzPropsPath=
        new File("../azkaban-web-server/src/test/resources/quartz.test.properties")
        .getCanonicalPath();
    final Props quartzProps = new Props(null, quartzPropsPath);
    scheduler = new QuartzScheduler(quartzProps);
    scheduler.start();
  }

  @AfterClass
  public static void destroyQuartz() {
    try {
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @After
  public void cleanup() {
    scheduler.cleanup();
  }

  @Test
  public void test1(){
    final String aString = "lll";
    assertThat(aString).isNotNull();
  }
}