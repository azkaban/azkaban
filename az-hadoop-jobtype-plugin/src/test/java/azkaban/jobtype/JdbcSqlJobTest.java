package azkaban.jobtype;

import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.AllJobExecutorTests;
import azkaban.jobExecutor.ProcessJob;
import azkaban.utils.Props;
import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JdbcSqlJobTest {

  private JdbcSqlJob job;
  private final Logger log = Logger.getLogger(ProcessJob.class);
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private Props props = AllJobExecutorTests.setUpCommonProps();
  private Props sysProps = new Props();
  private DB db;

  @Before
  public void setUp() throws IOException, ManagedProcessException {
    final File workingDir = this.temp.newFolder("TestProcess");

    db = DB.newEmbeddedDB(33071);
    db.start();
    // Initialize job
    this.props.put(AbstractProcessJob.WORKING_DIR, workingDir.getCanonicalPath());
    this.props.put("type", "jdbcSql");

    this.props.put("jdbcSql.file", "src/test/resources/plugins/jobtypes/jdbcSql/testSQL.sql");
    this.props.put("jdbcSql.file.1", "src/test/resources/plugins/jobtypes/jdbcSql/testSQL_1.sql");
    this.props.put("jdbcSql.file.2", "src/test/resources/plugins/jobtypes/jdbcSql/testSQL_2.sql");
    this.props.put("jdbcSql.preexecution_file",
        "src/test/resources/plugins/jobtypes/jdbcSql/testpreSQL.sql");
    this.props.put("jdbcSql.postexecution_file",
        "src/test/resources/plugins/jobtypes/jdbcSql/testpostSQL.sql");
    // clean derby db if exists
    this.sysProps.put("jdbcSql.myxyzDB.connectionurl", "jdbc:mysql://localhost:33071/test");
    this.sysProps.put("jdbcSql.myxyzDB.username", "root");
    this.sysProps.put("jdbcSql.myxyzDB.password", "");
    this.props.put("jdbcSql.database", "myxyzDB");
    this.props.put("jdbcSql.sqlparam.schema_name", "TEST_SCHEMA");
    this.props.put("jdbcSql.sqlparam.table_name", "PERSONS");
    // log sql statements
    this.props.put("jdbcSql.logsql", "true");
  }

  @After
  public void tearDown() throws Exception {
    db.stop();
  }

  @Test(expected = AssertionError.class)
  public void testCheckJobParameters() {
    this.props.removeLocal("jdbcSql.file");
    this.job = new JdbcSqlJob("TestProcess", this.sysProps, this.props, this.log);
  }

  @Test(expected = AssertionError.class)
  public void testCheckJobParameters2() {
    this.sysProps.removeLocal("jdbcSql.myxyzDB.password");
    this.job = new JdbcSqlJob("TestProcess", this.sysProps, this.props, this.log);
  }

  @Test
  public void testrun() throws Exception {
    this.job = new JdbcSqlJob("TestProcess", this.sysProps, this.props, this.log);
    this.job.run();
  }
}