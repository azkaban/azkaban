package azkaban.jobtype;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;
import com.google.common.io.Files;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;

/**
 *
 */
@SuppressWarnings("PointlessBooleanExpression")
public class JdbcSqlJob extends AbstractJob {

  private static final String WORKING_DIR = "working.dir";
  private static final String SQL_PARAM_JOB_NAME = "jdbcSql.sqlparam.jobname";
  private static final String SQL_DATABASE = "jdbcSql.database";
  private static final String SQL_FILE = "jdbcSql.file";
  private static final String SQL_PRE_EXECUTION_FILE = "jdbcSql.preexecution_file";
  private static final String SQL_POST_EXECUTION_FILE = "jdbcSql.postexecution_file";

  private final Props jobProps;
  private final Props sysProps;
  private final String sqlDatabase;
  private Connection connection;
  private BasicDataSource dataSource;

  public JdbcSqlJob(final String jobId, final Props sysProps, final Props jobProps,
      final Logger log) {
    super(jobId, log);

    this.jobProps = jobProps;
    this.sysProps = sysProps;

    sqlDatabase = jobProps.getString(SQL_DATABASE, "default");
    checkJobParameters();

    jobProps.put(SQL_PARAM_JOB_NAME, this.getId());
    info(this.getId());
  }

  /**
   *
   */
  @Override
  public void run() throws Exception {

    String preSQL = getSqlFromFile(SQL_PRE_EXECUTION_FILE);
    String postSQL = getSqlFromFile(SQL_POST_EXECUTION_FILE);
    List<String> sqlList = getSqlListFromFile();

    dataSource = getDataSource();
    if (connection == null) {

      info("Connecting to '" + sqlDatabase + "' database!");
      connection = dataSource.getConnection();
      info("Successfully connected to '" + sqlDatabase + "' database!");
    }

    if (preSQL != null) {
      info("Running preSQL statement...");
      executeSql(preSQL);
    }

    info("Running SQL statements...");
    for (String sql : sqlList) {
      info(sql);
      executeSql(sql);
    }

    if (postSQL != null) {
      info("Running postSQL statement...");
      executeSql(postSQL);
    }

    if (connection.getAutoCommit() == false) {
      connection.commit();
    }
  }

  /**
   *
   */
  private void executeSql(String sql) throws SQLException {

    if (jobProps.getBoolean("jdbcSql.logsql", false)) {
      info(sql);
    }

    try {
      Statement stmt = connection.createStatement();
      stmt.execute(sql);
    } catch (Exception e) {
      if (jobProps.getBoolean("jdbcSql.logsql", false)) {
        info(sql);
      }
      if (connection.getAutoCommit() == false) {
        connection.rollback();
      }
      throw e;
    }
  }

  /**
   *
   */
  private void checkJobParameters() {

    assert
        jobProps.containsKey(SQL_FILE) :
        "please set " + SQL_FILE + " parameter in job properties";
    // assert jobProps.containsKey(SQL_DATABASE) != false : "please set
    // "+SQL_DATABASE+" parameter in job properties";

    assert
        sysProps.containsKey("jdbcSql." + sqlDatabase + ".connectionurl") :
        "please set " + "jdbcSql."
            + sqlDatabase + ".connectionurl" + " parameter in plugin private.properties";
    assert
        sysProps.containsKey("jdbcSql." + sqlDatabase + ".username") :
        "please set " + "jdbcSql."
            + sqlDatabase + ".username" + " parameter in plugin private.properties";
    assert
        sysProps.containsKey("jdbcSql." + sqlDatabase + ".password") :
        "please set " + "jdbcSql."
            + sqlDatabase + ".password" + " parameter in plugin private.properties";
  }

  private String replaceSQLParams(String sql) {
    for (final String key : this.jobProps.getKeySet()) {
      if (key.contains("jdbcSql.sqlparam.")) {
        final String sqlParamKey = key.replace("jdbcSql.sqlparam.", "");
        sql = sql.replace("${" + sqlParamKey + "}", this.jobProps.getString(key));
      }
    }

    return sql;
  }


  private List<String> getSqlListFromFile() throws Exception {
    final List<String> commands = new ArrayList<>();
    commands.add(this.getSqlFromFile(SQL_FILE));
    for (int i = 1; this.jobProps.containsKey(SQL_FILE + "." + i); i++) {
      commands.add(this.getSqlFromFile(SQL_FILE + "." + i));
    }

    return commands;
  }

  /**
   *
   */
  private String getSqlFromFile(String sqlFileJobPropsName) throws Exception {

    if (!jobProps.containsKey(sqlFileJobPropsName)) {
      return null;
    }

    // assuming sql file given as absolute path
    File sqlFile = new File(jobProps.getString(sqlFileJobPropsName));
    // if not found load it from relative path
    if (!sqlFile.exists()) {
      sqlFile = new File(this.getWorkingDirectory(), jobProps.getString(sqlFileJobPropsName));
    }
    // file not found both at relative and absolute path locations
    if (!sqlFile.exists()) {
      throw new Exception("Could not find SQL file at: " + jobProps.getString(sqlFileJobPropsName)
          + " or at: " + this.getWorkingDirectory() + "/" + jobProps.getString(sqlFileJobPropsName)
          + "! please check Job Parameter " + sqlFileJobPropsName + "!");
    }

    String rawSql = Files.toString(sqlFile, StandardCharsets.UTF_8);
    rawSql = replaceSQLParams(rawSql);
    return rawSql;
  }

  /**
   *
   */
  private BasicDataSource getDataSource() {
    if (dataSource == null) {
      BasicDataSource ds = new BasicDataSource();
      ds.setUrl(sysProps.getString("jdbcSql." + sqlDatabase + ".connectionurl"));
      ds.setUsername(sysProps.getString("jdbcSql." + sqlDatabase + ".username"));
      ds.setPassword(sysProps.getString("jdbcSql." + sqlDatabase + ".password"));
      dataSource = ds;
    }
    return dataSource;
  }

  /**
   *
   */
  @Override
  public void cancel() throws Exception {
    if (connection != null) {
      if (connection.getAutoCommit() == false) {
        connection.rollback();
      }
      connection.close();
    }
    if (dataSource != null) {
      dataSource.close();
    }
    super.cancel();
  }

  private String getWorkingDirectory() {
    return jobProps.getString(WORKING_DIR, "");
  }

}
