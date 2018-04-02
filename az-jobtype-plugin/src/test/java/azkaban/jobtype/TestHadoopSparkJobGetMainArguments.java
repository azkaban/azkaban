package azkaban.jobtype;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.utils.Props;

public class TestHadoopSparkJobGetMainArguments {
  Props jobProps = null;

  Logger logger = Logger.getRootLogger();

  String workingDirString = "/tmp/TestHadoopSpark";

  File workingDirFile = new File(workingDirString);

  File libFolderFile = new File(workingDirFile, "lib");

  String executionJarName = "hadoop-spark-job-test-execution-x.y.z-a.b.c.jar";

  File executionJarFile = new File(libFolderFile, "hadoop-spark-job-test-execution-x.y.z-a.b.c.jar");

  File libraryJarFile = new File(libFolderFile, "library.jar");

  String delim = SparkJobArg.delimiter;

  @Before
  public void beforeMethod() throws IOException {
    if (workingDirFile.exists())
      FileUtils.deleteDirectory(workingDirFile);
    workingDirFile.mkdirs();
    libFolderFile.mkdirs();
    executionJarFile.createNewFile();
    libraryJarFile.createNewFile();

    jobProps = new Props();
    jobProps.put("azkaban.link.workflow.url", "http://azkaban.link.workflow.url");
    jobProps.put("azkaban.link.job.url", "http://azkaban.link.job.url");
    jobProps.put("azkaban.link.execution.url", "http://azkaban.link.execution.url");
    jobProps.put("azkaban.link.jobexec.url", "http://azkaban.link.jobexec.url");
    jobProps.put("azkaban.link.attempt.url", "http://azkaban.link.attempt.url");
    jobProps.put(SparkJobArg.CLASS.azPropName, "hadoop.spark.job.test.ExecutionClass");
    jobProps.put(SparkJobArg.EXECUTION_JAR.azPropName, "./lib/hadoop-spark-job-test-execution.jar");

  }

  @Test
  public void testDefault() {
    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    // the first one, so no delimiter at front
    // due to new communication mechanism between HAdoopSparkJob and HadoopSparkSecureWrapper,
    // these Azkaban variables are sent through the configuration file and not through the command line
    Assert.assertTrue(retval.contains(SparkJobArg.DRIVER_JAVA_OPTIONS.sparkParamName + delim + "" +  delim));
    Assert.assertTrue(retval.contains(delim + SparkJobArg.CLASS.sparkParamName + delim
            + "hadoop.spark.job.test.ExecutionClass" + delim));
      // last one, no delimiter at back
    Assert.assertTrue(retval.contains(delim
            + "/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"));

    // test flag values such as verbose do not come in by default
    Assert.assertFalse(retval.contains("--verbose"));
    Assert.assertFalse(retval.contains("--help"));
    Assert.assertFalse(retval.contains("--version"));
  }

  @Test
  public void testDefaultWithExecutionJarSpecification2() {
    jobProps.put(SparkJobArg.EXECUTION_JAR.azPropName, "./lib/hadoop-spark-job-test-execution");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim
            + "/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"));
  }

  @Test
  public void testNoClass() {
    jobProps.removeLocal(SparkJobArg.CLASS.azPropName);

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertFalse(retval.contains(delim + SparkJobArg.CLASS.sparkParamName + delim));
  }

  @Test
  public void testChangeMaster() {
    jobProps.put(SparkJobArg.MASTER.azPropName, "NEW_SPARK_MASTER");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.MASTER.sparkParamName + delim
            + "NEW_SPARK_MASTER" + delim));
    Assert.assertFalse(retval.contains(delim + SparkJobArg.MASTER.sparkParamName + delim
            + "yarn-cluster" + delim));

  }

  @Test
  public void testDeployMode() {
    jobProps.put(SparkJobArg.DEPLOY_MODE.azPropName, "NEW_DEPLOY_MODE");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.DEPLOY_MODE.sparkParamName + delim
            + "NEW_DEPLOY_MODE" + delim));
  }

  @Test
  public void testExecutionClass() throws IOException {

    jobProps.put(SparkJobArg.CLASS.azPropName, "new.ExecutionClass");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.CLASS.sparkParamName + delim
            + "new.ExecutionClass" + delim));
    Assert.assertFalse(retval.contains(delim + SparkJobArg.CLASS.sparkParamName + delim
            + "hadoop.spark.job.test.ExecutionClass" + delim));

  }

  @Test
  public void testName() throws IOException {

    jobProps.put(SparkJobArg.NAME.azPropName, "NEW_NAME");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.NAME.sparkParamName + delim + "NEW_NAME"
            + delim));
  }

  @Test
  public void testChangeSparkJar() throws IOException {
    String topLevelJarString = "topLevelJar.jar";
    File toplevelJarFile = new File(workingDirFile, topLevelJarString);
    toplevelJarFile.createNewFile();
    jobProps.put(SparkJobArg.SPARK_JARS.azPropName, "./*");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.SPARK_JARS.sparkParamName + delim
            + "/tmp/TestHadoopSpark/./" + topLevelJarString + delim));
    Assert.assertFalse(retval
            .contains(delim
                    + SparkJobArg.SPARK_JARS.sparkParamName
                    + delim
                    + "/tmp/TestHadoopSpark/./lib/library.jar,/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"
                    + delim));

  }

  @Test
  public void testPackages() throws IOException {

    jobProps.put(SparkJobArg.PACKAGES.azPropName, "a:b:c,d:e:f");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.PACKAGES.sparkParamName + delim
            + "a:b:c,d:e:f" + delim));
  }

  @Test
  public void testRepositories() throws IOException {

    jobProps.put(SparkJobArg.REPOSITORIES.azPropName, "repo1,repo2");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.REPOSITORIES.sparkParamName + delim
            + "repo1,repo2" + delim));
  }

  @Test
  public void testPyFiles() {
    jobProps.put(SparkJobArg.PY_FILES.azPropName, "file1.py,file2.egg,file3.zip");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.PY_FILES.sparkParamName + delim
            + "file1.py,file2.egg,file3.zip" + delim));
  }

  @Test
  public void testFiles() {
    jobProps.put(SparkJobArg.FILES.azPropName, "file1.py,file2.egg,file3.zip");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.FILES.sparkParamName + delim
            + "file1.py,file2.egg,file3.zip" + delim));
  }

  @Test
  public void testSparkConf() throws IOException {

    jobProps.put(SparkJobArg.SPARK_CONF_PREFIX.azPropName + "conf1", "confValue1");
    jobProps.put(SparkJobArg.SPARK_CONF_PREFIX.azPropName + "conf2", "confValue2");
    jobProps.put(SparkJobArg.SPARK_CONF_PREFIX.azPropName + "conf3", "confValue3");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    for (int i = 1; i <= 3; i++) {
      String confAnswer = String.format(delim + "%s" + delim + "%s%d=%s%d" + delim,
              SparkJobArg.SPARK_CONF_PREFIX.sparkParamName, "conf", i, "confValue", i);
      System.out.println("looking for: " + confAnswer);
      Assert.assertTrue(retval.contains(confAnswer));
    }

  }

  @Test
  public void testPropertiesFile() {
    jobProps.put(SparkJobArg.PROPERTIES_FILE.azPropName, "NEW_PROPERTIES_FILE");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.PROPERTIES_FILE.sparkParamName + delim
            + "NEW_PROPERTIES_FILE" + delim));
  }

  @Test
  public void testDriverMemory() throws IOException {

    jobProps.put(SparkJobArg.DRIVER_MEMORY.azPropName, "1t");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.DRIVER_MEMORY.sparkParamName + delim
            + "1t" + delim));
    Assert.assertFalse(retval.contains(delim + SparkJobArg.DRIVER_MEMORY.sparkParamName + delim
            + "2g" + delim));

  }

  /*
   * Note that for this test, there are already default stuff in --driver-java-options. So we have
   * to test to make sure the user specified ones are properly included/appended
   */
  @Test
  public void testDriverJavaOptions() {
    jobProps.put(SparkJobArg.DRIVER_JAVA_OPTIONS.azPropName, "-Dabc=def -Dfgh=ijk");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    // only on the ending side has the delimiter
    Assert.assertTrue(retval.contains(" -Dabc=def -Dfgh=ijk" + delim));
  }

  @Test
  public void testDriverLibraryPath() {
    String libraryPathSpec = "/this/is/library/path:/this/is/library/path/too";
    jobProps.put(SparkJobArg.DRIVER_LIBRARY_PATH.azPropName, libraryPathSpec);

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.DRIVER_LIBRARY_PATH.sparkParamName
            + delim + libraryPathSpec + delim));
  }

  @Test
  public void testDriverClassPath() {
    String classPathSpec = "/this/is/class/path:/this/is/class/path/too";
    jobProps.put(SparkJobArg.DRIVER_CLASS_PATH.azPropName, classPathSpec);

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.DRIVER_CLASS_PATH.sparkParamName + delim
            + classPathSpec + delim));
  }

  @Test
  public void testExecutorMemory() throws IOException {

    jobProps.put(SparkJobArg.EXECUTOR_MEMORY.azPropName, "1t");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.EXECUTOR_MEMORY.sparkParamName + delim
            + "1t" + delim));
    Assert.assertFalse(retval.contains(delim + SparkJobArg.EXECUTOR_MEMORY.sparkParamName + delim
            + "1g" + delim));
  }

  @Test
  public void testProxyUser() throws IOException {

    jobProps.put(SparkJobArg.PROXY_USER.azPropName, "NEW_PROXY_USER");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.PROXY_USER.sparkParamName + delim
            + "NEW_PROXY_USER" + delim));
  }

  @Test
  public void testSparkFlagOn() {
    jobProps.put(SparkJobArg.SPARK_FLAG_PREFIX.azPropName + "verbose", "true");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains("--verbose"));
    Assert.assertFalse(retval.contains("true"));
  }

  @Test
  public void testSparkFlagOffIfValueIsNotTrue() {
    jobProps.put(SparkJobArg.SPARK_FLAG_PREFIX.azPropName + "verbose", "I am a value, and I do not .equals true");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertFalse(retval.contains("--verbose"));
  }

  /*
   * End of general SparkSubmit argument section, Start of Yarn specific SparkSubmit arguments
   */

  @Test
  public void testExecutorCores() throws IOException {

    jobProps.put(SparkJobArg.EXECUTOR_CORES.azPropName, "2000");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.EXECUTOR_CORES.sparkParamName + delim
            + "2000" + delim));
    Assert.assertFalse(retval.contains(delim + SparkJobArg.EXECUTOR_CORES.sparkParamName + delim
            + "1" + delim));

  }

  @Test
  public void testDriverCores() throws IOException {

    jobProps.put(SparkJobArg.DRIVER_CORES.azPropName, "2000");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.DRIVER_CORES.sparkParamName + delim
            + "2000" + delim));
  }

  @Test
  public void testQueue() throws IOException {

    jobProps.put(SparkJobArg.QUEUE.azPropName, "my_own_queue");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.QUEUE.sparkParamName + delim
            + "my_own_queue" + delim));
    Assert.assertFalse(retval.contains(delim + SparkJobArg.QUEUE.sparkParamName + delim
            + "marathon" + delim));

  }

  @Test
  public void testNumExecutors() throws IOException {

    jobProps.put(SparkJobArg.NUM_EXECUTORS.azPropName, "1000");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.NUM_EXECUTORS.sparkParamName + delim
            + "1000" + delim));
    Assert.assertFalse(retval.contains(delim + SparkJobArg.NUM_EXECUTORS.sparkParamName + delim
            + "2" + delim));
  }

  @Test
  public void testArchives() throws IOException {
    String archiveSpec = "archive1,archive2";
    jobProps.put(SparkJobArg.ARCHIVES.azPropName, archiveSpec);

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.ARCHIVES.sparkParamName + delim
            + archiveSpec + delim));
  }

  @Test
  public void testPrincipal() throws IOException {

    jobProps.put(SparkJobArg.PRINCIPAL.azPropName, "NEW_PRINCIPAL");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.PRINCIPAL.sparkParamName + delim
            + "NEW_PRINCIPAL" + delim));
  }

  @Test
  public void testKeytab() throws IOException {

    jobProps.put(SparkJobArg.KEYTAB.azPropName, "NEW_KEYTAB");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + SparkJobArg.KEYTAB.sparkParamName + delim
            + "NEW_KEYTAB" + delim));
  }

  /*
   * End of general SparkSubmit argument section, Start of Yarn specific SparkSubmit arguments
   */

  @Test
  public void testExecutionJar() throws IOException {

    jobProps.put(SparkJobArg.EXECUTION_JAR.azPropName, "./lib/library");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim + "/tmp/TestHadoopSpark/./lib/library.jar" + delim));

  }

  @Test
  public void testParams() throws IOException {

    jobProps.put(SparkJobArg.PARAMS.azPropName, "param1 param2 param3 param4");

    String retval = HadoopSparkJob.testableGetMainArguments(jobProps, workingDirString, logger);

    Assert.assertTrue(retval.contains(delim
            + "/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar" + delim
            + "param1" + delim + "param2" + delim + "param3" + delim + "param4"));

  }

}
