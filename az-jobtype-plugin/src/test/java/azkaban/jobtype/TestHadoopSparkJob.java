/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobtype;

import azkaban.utils.Props;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestHadoopSparkJob {
  Logger logger = Logger.getRootLogger();

  private static final String SPARK_BASE_DIR = Files.createTempDir().getAbsolutePath() + "/TestHadoopSpark";
  private static final String SPARK_163_HOME = "spark-bin-x_1630";
  private static final String SPARK_210_HOME = "spark-bin-x_2105";
  private static final String SPARK_DEFAULT = "spark_default";
  private static final String SPARK_HOME_CONF = "conf";
  private static final String SPARK_DEFAULT_FILE_NAME = "spark-defaults.conf";
  private static final String SPARK_HOME_LIB = "lib";

  @Before
  public void beforeMethod()
      throws IOException {
    File workingDirFile = new File(SPARK_BASE_DIR);

    if (workingDirFile.exists()) {
      FileUtils.deleteDirectory(workingDirFile);
    }
    workingDirFile.mkdirs();
    File sparkDir = new File(workingDirFile, SPARK_163_HOME);
    sparkDir.mkdir();
    File sparkVerLibDir = new File(sparkDir, SPARK_HOME_LIB);
    sparkVerLibDir.mkdir();
    File sparkVerConfDir = new File(sparkDir, SPARK_HOME_CONF);
    sparkVerConfDir.mkdir();
    File sparkVerDefaultFile = new File(sparkVerConfDir, SPARK_DEFAULT_FILE_NAME);
    sparkVerDefaultFile.createNewFile();

    sparkDir = new File(workingDirFile, SPARK_210_HOME);
    sparkDir.mkdir();
    sparkVerLibDir = new File(sparkDir, SPARK_HOME_LIB);
    sparkVerLibDir.mkdir();
    sparkVerConfDir = new File(sparkDir, SPARK_HOME_CONF);
    sparkVerConfDir.mkdir();
    sparkVerDefaultFile = new File(sparkVerConfDir, SPARK_DEFAULT_FILE_NAME);
    sparkVerDefaultFile.createNewFile();

    File sparkDefaultDir = new File(workingDirFile, SPARK_DEFAULT);
    sparkDefaultDir.mkdir();
    File sparkDefaultLibDir = new File(sparkDefaultDir, SPARK_HOME_LIB);
    sparkDefaultLibDir.mkdir();
    File sparkDefaultConfDir = new File(sparkDefaultDir, SPARK_HOME_CONF);
    sparkDefaultConfDir.mkdir();
    File sparkDefaultFile = new File(sparkDefaultConfDir, SPARK_DEFAULT_FILE_NAME);
    sparkDefaultFile.createNewFile();
  }

  @Test
  public void testSparkLibConf() {
    // This method is testing whether correct spark home is selected when spark.{xyz_version}.home is specified and
    // spark-version is set to xyz_version.
    Props jobProps = new Props();
    jobProps.put(SparkJobArg.SPARK_VERSION.azPropName, "1.6.3");

    Props sysProps = new Props();
    sysProps.put("spark.home", SPARK_BASE_DIR + "/" + SPARK_DEFAULT);
    sysProps.put("spark.1.6.3.home", SPARK_BASE_DIR + "/" + SPARK_163_HOME);

    HadoopSparkJob sparkJob = new HadoopSparkJob("azkaban_job_1", sysProps, jobProps, logger);
    String[] sparkHomeConfPath = sparkJob.getSparkLibConf();
    Assert.assertTrue(sparkHomeConfPath.length == 2);
    String sparkLibPath = SPARK_BASE_DIR + "/" + SPARK_163_HOME + "/" + SPARK_HOME_LIB;
    Assert.assertEquals(sparkHomeConfPath[0], sparkLibPath);
    String sparkConfPath = SPARK_BASE_DIR + "/" + SPARK_163_HOME + "/" + SPARK_HOME_CONF;
    Assert.assertEquals(sparkHomeConfPath[1], sparkConfPath);
  }

  @Test
  public void testSparkDefaultLibConf() {
    // This method is testing whether correct spark home is selected when spark-version is not set in job. In this case,
    // default spark home will be picked up.
    Props jobProps = new Props();

    Props sysProps = new Props();
    sysProps.put("spark.home", SPARK_BASE_DIR + "/" + SPARK_DEFAULT);
    sysProps.put("spark.1.6.3.home", SPARK_BASE_DIR + "/" + SPARK_163_HOME);

    HadoopSparkJob sparkJob = new HadoopSparkJob("azkaban_job_1", sysProps, jobProps, logger);
    String[] sparkHomeConfPath = sparkJob.getSparkLibConf();
    Assert.assertTrue(sparkHomeConfPath.length == 2);
    String sparkLibPath = SPARK_BASE_DIR + "/" + SPARK_DEFAULT + "/" + SPARK_HOME_LIB;
    Assert.assertEquals(sparkHomeConfPath[0], sparkLibPath);
    String sparkConfPath = SPARK_BASE_DIR + "/" + SPARK_DEFAULT + "/" + SPARK_HOME_CONF;
    Assert.assertEquals(sparkHomeConfPath[1], sparkConfPath);
  }

  @Test
  public void testSparkPatternLibConf() {
    // This method is testing whether correct spark home is selected when spark.base.dir, spark.home.prefix,
    // spark.version.regex.to.replace and spark.version.regex.to.replace.with are provided and spark-version is passed
    // for which spark.{version}.home doesn't exist.
    Props jobProps = new Props();
    jobProps.put(SparkJobArg.SPARK_VERSION.azPropName, "2.1.0");

    Props sysProps = new Props();
    sysProps.put("spark.home", SPARK_BASE_DIR + "/" + SPARK_DEFAULT);
    sysProps.put("spark.1.6.3.home", SPARK_BASE_DIR + "/" + SPARK_163_HOME);
    sysProps.put(HadoopSparkJob.SPARK_BASE_DIR, SPARK_BASE_DIR);
    sysProps.put(HadoopSparkJob.SPARK_HOME_PREFIX, "spark-bin-x_");
    sysProps.put(HadoopSparkJob.SPARK_VERSION_REGEX_TO_REPLACE, ".");
    sysProps.put(HadoopSparkJob.SPARK_VERSION_REGEX_TO_REPLACE_WITH, "");
    sysProps.put(HadoopSparkJob.SPARK_REFERENCE_DOCUMENT, "http://spark.apache.org/documentation.html");

    HadoopSparkJob sparkJob = new HadoopSparkJob("azkaban_job_1", sysProps, jobProps, logger);
    String[] sparkHomeConfPath = sparkJob.getSparkLibConf();
    Assert.assertTrue(sparkHomeConfPath.length == 2);
    String sparkLibPath = SPARK_BASE_DIR + "/" + SPARK_210_HOME + "/" + SPARK_HOME_LIB;
    Assert.assertEquals(sparkHomeConfPath[0], sparkLibPath);
    String sparkConfPath = SPARK_BASE_DIR + "/" + SPARK_210_HOME + "/" + SPARK_HOME_CONF;
    Assert.assertEquals(sparkHomeConfPath[1], sparkConfPath);
  }

  @Test
  public void testSparkLoadsAdditionalNamenodes() throws Exception {
    File source = new File("test_resource/additional-namenodes-spark-defaults.conf");
    File target = new File(SPARK_BASE_DIR + "/" + SPARK_163_HOME + "/" +
        SPARK_HOME_CONF + "/" + SPARK_DEFAULT_FILE_NAME);
    Files.copy(source, target);

    Props jobProps = new Props();
    jobProps.put(SparkJobArg.SPARK_VERSION.azPropName, "1.6.3");

    Props sysProps = new Props();
    sysProps.put("spark.home", SPARK_BASE_DIR + "/" + SPARK_DEFAULT);
    sysProps.put("spark.1.6.3.home", SPARK_BASE_DIR + "/" + SPARK_163_HOME);

    HadoopSparkJob sparkJob = new HadoopSparkJob("azkaban_job_1", sysProps, jobProps, logger);

    Props testProps = new Props();
    sparkJob.addAdditionalNamenodesFromConf(testProps);
    Assert.assertEquals("hdfs://testNN:9000", testProps.get("other_namenodes"));

    testProps = new Props();
    testProps.put("other_namenodes", "hdfs://testNN1:9000,hdfs://testNN2:9000");
    sparkJob.addAdditionalNamenodesFromConf(testProps);
    Assert.assertEquals("hdfs://testNN1:9000,hdfs://testNN2:9000,hdfs://testNN:9000",
        testProps.get("other_namenodes"));
  }
}
