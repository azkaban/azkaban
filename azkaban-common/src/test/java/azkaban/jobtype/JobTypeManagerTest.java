/*
 * Copyright 2014 LinkedIn Corp.
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

import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import azkaban.jobExecutor.Job;
import azkaban.utils.Props;

/**
 * Test the flow run, especially with embedded flows. Files are in
 * unit/plugins/jobtypes
 *
 */
public class JobTypeManagerTest {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public final static String TEST_PLUGIN_DIR = "jobtypes_test";

  private Logger logger = Logger.getLogger(JobTypeManagerTest.class);
  private String testPluginDirPath;
  private JobTypeManager manager;

  public JobTypeManagerTest() {
  }

  @Before
  public void setUp() throws Exception {
    File jobTypeDir = temp.newFolder(TEST_PLUGIN_DIR);
    testPluginDirPath = jobTypeDir.getCanonicalPath();

    URL resourceUrl = Resources.getResource("plugins/jobtypes");
    assertNotNull(resourceUrl);
    FileUtils.copyDirectory(new File(resourceUrl.toURI()), jobTypeDir);
    manager = new JobTypeManager(testPluginDirPath, null,
        this.getClass().getClassLoader());
  }

  @After
  public void tearDown() throws IOException {
    temp.delete();
  }

  /**
   * Tests that the common and common private properties are loaded correctly
   *
   * @throws Exception
   */
  @Test
  public void testCommonPluginProps() throws Exception {
    JobTypePluginSet pluginSet = manager.getJobTypePluginSet();

    Props props = pluginSet.getCommonPluginJobProps();
    System.out.println(props.toString());
    assertEquals("commonprop1", props.getString("commonprop1"));
    assertEquals("commonprop2", props.getString("commonprop2"));
    assertEquals("commonprop3", props.getString("commonprop3"));

    Props priv = pluginSet.getCommonPluginLoadProps();
    assertEquals("commonprivate1", priv.getString("commonprivate1"));
    assertEquals("commonprivate2", priv.getString("commonprivate2"));
    assertEquals("commonprivate3", priv.getString("commonprivate3"));
  }

  /**
   * Tests that the proper classes were loaded and that the common and the load
   * properties are properly loaded.
   *
   * @throws Exception
   */
  @Test
  public void testLoadedClasses() throws Exception {
    JobTypePluginSet pluginSet = manager.getJobTypePluginSet();

    Props props = pluginSet.getCommonPluginJobProps();
    System.out.println(props.toString());
    assertEquals("commonprop1", props.getString("commonprop1"));
    assertEquals("commonprop2", props.getString("commonprop2"));
    assertEquals("commonprop3", props.getString("commonprop3"));
    assertNull(props.get("commonprivate1"));

    Props priv = pluginSet.getCommonPluginLoadProps();
    assertEquals("commonprivate1", priv.getString("commonprivate1"));
    assertEquals("commonprivate2", priv.getString("commonprivate2"));
    assertEquals("commonprivate3", priv.getString("commonprivate3"));

    // Testing the anothertestjobtype
    Class<? extends Job> aPluginClass =
        pluginSet.getPluginClass("anothertestjob");
    assertEquals("azkaban.jobtype.FakeJavaJob", aPluginClass.getName());
    Props ajobProps = pluginSet.getPluginJobProps("anothertestjob");
    Props aloadProps = pluginSet.getPluginLoaderProps("anothertestjob");

    // Loader props
    assertEquals("lib/*", aloadProps.get("jobtype.classpath"));
    assertEquals("azkaban.jobtype.FakeJavaJob",
        aloadProps.get("jobtype.class"));
    assertEquals("commonprivate1", aloadProps.get("commonprivate1"));
    assertEquals("commonprivate2", aloadProps.get("commonprivate2"));
    assertEquals("commonprivate3", aloadProps.get("commonprivate3"));
    // Job props
    assertEquals("commonprop1", ajobProps.get("commonprop1"));
    assertEquals("commonprop2", ajobProps.get("commonprop2"));
    assertEquals("commonprop3", ajobProps.get("commonprop3"));
    assertNull(ajobProps.get("commonprivate1"));

    Class<? extends Job> tPluginClass = pluginSet.getPluginClass("testjob");
    assertEquals("azkaban.jobtype.FakeJavaJob2", tPluginClass.getName());
    Props tjobProps = pluginSet.getPluginJobProps("testjob");
    Props tloadProps = pluginSet.getPluginLoaderProps("testjob");

    // Loader props
    assertNull(tloadProps.get("jobtype.classpath"));
    assertEquals("azkaban.jobtype.FakeJavaJob2",
        tloadProps.get("jobtype.class"));
    assertEquals("commonprivate1", tloadProps.get("commonprivate1"));
    assertEquals("commonprivate2", tloadProps.get("commonprivate2"));
    assertEquals("private3", tloadProps.get("commonprivate3"));
    assertEquals("0", tloadProps.get("testprivate"));
    // Job props
    assertEquals("commonprop1", tjobProps.get("commonprop1"));
    assertEquals("commonprop2", tjobProps.get("commonprop2"));
    assertEquals("1", tjobProps.get("pluginprops1"));
    assertEquals("2", tjobProps.get("pluginprops2"));
    assertEquals("3", tjobProps.get("pluginprops3"));
    assertEquals("pluginprops", tjobProps.get("commonprop3"));
    // Testing that the private properties aren't shared with the public ones
    assertNull(tjobProps.get("commonprivate1"));
    assertNull(tjobProps.get("testprivate"));
  }

  /**
   * Test building classes
   *
   * @throws Exception
   */
  @Test
  public void testBuildClass() throws Exception {
    Props jobProps = new Props();
    jobProps.put("type", "anothertestjob");
    jobProps.put("test", "test1");
    jobProps.put("pluginprops3", "4");
    Job job = manager.buildJobExecutor("anothertestjob", jobProps, logger);

    assertTrue(job instanceof FakeJavaJob);
    FakeJavaJob fjj = (FakeJavaJob) job;

    Props props = fjj.getJobProps();
    assertEquals("test1", props.get("test"));
    assertNull(props.get("pluginprops1"));
    assertEquals("4", props.get("pluginprops3"));
    assertEquals("commonprop1", props.get("commonprop1"));
    assertEquals("commonprop2", props.get("commonprop2"));
    assertEquals("commonprop3", props.get("commonprop3"));
    assertNull(props.get("commonprivate1"));
  }

  /**
   * Test building classes 2
   *
   * @throws Exception
   */
  @Test
  public void testBuildClass2() throws Exception {
    Props jobProps = new Props();
    jobProps.put("type", "testjob");
    jobProps.put("test", "test1");
    jobProps.put("pluginprops3", "4");
    Job job = manager.buildJobExecutor("testjob", jobProps, logger);

    assertTrue(job instanceof FakeJavaJob2);
    FakeJavaJob2 fjj = (FakeJavaJob2) job;

    Props props = fjj.getJobProps();
    assertEquals("test1", props.get("test"));
    assertEquals("1", props.get("pluginprops1"));
    assertEquals("2", props.get("pluginprops2"));
    assertEquals("4", props.get("pluginprops3")); // Overridden value
    assertEquals("commonprop1", props.get("commonprop1"));
    assertEquals("commonprop2", props.get("commonprop2"));
    assertEquals("pluginprops", props.get("commonprop3"));
    assertNull(props.get("commonprivate1"));
  }

  /**
   * Test out reloading properties
   *
   * @throws Exception
   */
  @Test
  public void testResetPlugins() throws Exception {
    // Add a plugins file to the anothertestjob folder
    File anothertestfolder = new File(testPluginDirPath + "/anothertestjob");
    Props pluginProps = new Props();
    pluginProps.put("test1", "1");
    pluginProps.put("test2", "2");
    pluginProps.put("pluginprops3", "4");
    pluginProps
        .storeFlattened(new File(anothertestfolder, "plugin.properties"));

    // clone the testjob folder
    File testFolder = new File(testPluginDirPath + "/testjob");
    FileUtils.copyDirectory(testFolder, new File(testPluginDirPath
        + "/newtestjob"));

    // change the common properties
    Props commonPlugin =
        new Props(null, testPluginDirPath + "/common.properties");
    commonPlugin.put("commonprop1", "1");
    commonPlugin.put("newcommonprop1", "2");
    commonPlugin.removeLocal("commonprop2");
    commonPlugin
        .storeFlattened(new File(testPluginDirPath + "/common.properties"));

    // change the common properties
    Props commonPrivate =
        new Props(null, testPluginDirPath + "/commonprivate.properties");
    commonPrivate.put("commonprivate1", "1");
    commonPrivate.put("newcommonprivate1", "2");
    commonPrivate.removeLocal("commonprivate2");
    commonPrivate.storeFlattened(new File(testPluginDirPath
        + "/commonprivate.properties"));

    // change testjob private property
    Props loadProps =
        new Props(null, testPluginDirPath + "/testjob/private.properties");
    loadProps.put("privatetest", "test");

    // Reload the plugins here!!
    manager.loadPlugins();

    // Checkout common props
    JobTypePluginSet pluginSet = manager.getJobTypePluginSet();
    Props commonProps = pluginSet.getCommonPluginJobProps();
    assertEquals("1", commonProps.get("commonprop1"));
    assertEquals("commonprop3", commonProps.get("commonprop3"));
    assertEquals("2", commonProps.get("newcommonprop1"));
    assertNull(commonProps.get("commonprop2"));

    // Checkout common private
    Props commonPrivateProps = pluginSet.getCommonPluginLoadProps();
    assertEquals("1", commonPrivateProps.get("commonprivate1"));
    assertEquals("commonprivate3", commonPrivateProps.get("commonprivate3"));
    assertEquals("2", commonPrivateProps.get("newcommonprivate1"));
    assertNull(commonPrivateProps.get("commonprivate2"));

    // Verify anothertestjob changes
    Class<? extends Job> atjClass = pluginSet.getPluginClass("anothertestjob");
    assertEquals("azkaban.jobtype.FakeJavaJob", atjClass.getName());
    Props ajobProps = pluginSet.getPluginJobProps("anothertestjob");
    assertEquals("1", ajobProps.get("test1"));
    assertEquals("2", ajobProps.get("test2"));
    assertEquals("4", ajobProps.get("pluginprops3"));
    assertEquals("commonprop3", ajobProps.get("commonprop3"));

    Props aloadProps = pluginSet.getPluginLoaderProps("anothertestjob");
    assertEquals("1", aloadProps.get("commonprivate1"));
    assertNull(aloadProps.get("commonprivate2"));
    assertEquals("commonprivate3", aloadProps.get("commonprivate3"));

    // Verify testjob changes
    Class<? extends Job> tjClass = pluginSet.getPluginClass("testjob");
    assertEquals("azkaban.jobtype.FakeJavaJob2", tjClass.getName());
    Props tjobProps = pluginSet.getPluginJobProps("testjob");
    assertEquals("1", tjobProps.get("commonprop1"));
    assertEquals("2", tjobProps.get("newcommonprop1"));
    assertEquals("1", tjobProps.get("pluginprops1"));
    assertEquals("2", tjobProps.get("pluginprops2"));
    assertEquals("3", tjobProps.get("pluginprops3"));
    assertEquals("pluginprops", tjobProps.get("commonprop3"));
    assertNull(tjobProps.get("commonprop2"));

    Props tloadProps = pluginSet.getPluginLoaderProps("testjob");
    assertNull(tloadProps.get("jobtype.classpath"));
    assertEquals("azkaban.jobtype.FakeJavaJob2",
        tloadProps.get("jobtype.class"));
    assertEquals("1", tloadProps.get("commonprivate1"));
    assertNull(tloadProps.get("commonprivate2"));
    assertEquals("private3", tloadProps.get("commonprivate3"));

    // Verify newtestjob
    Class<? extends Job> ntPluginClass = pluginSet.getPluginClass("newtestjob");
    assertEquals("azkaban.jobtype.FakeJavaJob2", ntPluginClass.getName());
    Props ntjobProps = pluginSet.getPluginJobProps("newtestjob");
    Props ntloadProps = pluginSet.getPluginLoaderProps("newtestjob");

    // Loader props
    assertNull(ntloadProps.get("jobtype.classpath"));
    assertEquals("azkaban.jobtype.FakeJavaJob2",
        ntloadProps.get("jobtype.class"));
    assertEquals("1", ntloadProps.get("commonprivate1"));
    assertNull(ntloadProps.get("commonprivate2"));
    assertEquals("private3", ntloadProps.get("commonprivate3"));
    assertEquals("0", ntloadProps.get("testprivate"));
    // Job props
    assertEquals("1", ntjobProps.get("commonprop1"));
    assertNull(ntjobProps.get("commonprop2"));
    assertEquals("1", ntjobProps.get("pluginprops1"));
    assertEquals("2", ntjobProps.get("pluginprops2"));
    assertEquals("3", ntjobProps.get("pluginprops3"));
    assertEquals("pluginprops", ntjobProps.get("commonprop3"));
  }
}
