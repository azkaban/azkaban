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

package azkaban.jobExecutor;

import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import azkaban.utils.Props;

public class PythonJobTest {
  private PythonJob job = null;
  // private JobDescriptor descriptor = null;
  private Props props = null;
  private Logger log = Logger.getLogger(PythonJob.class);

  private static final String scriptContent =
      "#!/usr/bin/python  \n" +
          "import re, string, sys  \n" +
          "# if no arguments were given, print a helpful message \n" +
          "l=len(sys.argv) \n" +
          "if l < 1: \n"+
              "\tprint 'Usage: celsium --t temp' \n" +
              "\tsys.exit(1) \n" +
          "\n" +
          "# Loop over the arguments \n" +
          "i=1 \n" +
          "while i < l-1 : \n" +
              "\tname = sys.argv[i] \n" +
              "\tvalue = sys.argv[i+1] \n" +
              "\tif name == \"--t\": \n" +
              "\t\ttry: \n" +
                      "\t\t\tfahrenheit = float(string.atoi(value)) \n" +
              "\t\texcept string.atoi_error: \n" +
                     "\t\t\tprint repr(value), \" not a numeric value\" \n" +
              "\t\telse: \n" +
                      "\t\t\tcelsius=(fahrenheit-32)*5.0/9.0 \n" +
                      "\t\t\tprint '%i F = %iC' % (int(fahrenheit), int(celsius+.5)) \n" +
                      "\t\t\tsys.exit(0) \n" +
              "\t\ti=i+2\n";

  private static String scriptFile;

  @BeforeClass
  public static void init() {

    long time = (new Date()).getTime();
    scriptFile = "/tmp/azkaban_python" + time + ".py";
    // dump script file
    try {
      Utils.dumpFile(scriptFile, scriptContent);
    } catch (IOException e) {
      e.printStackTrace(System.err);
      Assert.fail("error in creating script file:" + e.getLocalizedMessage());
    }

  }

  @AfterClass
  public static void cleanup() {
    // remove the input file and error input file
    Utils.removeFile(scriptFile);
  }

  @Ignore("Test appears to hang.")
  @Test
  public void testPythonJob() {

    /* initialize job */
    // descriptor = EasyMock.createMock(JobDescriptor.class);

    props = new Props();
    props.put(AbstractProcessJob.WORKING_DIR, ".");
    props.put("type", "python");
    props.put("script", scriptFile);
    props.put("t", "90");
    props.put("type", "script");
    props.put("fullPath", ".");

    // EasyMock.expect(descriptor.getId()).andReturn("script").times(1);
    // EasyMock.expect(descriptor.getProps()).andReturn(props).times(3);
    // EasyMock.expect(descriptor.getFullPath()).andReturn(".").times(1);
    // EasyMock.replay(descriptor);
    job = new PythonJob("TestProcess", props, props, log);
    // EasyMock.verify(descriptor);
    try {
      job.run();
    } catch (Exception e) {
      e.printStackTrace(System.err);
      Assert.fail("Python job failed:" + e.getLocalizedMessage());
    }
  }

}
