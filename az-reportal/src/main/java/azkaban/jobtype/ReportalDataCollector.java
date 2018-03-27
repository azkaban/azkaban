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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import azkaban.flow.CommonJobProperties;
import azkaban.reportal.util.CompositeException;
import azkaban.reportal.util.IStreamProvider;
import azkaban.reportal.util.ReportalUtil;
import azkaban.utils.Props;

public class ReportalDataCollector extends ReportalAbstractRunner {

  Props prop;

  public ReportalDataCollector(String jobName, Properties props) {
    super(props);
    prop = new Props();
    prop.put(props);
  }

  @Override
  protected void runReportal() throws Exception {
    System.out.println("Reportal Data Collector: Initializing");

    String outputFileSystem =
        props.getString("reportal.output.filesystem", "local");
    String outputBase = props.getString("reportal.output.dir", "/tmp/reportal");
    String execId = props.getString(CommonJobProperties.EXEC_ID);

    int jobNumber = prop.getInt("reportal.job.number");
    List<Exception> exceptions = new ArrayList<Exception>();
    for (int i = 0; i < jobNumber; i++) {
      InputStream tempStream = null;
      IStreamProvider outputProvider = null;
      OutputStream persistentStream = null;
      try {
        String jobTitle = prop.getString("reportal.job." + i);
        System.out.println("Reportal Data Collector: Job name=" + jobTitle);

        String tempFileName = jobTitle + ".csv";
        // We add the job index to the beginning of the job title to allow us to
        // sort the files correctly.
        String persistentFileName = i + "-" + tempFileName;

        String subPath = "/" + execId + "/" + persistentFileName;
        String locationFull = (outputBase + subPath).replace("//", "/");
        String locationTemp = ("./reportal/" + tempFileName).replace("//", "/");
        File tempOutput = new File(locationTemp);
        if (!tempOutput.exists()) {
          throw new FileNotFoundException("File: "
              + tempOutput.getAbsolutePath() + " does not exist.");
        }

        // Copy file to persistent saving location
        System.out
            .println("Reportal Data Collector: Saving output to persistent storage");
        System.out.println("Reportal Data Collector: FS=" + outputFileSystem
            + ", Location=" + locationFull);
        // Open temp file
        tempStream = new BufferedInputStream(new FileInputStream(tempOutput));
        // Open file from HDFS if specified
        outputProvider = ReportalUtil.getStreamProvider(outputFileSystem);
        persistentStream = outputProvider.getFileOutputStream(locationFull);
        // Copy it
        IOUtils.copy(tempStream, persistentStream);

      } catch (Exception e) {
        System.out.println("Reportal Data Collector: Data collection failed. "
            + e.getMessage());
        e.printStackTrace();
        exceptions.add(e);
      } finally {
        IOUtils.closeQuietly(tempStream);
        IOUtils.closeQuietly(persistentStream);

        try {
          outputProvider.cleanUp();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    if (exceptions.size() > 0) {
      throw new CompositeException(exceptions);
    }

    System.out.println("Reportal Data Collector: Ended successfully");
  }

  @Override
  protected boolean requiresOutput() {
    return false;
  }
}
