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

package azkaban.viewer.reportal;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;

import azkaban.reportal.util.Reportal;
import azkaban.user.User;
import azkaban.utils.Props;

public enum ReportalType {

  PigJob("ReportalPig", "reportalpig", "hadoop") {
    @Override
    public void buildJobFiles(Reportal reportal, Props propertiesFile,
        File jobFile, String jobName, String queryScript, String proxyUser) {
      File resFolder = new File(jobFile.getParentFile(), "res");
      resFolder.mkdirs();
      File scriptFile = new File(resFolder, jobName + ".pig");

      OutputStream fileOutput = null;
      try {
        scriptFile.createNewFile();
        fileOutput = new BufferedOutputStream(new FileOutputStream(scriptFile));
        fileOutput.write(queryScript.getBytes(Charset.forName("UTF-8")));
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (fileOutput != null) {
          try {
            fileOutput.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }

      propertiesFile.put("reportal.pig.script", "res/" + jobName + ".pig");
    }
  },
  HiveJob("ReportalHive", "reportalhive", "hadoop"), TeraDataJob(
      "ReportalTeraData", "reportalteradata", "teradata"), DataCollectorJob(
      ReportalTypeManager.DATA_COLLECTOR_JOB,
      ReportalTypeManager.DATA_COLLECTOR_JOB_TYPE, "") {
    @Override
    public void buildJobFiles(Reportal reportal, Props propertiesFile,
        File jobFile, String jobName, String queryScript, String proxyUser) {
      propertiesFile.put("user.to.proxy", proxyUser);
    }
  };

  private final String typeName;
  private final String jobTypeName;
  private final String permissionName;

  private ReportalType(String typeName, String jobTypeName,
      String permissionName) {
    this.typeName = typeName;
    this.jobTypeName = jobTypeName;
    this.permissionName = permissionName;
  }

  public void buildJobFiles(Reportal reportal, Props propertiesFile,
      File jobFile, String jobName, String queryScript, String proxyUser) {

  }

  public String getJobTypeName() {
    return jobTypeName;
  }

  private static HashMap<String, ReportalType> reportalTypes =
      new HashMap<String, ReportalType>();

  static {
    for (ReportalType type : ReportalType.values()) {
      reportalTypes.put(type.typeName, type);
    }
  }

  public static ReportalType getTypeByName(String typeName) {
    return reportalTypes.get(typeName);
  }

  public boolean checkPermission(User user) {
    return user.hasPermission(permissionName);
  }

  @Override
  public String toString() {
    return typeName;
  }
}
