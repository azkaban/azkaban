/*
 * Copyright 2018 LinkedIn Corp.
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

import azkaban.reportal.util.Reportal;
import azkaban.user.User;
import azkaban.utils.Props;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;

public enum ReportalType {

  PigJob("ReportalPig", "reportalpig", "hadoop") {
    @Override
    public void buildJobFiles(final Reportal reportal, final Props propertiesFile,
        final File jobFile, final String jobName, final String queryScript,
        final String proxyUser) {
      final File resFolder = new File(jobFile.getParentFile(), "res");
      resFolder.mkdirs();
      final File scriptFile = new File(resFolder, jobName + ".pig");

      OutputStream fileOutput = null;
      try {
        scriptFile.createNewFile();
        fileOutput = new BufferedOutputStream(new FileOutputStream(scriptFile));
        fileOutput.write(queryScript.getBytes(Charset.forName("UTF-8")));
      } catch (final IOException e) {
        e.printStackTrace();
      } finally {
        if (fileOutput != null) {
          try {
            fileOutput.close();
          } catch (final IOException e) {
            e.printStackTrace();
          }
        }
      }
      propertiesFile.put("reportal.pig.script", "res/" + jobName + ".pig");
    }
  },
  HiveJob("ReportalHive", "reportalhive", "hadoop"), TeraDataJob(
      "ReportalTeraData", "reportalteradata", "teradata"),
  TableauJob("ReportalTableau", "reportaltableau", "tableau"),
  DataCollectorJob(
      ReportalTypeManager.DATA_COLLECTOR_JOB, ReportalTypeManager.DATA_COLLECTOR_JOB_TYPE, "") {
    @Override
    public void buildJobFiles(final Reportal reportal, final Props propertiesFile,
        final File jobFile, final String jobName, final String queryScript,
        final String proxyUser) {
      propertiesFile.put("user.to.proxy", proxyUser);
    }
  };

  private static final HashMap<String, ReportalType> reportalTypes =
      new HashMap<>();

  static {
    for (final ReportalType type : ReportalType.values()) {
      reportalTypes.put(type.typeName, type);
    }
  }

  private final String typeName;
  private final String jobTypeName;
  private final String permissionName;

  private ReportalType(final String typeName, final String jobTypeName,
      final String permissionName) {
    this.typeName = typeName;
    this.jobTypeName = jobTypeName;
    this.permissionName = permissionName;
  }

  public static ReportalType getTypeByName(final String typeName) {
    return reportalTypes.get(typeName);
  }

  public void buildJobFiles(final Reportal reportal, final Props propertiesFile,
      final File jobFile, final String jobName, final String queryScript, final String proxyUser) {

  }

  public String getJobTypeName() {
    return this.jobTypeName;
  }

  public boolean checkPermission(final User user) {
    return user.hasPermission(this.permissionName);
  }

  @Override
  public String toString() {
    return this.typeName;
  }
}
