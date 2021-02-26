/*
 * Copyright 2017 LinkedIn Corp.
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
 *
 */

package azkaban.webapp;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This is an abstract class for representing azkaban common status.
 */
public abstract class Status {
  @JsonProperty("version")
  private final String version;
  @JsonProperty("pid")
  private final String pid;
  @JsonProperty("installationPath")
  private final String installationPath;
  @JsonProperty("usedMemory")
  private final long usedMemory;
  @JsonProperty("xmx")
  private final long xmx;
  @JsonProperty("isDatabaseUp")
  private final boolean isDatabaseUp;

  public Status(final String version,
      final String pid,
      final String installationPath,
      final long usedMemory,
      final long xmx,
      final boolean isDatabaseUp) {
    this.version = version;
    this.pid = pid;
    this.installationPath = installationPath;
    this.usedMemory = usedMemory;
    this.xmx = xmx;
    this.isDatabaseUp = isDatabaseUp;
  }

  public String getVersion() {
    return version;
  }

  public String getPid() {
    return pid;
  }

  public String getInstallationPath() {
    return installationPath;
  }

  public long getUsedMemory() {
    return usedMemory;
  }

  public long getXmx() {
    return xmx;
  }

  public boolean isDatabaseUp() {
    return isDatabaseUp;
  }
}
