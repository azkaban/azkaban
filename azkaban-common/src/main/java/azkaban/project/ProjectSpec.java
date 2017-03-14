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

package azkaban.project;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;


public class ProjectSpec implements Serializable {
  private String version;
  private PreExecutionSpec preExec;

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public PreExecutionSpec getPreExec() {
    return preExec;
  }

  public void setPreExec(PreExecutionSpec preExec) {
    this.preExec = preExec;
  }

  @Override
  public String toString() {
    return "ProjectSpec{" + "version='" + version + '\'' + ", preExec=" + preExec + '}';
  }

  public static class PreExecutionSpec implements Serializable {
    private Map<String, URI> fetch;

    public Map<String, URI> getFetch() {
      return fetch;
    }

    public void setFetch(Map<String, URI> fetch) {
      this.fetch = fetch;
    }

    @Override
    public String toString() {
      return "PreExecutionSpec{" + "fetch=" + fetch + '}';
    }
  }

}
