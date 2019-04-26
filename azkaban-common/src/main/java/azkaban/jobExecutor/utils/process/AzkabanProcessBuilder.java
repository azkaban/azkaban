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
package azkaban.jobExecutor.utils.process;

import com.google.common.base.Joiner;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * Helper code for building a process
 */
public class AzkabanProcessBuilder {

  private final List<String> cmd = new ArrayList<>();
  private Map<String, String> env = new HashMap<>();
  private String workingDir = System.getProperty("user.dir");
  private Logger logger = Logger.getLogger(AzkabanProcess.class);
  private boolean isExecuteAsUser = false;
  private String executeAsUserBinaryPath = null;
  private String effectiveUser = null;

  private int stdErrSnippetSize = 30;
  private int stdOutSnippetSize = 30;

  public AzkabanProcessBuilder(final String... command) {
    addArg(command);
  }

  public AzkabanProcessBuilder addArg(final String... command) {
    for (final String c : command) {
      this.cmd.add(c);
    }
    return this;
  }

  public AzkabanProcessBuilder setWorkingDir(final String dir) {
    this.workingDir = dir;
    return this;
  }

  public String getWorkingDir() {
    return this.workingDir;
  }

  public AzkabanProcessBuilder setWorkingDir(final File f) {
    return setWorkingDir(f.getAbsolutePath());
  }

  public AzkabanProcessBuilder addEnv(final String variable, final String value) {
    this.env.put(variable, value);
    return this;
  }

  public Map<String, String> getEnv() {
    return this.env;
  }

  public AzkabanProcessBuilder setEnv(final Map<String, String> m) {
    this.env = m;
    return this;
  }

  public int getStdErrorSnippetSize() {
    return this.stdErrSnippetSize;
  }

  public AzkabanProcessBuilder setStdErrorSnippetSize(final int size) {
    this.stdErrSnippetSize = size;
    return this;
  }

  public int getStdOutSnippetSize() {
    return this.stdOutSnippetSize;
  }

  public AzkabanProcessBuilder setStdOutSnippetSize(final int size) {
    this.stdOutSnippetSize = size;
    return this;
  }

  public AzkabanProcessBuilder setLogger(final Logger logger) {
    this.logger = logger;
    return this;
  }

  public AzkabanProcess build() {
    if (this.isExecuteAsUser) {
      return new AzkabanProcess(this.cmd, this.env, this.workingDir, this.logger,
          this.executeAsUserBinaryPath, this.effectiveUser);
    } else {
      return new AzkabanProcess(this.cmd, this.env, this.workingDir, this.logger);
    }
  }

  public List<String> getCommand() {
    return this.cmd;
  }

  public String getCommandString() {
    return Joiner.on(" ").join(getCommand());
  }

  @Override
  public String toString() {
    return "ProcessBuilder(cmd = " + Joiner.on(" ").join(this.cmd) + ", env = "
        + this.env + ", cwd = " + this.workingDir + ")";
  }

  public AzkabanProcessBuilder enableExecuteAsUser() {
    this.isExecuteAsUser = true;
    return this;
  }

  public AzkabanProcessBuilder setExecuteAsUserBinaryPath(final String executeAsUserBinaryPath) {
    this.executeAsUserBinaryPath = executeAsUserBinaryPath;
    return this;
  }

  public AzkabanProcessBuilder setEffectiveUser(final String effectiveUser) {
    this.effectiveUser = effectiveUser;
    return this;
  }
}
