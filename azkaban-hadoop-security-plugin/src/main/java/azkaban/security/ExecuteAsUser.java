/*
 * Copyright 2011 LinkedIn Corp.
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
package azkaban.security;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a wrapper over the binary executable execute-as-user. It provides a simple API to run commands as
 * another user while abstracting away the process logic, commandline handling, etc.
 *
 */
public class ExecuteAsUser {
  private final static Logger log = Logger.getLogger(ExecuteAsUser.class);
  private final static String EXECUTE_AS_USER = "execute-as-user";

  private final File binaryExecutable;

  /**
   * Construct the object
   *
   * @param nativeLibDirectory Absolute path to the native Lib Directory
   */
  public ExecuteAsUser(final String nativeLibDirectory) {
    this.binaryExecutable = new File(nativeLibDirectory, EXECUTE_AS_USER);
    validate();
  }

  private void validate() {
    if (!binaryExecutable.canExecute()) {
      throw new RuntimeException("Unable to execute execute-as-user binary. Invalid Path: "
          + binaryExecutable.getAbsolutePath());
    }
  }

  /**
   * API to execute a command on behalf of another user.
   *
   * @param user The proxy user
   * @param command the list containing the program and its arguments
   * @return The return value of the shell command
   * @throws IOException
   */
  public int execute(final String user, final List<String> command) throws IOException {
    log.info("Command: " + command);
    Process process = new ProcessBuilder()
        .command(constructExecuteAsCommand(user, command))
        .inheritIO()
        .start();

    int exitCode;
    try {
      exitCode = process.waitFor();
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
      exitCode = 1;
    }
    return exitCode;
  }

  private List<String> constructExecuteAsCommand(String user, List<String> command) {
    List<String> commandList = new ArrayList<>();
    commandList.add(binaryExecutable.getAbsolutePath());
    commandList.add(user);
    commandList.addAll(command);
    return commandList;
  }
}
