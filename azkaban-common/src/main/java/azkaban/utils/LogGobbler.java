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
package azkaban.utils;

import com.google.common.base.Joiner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class LogGobbler extends Thread {

  private final BufferedReader inputReader;
  private final Logger logger;
  private final Level loggingLevel;
  private final CircularBuffer<String> buffer;

  public LogGobbler(final Reader inputReader, final Logger logger,
      final Level level, final int bufferLines) {
    this.inputReader = new BufferedReader(inputReader);
    this.logger = logger;
    this.loggingLevel = level;
    this.buffer = new CircularBuffer<>(bufferLines);
  }

  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        final String line = this.inputReader.readLine();
        if (line == null) {
          return;
        }

        this.buffer.append(line);
        log(line);
      }
    } catch (final IOException e) {
      error("Error reading from logging stream:", e);
    }
  }

  private void log(final String message) {
    if (this.logger != null) {
      this.logger.log(this.loggingLevel, message);
    }
  }

  private void error(final String message, final Exception e) {
    if (this.logger != null) {
      this.logger.error(message, e);
    }
  }

  private void info(final String message, final Exception e) {
    if (this.logger != null) {
      this.logger.info(message, e);
    }
  }

  public void awaitCompletion(final long waitMs) {
    try {
      join(waitMs);
    } catch (final InterruptedException e) {
      info("I/O thread interrupted.", e);
    }
  }

  public String getRecentLog() {
    return Joiner.on(System.getProperty("line.separator")).join(this.buffer);
  }
}
