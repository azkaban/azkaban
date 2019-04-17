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
package azkaban.jobExecutor;

import azkaban.utils.Props;
import org.apache.log4j.Logger;


/**
 * Base Job
 */
public abstract class AbstractJob implements Job {

  public static final String JOB_TYPE = "type";
  public static final String JOB_CLASS = "job.class";
  public static final String JOB_PATH = "job.path";
  public static final String JOB_FULLPATH = "job.fullpath";
  public static final String JOB_ID = "job.id";

  private final String _id;
  private final Logger _log;
  private volatile double _progress;

  protected AbstractJob(final String id, final Logger log) {
    this._id = id;
    this._log = log;
    this._progress = 0.0;
  }

  @Override
  public String getId() {
    return this._id;
  }

  @Override
  public double getProgress() throws Exception {
    return this._progress;
  }

  public void setProgress(final double progress) {
    this._progress = progress;
  }

  @Override
  public void cancel() throws Exception {
    throw new RuntimeException("Job " + this._id + " does not support cancellation!");
  }

  @Override
  public Props getJobGeneratedProperties() {
    return new Props();
  }

  @Override
  public abstract void run() throws Exception;

  @Override
  public boolean isCanceled() {
    return false;
  }

  public Logger getLog() {
    return this._log;
  }

  public void debug(final String message) {
    this._log.debug(message);
  }

  public void debug(final String message, final Throwable t) {
    this._log.debug(message, t);
  }

  public void info(final String message) {
    this._log.info(message);
  }

  public void info(final String message, final Throwable t) {
    this._log.info(message, t);
  }

  public void warn(final String message) {
    this._log.warn(message);
  }

  public void warn(final String message, final Throwable t) {
    this._log.warn(message, t);
  }

  public void error(final String message) {
    this._log.error(message);
  }

  public void error(final String message, final Throwable t) {
    this._log.error(message, t);
  }
}
