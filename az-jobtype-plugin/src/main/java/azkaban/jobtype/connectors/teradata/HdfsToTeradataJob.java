/*
 * Copyright 2015-2016 LinkedIn Corp.
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

package azkaban.jobtype.connectors.teradata;

import static azkaban.jobtype.connectors.teradata.TdchConstants.*;

import org.apache.log4j.Logger;

import azkaban.utils.Props;

/**
 * Entry point for HDFSToTeradata Job that prepares/forks new JVM for the job.
 */
public class HdfsToTeradataJob extends TeradataJob {
  private static final Logger logger = Logger.getLogger(HdfsToTeradataJob.class);

  public HdfsToTeradataJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);
    getJobProps().put(LIB_JARS_HIVE_KEY, sysProps.get(LIB_JARS_HIVE_KEY));
  }

  @Override
  protected String getJavaClass() {
    return HdfsToTeradataJobRunnerMain.class.getName();
  }
}
