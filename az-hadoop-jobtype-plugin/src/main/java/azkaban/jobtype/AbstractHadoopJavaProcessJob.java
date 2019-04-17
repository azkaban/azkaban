/*
 * Copyright 2019 LinkedIn Corp.
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

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.utils.Props;
import org.apache.log4j.Logger;


/**
 * Abstract Hadoop Java Process Job for Job PlugIn
 */
public abstract class AbstractHadoopJavaProcessJob extends JavaProcessJob implements IHadoopJob {

  public AbstractHadoopJavaProcessJob(String jobid, Props sysProps, Props jobProps, Logger logger) {
    super(jobid, sysProps, jobProps, logger);
    hadoopProxy.init(sysProps, jobProps, logger);
  }

  @Override
  public void run() throws Exception {
    try {
      super.run();
    } catch (Throwable t) {
      t.printStackTrace();
      getLog().error("caught error running the job", t);
      throw t;
    } finally {
      hadoopProxy.cancelHadoopTokens(getLog());
    }
  }

  @Override
  public void setupHadoopJobProperties() {
    String[] tagKeys = new String[]{
        CommonJobProperties.EXEC_ID,
        CommonJobProperties.FLOW_ID,
        CommonJobProperties.PROJECT_NAME
    };
    getJobProps().put(
        HadoopConfigurationInjector.INJECT_PREFIX + HadoopJobUtils.MAPREDUCE_JOB_TAGS,
        HadoopJobUtils.constructHadoopTags(getJobProps(), tagKeys)
    );
  }

  protected String getJVMProxySecureArgument() {
    return hadoopProxy.getJVMArgument(getSysProps(), getJobProps(), getLog());
  }

  @Override
  public Props appendExtraProps(Props props) {
    HadoopJobUtils.addAdditionalNamenodesToPropsFromMRJob(props, getLog());
    return props;
  }
}
