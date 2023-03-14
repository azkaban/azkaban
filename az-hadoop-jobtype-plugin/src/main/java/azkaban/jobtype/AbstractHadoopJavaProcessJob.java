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
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * Abstract Hadoop Java Process Job for Job PlugIn
 */
public abstract class AbstractHadoopJavaProcessJob extends JavaProcessJob implements IHadoopJob {

  private final HadoopProxy hadoopProxy;

  // when jobtype runs in a classloader that is different than that of HadoopSecurityManager
  // any call to new Configuration() in jobtype will not add the following files as default
  // resources. This is not a problem when HadoopSecurityManager_H_2_0 and jobtype plugin code
  // runs within the same classloader because HadoopSecurityManager_H_2_0 does it already.
  static {
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
    Configuration.addDefaultResource("yarn-default.xml");
    Configuration.addDefaultResource("yarn-site.xml");
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  public AbstractHadoopJavaProcessJob(String jobid, Props sysProps, Props jobProps, Logger logger) {
    super(jobid, sysProps, jobProps, logger);
    this.hadoopProxy = new HadoopProxy(sysProps, jobProps, logger);
  }

  public AbstractHadoopJavaProcessJob(String jobid, Props sysProps, Props jobProps,
      Props privateProps, Logger logger) {
    super(jobid, sysProps, jobProps, privateProps, logger);
    this.hadoopProxy = new HadoopProxy(sysProps, jobProps, logger);
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
        CommonJobProperties.PROJECT_NAME,
        CommonJobProperties.AZKABAN_WEBSERVERHOST,
        CommonJobProperties.JOB_ID,
        CommonJobProperties.JOB_ATTEMPT,
        CommonJobProperties.NESTED_FLOW_PATH
    };
    Props jobProps = getJobProps();
    String tagList = HadoopJobUtils.constructHadoopTags(jobProps, tagKeys);
    tagList = Joiner.on(',').skipNulls().join(tagList, HadoopJobUtils.constructSubflowTags(jobProps));
    if (jobProps.containsKey(CommonJobProperties.PROJECT_NAME)
        && jobProps.containsKey(CommonJobProperties.FLOW_ID)) {
      String workflowTag = "workflowid:"
          + jobProps.get(CommonJobProperties.PROJECT_NAME)
          + HadoopConfigurationInjector.WORKFLOW_ID_SEPERATOR
          + jobProps.get(CommonJobProperties.FLOW_ID);
      workflowTag = workflowTag.substring(0,
          Math.min(workflowTag.length(), HadoopJobUtils.APPLICATION_TAG_MAX_LENGTH));
      tagList = Joiner.on(',').skipNulls().join(tagList, workflowTag);
    }
    getLog().info("[HadoopTag] generated job tags: " + tagList);
    getJobProps().put(
        HadoopConfigurationInjector.INJECT_PREFIX + HadoopJobUtils.MAPREDUCE_JOB_TAGS,
        tagList
    );
  }

  protected String getJVMJobTypeParameters() {
    String args = "";

    String jobTypeUserGlobalJVMArgs = getJobProps().getString(HadoopJobUtils.JOBTYPE_GLOBAL_JVM_ARGS, null);
    if (jobTypeUserGlobalJVMArgs != null) {
      args += " " + jobTypeUserGlobalJVMArgs;
    }
    String jobTypeSysGlobalJVMArgs = getSysProps().getString(HadoopJobUtils.JOBTYPE_GLOBAL_JVM_ARGS, null);
    if (jobTypeSysGlobalJVMArgs != null) {
      args += " " + jobTypeSysGlobalJVMArgs;
    }
    String jobTypeUserJVMArgs = getJobProps().getString(HadoopJobUtils.JOBTYPE_JVM_ARGS, null);
    if (jobTypeUserJVMArgs != null) {
      args += " " + jobTypeUserJVMArgs;
    }
    String jobTypeSysJVMArgs = getSysProps().getString(HadoopJobUtils.JOBTYPE_JVM_ARGS, null);
    if (jobTypeSysJVMArgs != null) {
      args += " " + jobTypeSysJVMArgs;
    }
    return args;
  }

  protected String getJVMProxySecureArgument() {
    return hadoopProxy.getJVMArgument(getSysProps(), getJobProps(), getLog());
  }

  protected List<String> getAzkabanCommonClassPaths() {
    return ImmutableList.<String>builder()
        .add(FileIOUtils.getSourcePathFromClass(Props.class)) // add az-core jar classpath
        .add(FileIOUtils.getSourcePathFromClass(JavaProcessJob.class)) // add az-common jar classpath
        .add(FileIOUtils.getSourcePathFromClass(HadoopSecureHiveWrapper.class))
        .add(FileIOUtils.getSourcePathFromClass(HadoopSecurityManager.class))
        .build();
  }

  @Override
  public Props appendExtraProps(Props props) {
    HadoopJobUtils.addAdditionalNamenodesToPropsFromMRJob(props, getLog());
    return props;
  }

  @Override
  public HadoopProxy getHadoopProxy() {
    return hadoopProxy;
  }
}
