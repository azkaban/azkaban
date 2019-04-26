/*
 * Copyright 2014 LinkedIn, Inc
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
package azkaban.executor;

import azkaban.jobExecutor.JavaProcessJob;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import java.io.File;
import java.util.List;
import org.apache.log4j.Logger;


public class JavaJob extends JavaProcessJob {

  public static final String RUN_METHOD_PARAM = "method.run";
  public static final String CANCEL_METHOD_PARAM = "method.cancel";
  public static final String PROGRESS_METHOD_PARAM = "method.progress";

  public static final String JOB_CLASS = "job.class";
  public static final String DEFAULT_CANCEL_METHOD = "cancel";
  public static final String DEFAULT_RUN_METHOD = "run";
  public static final String DEFAULT_PROGRESS_METHOD = "getProgress";
  private final Object _javaObject = null;
  private String _runMethod;
  private String _cancelMethod;
  private String _progressMethod;
  private String props;

  public JavaJob(final String jobid, final Props sysProps, final Props jobProps, final Logger log) {
    super(jobid, sysProps, new Props(sysProps, jobProps), log);
  }

  @Override
  protected List<String> getClassPaths() {
    final List<String> classPath = super.getClassPaths();

    classPath.add(FileIOUtils.getSourcePathFromClass(JavaJobRunnerMain.class));
    classPath.add(FileIOUtils.getSourcePathFromClass(Props.class));

    final String loggerPath = FileIOUtils.getSourcePathFromClass(org.apache.log4j.Logger.class);
    if (!classPath.contains(loggerPath)) {
      classPath.add(loggerPath);
    }

    // Add hadoop home to classpath
    final String hadoopHome = System.getenv("HADOOP_HOME");
    if (hadoopHome == null) {
      info("HADOOP_HOME not set, using default hadoop config.");
    } else {
      info("Using hadoop config found in " + hadoopHome);
      classPath.add(new File(hadoopHome, "conf").getPath());
    }
    return classPath;
  }

  @Override
  protected String getJavaClass() {
    return JavaJobRunnerMain.class.getName();
  }

  @Override
  public String toString() {
    return "JavaJob{" + "_runMethod='" + this._runMethod + '\''
        + ", _cancelMethod='" + this._cancelMethod + '\'' + ", _progressMethod='"
        + this._progressMethod + '\'' + ", _javaObject=" + this._javaObject + ", props="
        + this.props + '}';
  }
}
