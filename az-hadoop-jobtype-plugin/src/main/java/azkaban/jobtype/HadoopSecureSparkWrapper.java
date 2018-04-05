/*
 * Copyright 2015 LinkedIn Corp.
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

import azkaban.utils.Props;
import com.google.common.collect.Maps;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.util.Utils;

import static azkaban.flow.CommonJobProperties.ATTEMPT_LINK;
import static azkaban.flow.CommonJobProperties.EXECUTION_LINK;
import static azkaban.flow.CommonJobProperties.JOB_LINK;
import static azkaban.flow.CommonJobProperties.WORKFLOW_LINK;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

/**
 * <pre>
 * A Spark wrapper (more specifically a spark-submit wrapper) that works with Azkaban.
 * This class will be running on a separate process with JVM/ENV properties, classpath and main args
 *  built from {@link HadoopSparkJob}.
 * This class's main() will receive input args built from {@link HadoopSparkJob},
 *  and pass it on to spark-submit to launch spark job.
 * This process will be the client of the spark job.
 *
 * </pre>
 *
 * @see HadoopSecureSparkWrapper
 */
public class HadoopSecureSparkWrapper {

  private static final Logger logger = Logger.getRootLogger();
  private static final String EMPTY_STRING = "";

  //SPARK CONF PARAM
  private static final String SPARK_CONF_EXTRA_DRIVER_OPTIONS = "spark.driver.extraJavaOptions";
  private static final String SPARK_CONF_NUM_EXECUTORS = "spark.executor.instances";
  private static final String SPARK_CONF_SHUFFLE_SERVICE_ENABLED = "spark.shuffle.service.enabled";
  private static final String SPARK_CONF_DYNAMIC_ALLOC_ENABLED = "spark.dynamicAllocation.enabled";
  private static final String SPARK_CONF_QUEUE = "spark.yarn.queue";
  private static final String SPARK_EXECUTOR_NODE_LABEL_EXP = "spark.yarn.executor.nodeLabelExpression";
  private static final String SPARK_EXECUTOR_MEMORY_OVERHEAD = "spark.yarn.executor.memoryOverhead";
  private static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
  private static final String SPARK_EXECUTOR_DEFAULT_MEMORY = "1024M";
  private static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";
  private static final String SPARK_EXECUTOR_DEFAULT_CORES = "1";

  //YARN CONF PARAM
  private static final String YARN_CONF_NODE_LABELING_ENABLED = "yarn.node-labels.enabled";
  public static final String DEFAULT_QUEUE = "default";

  /**
   * Entry point: a Java wrapper to the spark-submit command
   * Args is built in HadoopSparkJob.
   *
   * @param args
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {

    Properties jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();
    HadoopConfigurationInjector.injectResources(new Props(null, jobProps));

    if (HadoopSecureWrapperUtils.shouldProxy(jobProps)) {
      String tokenFile = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
      UserGroupInformation proxyUser =
          HadoopSecureWrapperUtils.setupProxyUser(jobProps, tokenFile, logger);
      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          runSpark(args);
          return null;
        }
      });
    } else {
      runSpark(args);
    }
  }

  /**
   * Actually adjusts cmd args based on execution environment and calls the spark-submit command
   *
   * @param args
   */
  private static void runSpark(String[] args) {

    if (args.length == 0) {
      throw new RuntimeException("SparkSubmit cannot run with zero args");
    }

    // Arg String passed to here are long strings delimited by SparkJobArg.delimiter
    // merge everything together and repartition based by our ^Z character, instead of by the
    // default "space" character
    StringBuilder concat = new StringBuilder();
    concat.append(args[0]);
    for (int i = 1; i < args.length; i++) {
      concat.append(" " + args[i]);
    }
    String[] newArgs = concat.toString().split(SparkJobArg.delimiter);

    // Sample: [--driver-java-options, , --master, yarn-cluster, --class, myclass,
    // --conf, queue=default, --executor-memory, 1g, --num-executors, 15, my.jar, myparams]
    logger.info("Args before adjusting driver java opts: " + Arrays.toString(newArgs));

    // Adjust driver java opts param
    handleDriverJavaOpts(newArgs);

    // If dynamic allocation policy for this jobtype is turned on, adjust related param
    newArgs = handleDynamicResourceAllocation(newArgs);

    // If yarn cluster enables node labeling, adjust related param
    newArgs = handleNodeLabeling(newArgs);

    // Realign params after adjustment
    newArgs = removeNullsFromArgArray(newArgs);
    logger.info("Args after adjusting driver java opts: " + Arrays.toString(newArgs));

    org.apache.spark.deploy.SparkSubmit$.MODULE$.main(newArgs);
  }

  private static void handleDriverJavaOpts(String[] argArray) {
    Configuration conf = new Configuration();
    // Driver java opts is always the first elem(param name) and second elem(value) in the argArray
    // Get current driver java opts here
    StringBuilder driverJavaOptions = new StringBuilder(argArray[1]);
    // In spark-submit, when both --driver-java-options and conf spark.driver.extraJavaOptions is used,
    // spark-submit will only pick --driver-java-options, an arg we always have
    // So if user gives --conf spark.driver.extraJavaOptions=XX, we append the value in --driver-java-options
    for (int i = 0; i < argArray.length; i++) {
      if (argArray[i].equals(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName)
        && argArray[i+1].startsWith(SPARK_CONF_EXTRA_DRIVER_OPTIONS)) {
        driverJavaOptions.append(" ").append(argArray[++i].substring(SPARK_CONF_EXTRA_DRIVER_OPTIONS.length() + 1));
      }
    }

    // Append addtional driver java opts about azkaban context
    String[] requiredJavaOpts = { WORKFLOW_LINK, JOB_LINK, EXECUTION_LINK, ATTEMPT_LINK };
    for (int i = 0; i < requiredJavaOpts.length; i++) {
        driverJavaOptions.append(" ").append(HadoopJobUtils.javaOptStringFromHadoopConfiguration(conf,
                  requiredJavaOpts[i]));
    }
    // Update driver java opts
    argArray[1] = driverJavaOptions.toString();
  }

  private static String[] handleDynamicResourceAllocation(String[] argArray) {
    // HadoopSparkJob will set env var on this process if we enforce dynamic allocation policy for spark jobtype.
    // This policy can be enabled through spark jobtype plugin's conf property.
    // Enabling dynamic allocation policy for azkaban spark jobtype is different from enabling dynamic allocation
    // feature for Spark. This config inside Spark jobtype is to enforce dynamic allocation feature is used for all
    // Spark applications submitted via Azkaban Spark job type.
    String dynamicAllocProp = System.getenv(HadoopSparkJob.SPARK_DYNAMIC_RES_ENV_VAR);
    boolean dynamicAllocEnabled = dynamicAllocProp != null && dynamicAllocProp.equals(Boolean.TRUE.toString());
    if (dynamicAllocEnabled) {
      for (int i = 0; i < argArray.length; i++) {
        if (argArray[i] == null) {
          continue;
        }

        // If user tries to disable dynamic allocation for his application
        // by setting some conf params to false, we need to ignore these settings to enforce the application
        // uses dynamic allocation for spark
        if (argArray[i].equals(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName) // --conf
            && (argArray[i + 1].startsWith(SPARK_CONF_SHUFFLE_SERVICE_ENABLED) // spark.shuffle.service.enabled
            || argArray[i + 1].startsWith(SPARK_CONF_DYNAMIC_ALLOC_ENABLED)) // spark.dynamicAllocation.enabled
            ) {

          logger.info(
              "Azbakan enforces dynamic resource allocation. Ignore user param: " + argArray[i] + " " + argArray[i
                  + 1]);
          argArray[i] = null;
          argArray[++i] = null;
        }
      }
      // If dynamic allocation is enabled, make sure application is scheduled in right queue
      argArray = handleQueueEnforcement(argArray);
    }
    return argArray;
  }

  /**
   * This method is used to enforce queue for Spark application. Rules are explained below.
   * a) If dynamic resource allocation is enabled for selected spark version and application requires large container
   *    then schedule it into default queue by a default conf(spark.yarn.queue) in spark-defaults.conf.
   * b) If dynamic resource allocation is enabled for selected spark version and application requires small container
   *    then schedule it into Org specific queue.
   * c) If dynamic resource allocation is disabled for selected spark version then schedule application into default
   *    queue by a default conf(spark.yarn.queue) in spark-defaults.conf.
   * @param argArray
   * @return
   */
  protected static String[] handleQueueEnforcement(String[] argArray) {
    SparkConf sparkConf = getSparkProperties();
    Configuration conf = new Configuration();

    int queueParameterIndex = getUserSpecifiedQueueParameterIndex(argArray);
    boolean requiredSparkDefaultQueue = false;
    if (sparkConf.getBoolean(SPARK_CONF_DYNAMIC_ALLOC_ENABLED, false)) {
      if (isLargeContainerRequired(argArray, conf, sparkConf)) {
        // Case A
        requiredSparkDefaultQueue = true;
        logger.info("Spark application requires Large containers. Scheduling this application into default queue by a "
            + "default conf(spark.yarn.queue) in spark-defaults.conf.");
      } else {
        // Case B
        logger.info(
            "Dynamic allocation is enabled for selected spark version and application requires small container. "
                + "Hence, scheduling this application into Org specific queue");
        if (queueParameterIndex == -1) {
          LinkedList<String> argList = new LinkedList(Arrays.asList(argArray));
          argList.addFirst(SPARK_CONF_QUEUE + "=" + DEFAULT_QUEUE);
          argList.addFirst(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName);
          argArray = argList.toArray(new String[argList.size()]);
        }
      }
    } else {
      // Case C
      logger.info("Spark version, selected for this application, doesn't support dynamic allocation. Scheduling this "
          + "application into default queue by a default conf(spark.yarn.queue) in spark-defaults.conf.");
      requiredSparkDefaultQueue = true;
    }

    if (queueParameterIndex != -1 && requiredSparkDefaultQueue) {
      logger.info("Azbakan enforces spark.yarn.queue queue. Ignore user param: " + argArray[queueParameterIndex] + " "
          + argArray[queueParameterIndex + 1]);
      argArray[queueParameterIndex] = null;
      argArray[queueParameterIndex + 1] = null;
    }
    return argArray;
  }

  /**
   * This method is used to check whether large container is required for application or not.
   * To decide that, it is using parameters like
   * User Job parameters/default value for : spark.executor.cores, spark.executor.memory, spark.yarn.executor.memoryOverhead
   * Jobtype Plugin parameters: spark.min.mem.vore.ratio, spark.min.memory-gb.size
   * If rounded memory / spark.executor.cores >= spark.min.mem.vore.ratio or rounded memory >= spark.min.memory-gb.size
   * then large container is required to schedule this application.
   * @param conf
   * @param sparkConf
   * @return
   */
  private static boolean isLargeContainerRequired(String[] argArray, Configuration conf, SparkConf sparkConf) {
    Map<String, String> executorParameters = getUserSpecifiedExecutorParameters(argArray);
    String executorVcore = executorParameters.get(SPARK_EXECUTOR_CORES);
    String executorMem = executorParameters.get(SPARK_EXECUTOR_MEMORY);
    String executorMemOverhead = executorParameters.get(SPARK_EXECUTOR_MEMORY_OVERHEAD);
    if (executorVcore == null) {
      executorVcore = sparkConf.get(SPARK_EXECUTOR_CORES, SPARK_EXECUTOR_DEFAULT_CORES);
    }
    if (executorMem == null) {
      executorMem = sparkConf.get(SPARK_EXECUTOR_MEMORY, SPARK_EXECUTOR_DEFAULT_MEMORY);
    }
    if (executorMemOverhead == null) {
      executorMemOverhead = sparkConf.get(SPARK_EXECUTOR_MEMORY_OVERHEAD, null);
    }

    double roundedMemoryGbSize = getRoundedMemoryGb(executorMem, executorMemOverhead, conf);

    double minRatio = Double.parseDouble(System.getenv(HadoopSparkJob.SPARK_MIN_MEM_VCORE_RATIO_ENV_VAR));
    double minMemSize = Double.parseDouble(System.getenv(HadoopSparkJob.SPARK_MIN_MEM_SIZE_ENV_VAR));

    logger.info(
        "RoundedMemoryGbSize: " + roundedMemoryGbSize + ", ExecutorVcore: " + executorVcore + ", MinRatio: " + minRatio
            + ", MinMemSize: " + minMemSize);
    return roundedMemoryGbSize / (double) Integer.parseInt(executorVcore) >= minRatio
        || roundedMemoryGbSize >= minMemSize;
  }

  protected static String[] handleNodeLabeling(String[] argArray) {
    // HadoopSparkJob will set env var on this process if we enable node labeling policy for spark jobtype.
    // We also detect the yarn cluster settings has enable node labeling
    // Enabling node labeling policy for spark job type is different from enabling node labeling
    // feature for Yarn. This config inside Spark job type is to enforce node labeling feature for all
    // Spark applications submitted via Azkaban Spark job type.
    Configuration conf = new Configuration();
    boolean nodeLabelingYarn = conf.getBoolean(YARN_CONF_NODE_LABELING_ENABLED, false);
    String nodeLabelingProp = System.getenv(HadoopSparkJob.SPARK_NODE_LABELING_ENV_VAR);
    boolean nodeLabelingPolicy = nodeLabelingProp != null && nodeLabelingProp.equals(Boolean.TRUE.toString());
    String autoNodeLabelProp = System.getenv(HadoopSparkJob.SPARK_AUTO_NODE_LABELING_ENV_VAR);
    boolean autoNodeLabeling = autoNodeLabelProp != null && autoNodeLabelProp.equals(Boolean.TRUE.toString());
    String desiredNodeLabel = System.getenv(HadoopSparkJob.SPARK_DESIRED_NODE_LABEL_ENV_VAR);

    SparkConf sparkConf = getSparkProperties();

    if (nodeLabelingYarn && nodeLabelingPolicy) {
      ignoreUserSpecifiedNodeLabelParameter(argArray, autoNodeLabeling);

      // If auto node labeling is enabled, automatically sets spark.yarn.executor.nodeLabelExpression
      // config based on user requested resources.
      if (autoNodeLabeling) {
        if (isLargeContainerRequired(argArray, conf, sparkConf)) {
          LinkedList<String> argList = new LinkedList<String>(Arrays.asList(argArray));
          argList.addFirst(SPARK_EXECUTOR_NODE_LABEL_EXP + "=" + desiredNodeLabel);
          argList.addFirst(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName);
          argArray = argList.toArray(new String[argList.size()]);
        }
      }
    }
    return argArray;
  }

  /**
   * This method is used to ignore user specified node label Parameter. When auto node labeling is enabled,
   * job type should ignore user supplied node label expression for Spark executors.
   * @param argArray
   * @param autoNodeLabeling
   */
  private static void ignoreUserSpecifiedNodeLabelParameter(String[] argArray, boolean autoNodeLabeling) {
    for (int i = 0; i < argArray.length; i++) {
      if (argArray[i] == null) {
        continue;
      }
      if (autoNodeLabeling) {
        // This config will be automatically set by the job type based on the mem-to-vcore resource ratio requested by
        // the user application.
        if (argArray[i].equals(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName) && argArray[i + 1]
            .startsWith(SPARK_EXECUTOR_NODE_LABEL_EXP)) {
          logger.info(
              "Azbakan auto-sets node label expression. Ignore user param: " + argArray[i] + " " + argArray[i + 1]);
          argArray[i] = null;
          argArray[++i] = null;
          continue;
        }
      }
    }
  }

  /**
   * This method is used to get User specified executor parameters. It is capturing executor-memory, executor-cores and
   * spark.yarn.executor.memoryOverhead.
   * @param argArray
   * @return
   */
  private static Map<String, String> getUserSpecifiedExecutorParameters(String[] argArray) {
    Map<String, String> executorParameters = Maps.newHashMap();
    for (int i = 0; i < argArray.length; i++) {
      if (argArray[i] == null) {
        continue;
      }
      if (argArray[i].equals(SparkJobArg.EXECUTOR_CORES.sparkParamName)) {
        executorParameters.put(SPARK_EXECUTOR_CORES, argArray[++i]);
      }
      if (argArray[i].equals(SparkJobArg.EXECUTOR_MEMORY.sparkParamName)) {
        executorParameters.put(SPARK_EXECUTOR_MEMORY, argArray[++i]);
      }
      if (argArray[i].equals(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName) && argArray[i + 1]
          .startsWith(SPARK_EXECUTOR_MEMORY_OVERHEAD)) {
        executorParameters.put(SPARK_EXECUTOR_MEMORY_OVERHEAD, argArray[i + 1].split("=")[1].trim());
      }
    }
    return executorParameters;
  }

  /**
   * This method is used to retrieve index of queue parameter passed by User.
   * @param argArray
   * @return
   */
  private static int getUserSpecifiedQueueParameterIndex(String[] argArray) {
    int queueParameterIndex = -1;
    for (int i = 0; i < argArray.length; i++) {
      if (argArray[i] == null) {
        continue;
      }
      // Fetch index of queue parameter passed by User.
      // (--queue test or --conf spark.yarn.queue=test)
      if ((argArray[i].equals(SparkJobArg.SPARK_CONF_PREFIX.sparkParamName) && argArray[i + 1]
          .startsWith(SPARK_CONF_QUEUE)) || (argArray[i].equals(SparkJobArg.QUEUE.sparkParamName))) {
        queueParameterIndex = i++;
        break;
      }
    }
    return queueParameterIndex;
  }

  /**
   * This method is used to get Spark properties which will fetch properties from spark-defaults.conf file.
   * @return
   */
  private static SparkConf getSparkProperties() {
    String sparkPropertyFile = HadoopSecureSparkWrapper.class.getClassLoader()
        .getResource("spark-defaults.conf").getPath();
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.setAll(Utils.getPropertiesFromFile(sparkPropertyFile));
    return sparkConf;
  }

  /**
   * Get the memory GB size of Spark executor containers. The logic is as follows:
   * 1) Transforms requested memory String into a number representing amount of MBs requested.
   * 2a) If memory overhead is not set by the user, use the default logic to calculate it,
   * which is to add max(requestedMemInMB * 10%, 384) to the requested memory size.
   * 2b) If memory overhead is set by the user, directly add it.
   * 3) Use the logic inside YARN to round up the container size according to defined min
   * allocation for memory size.
   * 4) Return the memory GB size.
   * @param mem requested executor memory size, of the format 2G or 1024M
   * @param memOverhead user defined memory overhead
   * @param config Hadoop Configuration object
   * @return the rounded executor memory GB size
   */
  private static double getRoundedMemoryGb(String mem, String memOverhead,
      Configuration config) {
    long memoryMb = JavaUtils.byteStringAsMb(mem);
    if (memOverhead == null || !NumberUtils.isDigits(memOverhead)) {
      memoryMb += Math.max(memoryMb / 10, 384);
    } else {
      memoryMb += Long.parseLong(memOverhead);
    }
    int increment = config.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    return Math.ceil(memoryMb * 1.0 / increment) * increment / 1024;
  }

  protected static String[] removeNullsFromArgArray(String[] argArray) {
    List<String> argList = new ArrayList<String>(Arrays.asList(argArray));
    argList.removeAll(Collections.singleton(null));
    return argList.toArray(new String[argList.size()]);
  }
}
