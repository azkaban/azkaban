package azkaban.jobtype;


public enum SparkJobArg {
  // standard spark submit arguments, ordered in the spark-submit --help order
  MASTER("master", false), // just to trick the eclipse formatter
  DEPLOY_MODE("deploy-mode", false), //
  CLASS("class", false), //
  NAME("name", false), //
  SPARK_JARS("jars", true), //
  SPARK_PACKAGES("packages", false),
  PACKAGES("packages", false), //
  REPOSITORIES("repositories", false), //
  PY_FILES("py-files", false), //
  FILES("files", false), //
  SPARK_CONF_PREFIX("conf.", "--conf", true), //
  PROPERTIES_FILE("properties-file", false), //
  DRIVER_MEMORY("driver-memory", false), //
  DRIVER_JAVA_OPTIONS("driver-java-options", true), //
  DRIVER_LIBRARY_PATH("driver-library-path", false), //
  DRIVER_CLASS_PATH("driver-class-path", false), //
  EXECUTOR_MEMORY("executor-memory", false), //
  PROXY_USER("proxy-user", false), //
  SPARK_FLAG_PREFIX("flag.", "--", true), // --help, --verbose, --supervise, --version

  // Yarn only Arguments
  EXECUTOR_CORES("executor-cores", false), //
  DRIVER_CORES("driver-cores", false), //
  QUEUE("queue", false), //
  NUM_EXECUTORS("num-executors", false), //
  ARCHIVES("archives", false), //
  PRINCIPAL("principal", false), //
  KEYTAB("keytab", false), //

  // Not SparkSubmit arguments: only exists in azkaban
  EXECUTION_JAR("execution-jar", null, true), //
  PARAMS("params", null, true), //
  SPARK_VERSION("spark-version", null, true),
  ;

  public static final String delimiter = "\u001A";

  SparkJobArg(String propName, boolean specialTreatment) {
    this(propName, "--" + propName, specialTreatment);
  }

  SparkJobArg(String azPropName, String sparkParamName, boolean specialTreatment) {
    this.azPropName = azPropName;
    this.sparkParamName = sparkParamName;
    this.needSpecialTreatment = specialTreatment;
  }

  final String azPropName;

  final String sparkParamName;

  final boolean needSpecialTreatment;
}
