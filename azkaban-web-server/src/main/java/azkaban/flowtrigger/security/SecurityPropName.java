package azkaban.flowtrigger.security;

/**
 * Lists some of the keys that are present in the props file. This file has specifically been
 * created for a proof-of-concept exercise. It will no longer be needed, once we create a
 * new config system for CloudFlow.
 */
class SecurityPropName {
  static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";
  static final String AZKABAN_KEYTAB_LOCATION = "proxy.keytab.location";
  static final String AZKABAN_PRINCIPAL = "proxy.user";
  static final String OBTAIN_JOBHISTORYSERVER_TOKEN = "obtain.jobhistoryserver.token";
  static final String OBTAIN_BINARY_TOKEN = "obtain.binary.token";
  }
