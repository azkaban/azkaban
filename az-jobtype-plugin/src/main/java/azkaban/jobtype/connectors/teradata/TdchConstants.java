/*
 * Copyright (C) 2015-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package azkaban.jobtype.connectors.teradata;

public interface TdchConstants {
  public static final String TERADATA_JDBCDRIVER_CLASSNAME = "com.teradata.jdbc.TeraDriver";
  public static final String TD_WALLET_FORMAT = "$tdwallet(%s)";
  public static final String DEFAULT_TDCH_JOB_TYPE = "hdfs";
  public static final String TDCH_HIVE_JOB_TYPE = "hive";
  public static final String AVRO_FILE_FORMAT = "avrofile";
  public static final String LIB_JAR_DELIMITER = ",";
  public static final int DEFAULT_NO_MAPPERS = 8;

  //Keys for the properties
  public static final String TDCH_JOB_TYPE = "tdch.jobtype";
  public static final String TDCH_LOG_LEVEL = "tdch.log.level";

  public static final String TD_WALLET_JAR = "jobtype.tdwallet.jar";
  public static final String LIB_JARS_KEY = "libjars";
  public static final String LIB_JARS_HIVE_KEY = "libjars.hive";
  public static final String TD_HOSTNAME_KEY = "td.hostname";
  public static final String TD_USERID_KEY = "td.userid";
  @Deprecated
  public static final String TD_CREDENTIAL_NAME_KEY = "td.credentialName";
  public static final String TD_ENCRYPTED_CREDENTIAL_KEY = "td.encrypted.credential";
  public static final String TD_CRYPTO_KEY_PATH_KEY = "td.crypto.key.path";
  public static final String AVRO_SCHEMA_PATH_KEY = "avro.schema.path";
  public static final String AVRO_SCHEMA_INLINE_KEY = "avro.schema.inline";

  public static final String TD_NUM_MAPPERS = "tdch.num.mappers";
  public static final String TD_INSERT_METHOD_KEY = "tdch.insert.method";
  public static final String SOURCE_HDFS_PATH_KEY = "source.hdfs.path";
  public static final String TARGET_TD_TABLE_NAME_KEY = "target.td.tablename";
  public static final String REPLACE_TARGET_TABLE_KEY = "target.td.table.replace";

  public static final String ERROR_DB_KEY = "td.error.database";
  public static final String ERROR_TABLE_KEY = "td.error.table";
  public static final String DROP_ERROR_TABLE_KEY = "td.error.table.drop";
  public static final String HDFS_FILE_FORMAT_KEY = "hdfs.fileformat";
  public static final String HDFS_FIELD_SEPARATOR_KEY = "hdfs.separator";
  public static final String HADOOP_CONFIG_KEY = "hadoop.config";
  public static final String HADOOP_CONFIG_PREFIX_KEY = "hadoop.config.";

  public static final String TD_RETRIEVE_METHOD_KEY = "tdch.retrieve.method";
  public static final String SOURCE_TD_TABLE_NAME_KEY = "source.td.tablename";
  public static final String SOURCE_TD_QUERY_NAME_KEY = "source.td.sourcequery";
  public static final String TARGET_HDFS_PATH_KEY = "target.hdfs.path";
  public static final String TD_OTHER_PROPERTIES_HOCON_KEY = "tdch.other.properties.hocon";
  public static final String JOB_OUTPUT_PROPERTIES_KEY = "output.property.keys";

  public static final String SOURCE_HIVE_DATABASE_NAME_KEY = "source.hive.databasename";
  public static final String SOURCE_HIVE_TABLE_NAME_KEY = "source.hive.tablename";
  public static final String TDCH_HIVE_CONF_KEY = "tdch.hive.conf";
}
