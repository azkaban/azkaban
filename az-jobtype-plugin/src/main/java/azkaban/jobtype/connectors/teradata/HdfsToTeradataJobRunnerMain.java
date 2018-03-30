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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobtype.*;
import azkaban.jobtype.connectors.jdbc.JdbcCommands;
import azkaban.jobtype.connectors.jdbc.TeradataCommands;
import azkaban.crypto.Decryptions;
import azkaban.jobtype.javautils.JobUtils;
import azkaban.jobtype.javautils.Whitelist;
import azkaban.utils.Props;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.teradata.hadoop.tool.TeradataExportTool;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

public class HdfsToTeradataJobRunnerMain {
  private static final List<String> ERR_TABLE_SUFFIXES = ImmutableList.<String>builder().add("_ERR_1", "_ERR_2").build();

  private final Properties _jobProps;
  private final TdchParameters _params;
  private final Logger _logger;


  public HdfsToTeradataJobRunnerMain() throws FileNotFoundException, IOException {
    this(HadoopSecureWrapperUtils.loadAzkabanProps());
  }

  private HdfsToTeradataJobRunnerMain(Properties jobProps) throws FileNotFoundException, IOException {
    this(jobProps, new Decryptions());
  }

  @VisibleForTesting
  HdfsToTeradataJobRunnerMain(Properties jobProps, Decryptions decryptions) throws FileNotFoundException, IOException {
    _logger = JobUtils.initJobLogger();
    _logger.info("Job properties: " + jobProps);

    String logLevel = jobProps.getProperty(TdchConstants.TDCH_LOG_LEVEL);
    if(!StringUtils.isEmpty(logLevel)) {
      _logger.setLevel(Level.toLevel(logLevel));
    }
    _jobProps = jobProps;
    Props props = new Props(null, _jobProps);

    HadoopConfigurationInjector.injectResources(props);
    Configuration conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);

    if (props.containsKey(Whitelist.WHITE_LIST_FILE_PATH_KEY)) {
      new Whitelist(props, FileSystem.get(conf)).validateWhitelisted(props);
    }

    String encryptedCredential = _jobProps.getProperty(TdchConstants.TD_ENCRYPTED_CREDENTIAL_KEY);
    String cryptoKeyPath = _jobProps.getProperty(TdchConstants.TD_CRYPTO_KEY_PATH_KEY);
    String password = null;

    if(encryptedCredential != null && cryptoKeyPath != null) {
      password = decryptions.decrypt(encryptedCredential, cryptoKeyPath, FileSystem.get(new Configuration()));
    }

    _params = TdchParameters.builder()
                            .mrParams(props.getMapByPrefix(TdchConstants.HADOOP_CONFIG_PREFIX_KEY).values())
                            .libJars(createLibJarStr(props))
                            .tdJdbcClassName(TdchConstants.TERADATA_JDBCDRIVER_CLASSNAME)
                            .teradataHostname(props.getString(TdchConstants.TD_HOSTNAME_KEY))
                            .fileFormat(_jobProps.getProperty(TdchConstants.HDFS_FILE_FORMAT_KEY))
                            .fieldSeparator(_jobProps.getProperty(TdchConstants.HDFS_FIELD_SEPARATOR_KEY))
                            .jobType(props.getString(TdchConstants.TDCH_JOB_TYPE, TdchConstants.DEFAULT_TDCH_JOB_TYPE))
                            .userName(props.getString(TdchConstants.TD_USERID_KEY))
                            .credentialName(_jobProps.getProperty(TdchConstants.TD_CREDENTIAL_NAME_KEY))
                            .password(password)
                            .avroSchemaPath(_jobProps.getProperty(TdchConstants.AVRO_SCHEMA_PATH_KEY))
                            .avroSchemaInline(_jobProps.getProperty(TdchConstants.AVRO_SCHEMA_INLINE_KEY))
                            .sourceHdfsPath(_jobProps.getProperty(TdchConstants.SOURCE_HDFS_PATH_KEY))
                            .targetTdTableName(props.getString(TdchConstants.TARGET_TD_TABLE_NAME_KEY))
                            .errorTdDatabase(_jobProps.getProperty(TdchConstants.ERROR_DB_KEY))
                            .errorTdTableName(_jobProps.getProperty(TdchConstants.ERROR_TABLE_KEY))
                            .tdInsertMethod(_jobProps.getProperty(TdchConstants.TD_INSERT_METHOD_KEY))
                            .numMapper(props.getInt(TdchConstants.TD_NUM_MAPPERS, TdchConstants.DEFAULT_NO_MAPPERS))
                            .hiveSourceDatabase(_jobProps.getProperty(TdchConstants.SOURCE_HIVE_DATABASE_NAME_KEY))
                            .hiveSourceTable(_jobProps.getProperty(TdchConstants.SOURCE_HIVE_TABLE_NAME_KEY))
                            .hiveConfFile(_jobProps.getProperty(TdchConstants.TDCH_HIVE_CONF_KEY))
                            .otherProperties(_jobProps.getProperty(TdchConstants.TD_OTHER_PROPERTIES_HOCON_KEY))
                            .build();
  }

  private String createLibJarStr(Props props) {
    if (TdchConstants.TDCH_HIVE_JOB_TYPE.equals(props.getString(TdchConstants.TDCH_JOB_TYPE, TdchConstants.DEFAULT_TDCH_JOB_TYPE))) {
      return props.getString(TdchConstants.LIB_JARS_HIVE_KEY);
    }
    return props.getString(TdchConstants.LIB_JARS_KEY);
  }

  public void run() throws IOException, InterruptedException {
    String jobName = System.getenv(AbstractProcessJob.JOB_NAME_ENV);
    _logger.info("Running job " + jobName);
    preprocess();

    if (HadoopSecureWrapperUtils.shouldProxy(_jobProps)) {
      String tokenFile = System.getenv(HADOOP_TOKEN_FILE_LOCATION);

      UserGroupInformation proxyUser =
          HadoopSecureWrapperUtils.setupProxyUser(_jobProps, tokenFile, _logger);

      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          copyHdfsToTd();
          return null;
        }
      });
    } else {
      copyHdfsToTd();
    }
  }

  /**
   * If user provided password, it performs pre-processing such as drop error table, and truncate target table, if requested by user.
   */
  private void preprocess() {
    if (!_params.getPassword().isPresent()) {
      _logger.warn("Preprocess (drop error table, replace target table) is not supported if " + TdchConstants.TD_ENCRYPTED_CREDENTIAL_KEY + " is not provided.");
      return;
    }

    try (Connection conn = newConnection()) {
      JdbcCommands command = newTeradataCommands(conn);

      boolean isDropErrTable = Boolean.valueOf(_jobProps.getProperty(TdchConstants.DROP_ERROR_TABLE_KEY, Boolean.FALSE.toString()));
      if(isDropErrTable) {
        _logger.info("Trying to drop error table.");
        if (!_params.getTdErrorTableName().isPresent()) {
          _logger.warn("Won't drop error tables because it will be randomly decided by Teradata."); //Not making this fail to be backward compatible.
        } else {
          Optional<String> db = _params.getTdErrorDatabase();
          if (!db.isPresent()) {
            db = _params.getTargetTdDatabase();
          }

          for (String suffix : ERR_TABLE_SUFFIXES) {
            String tableToDrop = _params.getTdErrorTableName().get() + suffix;

            if (command.doesExist(tableToDrop, db)) {
              _logger.info("Dropping error table " + tableToDrop + " at database " + db);
              command.dropTable(tableToDrop, db);
            }
          }
        }
      }

      boolean isReplaceTargetTable =
          Boolean.valueOf(_jobProps.getProperty(TdchConstants.REPLACE_TARGET_TABLE_KEY, Boolean.FALSE.toString()));
      if (isReplaceTargetTable) {
        _logger.info("Deleting all data in table " + _params.getTargetTdTableName());
        command.truncateTable(_params.getTargetTdTableName(), _params.getTargetTdDatabase());
      }

      conn.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  Connection newConnection() {
    try {
      Class.forName(_params.getTdJdbcClassName());
      return DriverManager.getConnection(_params.getTdUrl(), _params.getUserName(), _params.getPassword().get());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  JdbcCommands newTeradataCommands(Connection conn) {
    return new TeradataCommands(conn);
  }

  /**
   * Calling TDCH to move data from HDFS to Teradata
   *
   * @param args
   */
  @VisibleForTesting
  void copyHdfsToTd() {
    _logger.info(String.format("Executing %s with params: %s", HdfsToTeradataJobRunnerMain.class.getSimpleName(), _params));
    TeradataExportTool.main(_params.toTdchParams());
  }

  /**
   * Entry point of job process.
   *
   * @param args
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    new HdfsToTeradataJobRunnerMain().run();
  }
}
