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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import azkaban.jobtype.connectors.jdbc.TeradataCommands;
import azkaban.jobtype.javautils.FileUtils;
import azkaban.jobtype.javautils.ValidationUtils;

public class TdchParameters {
  private final static Logger _logger = Logger.getLogger(TdchParameters.class);
  private final static String TERADATA_JDBC_URL_PREFIX = "jdbc:teradata://";
  private final static String TERADATA_JDBC_URL_CHARSET_KEY = "/CHARSET=";
  private final static String DEFAULT_CHARSET = "UTF8";
  private static final String DEFAULT_RETRIEVE_METHOD = "split.by.amp";
  private static final int ERROR_TABLE_NAME_LENGTH_LIMIT = 24;

  private final List<String> _mrParams;
  private final String _libJars;
  private final String _tdJdbcClassName;
  private final String _tdUrl;
  private final String _fileFormat;
  private final Optional<String> _fieldSeparator;
  private final String _jobType;
  private final String _userName;
  private final Optional<String> _credentialName;
  private final Optional<String> _password;
  private final Optional<String> _avroSchemaPath;
  private final Optional<String> _avroSchemaInline;
  private final String _numMappers;
  private final Optional<Config> _otherProperties;

  private final TdchType _tdchType;

  //From HDFS to Teradata
  private final String _sourceHdfsPath;
  private final String _targetTdTableName;
  private final Optional<String> _targetTdDatabaseName;

  private final String _hiveSourceDatabase;
  private final String _hiveSourceTable;
  private final Optional<String> _hiveConfFile;

  private final Optional<String> _tdErrorDatabase;
  private final Optional<String> _tdErrorTableName;
  private final Optional<String> _tdInsertMethod;

  //From Teradata to HDFS
  private final Optional<String> _sourceQuery;
  private final Optional<String> _sourceTdTableName;
  private final Optional<String> _tdRetrieveMethod;
  private final String _targetHdfsPath;

  public static Builder builder() {
    return new Builder();
  }

  private TdchParameters(Builder builder) {
    this._mrParams = builder._mrParams;
    this._libJars = builder._libJars;
    this._tdJdbcClassName = builder._tdJdbcClassName;
    this._tdUrl = builder._tdUrl;
    this._fileFormat = builder._fileFormat;
    this._fieldSeparator = Optional.fromNullable(builder._fieldSeparator);

    this._jobType = builder._jobType;
    this._userName = builder._userName;
    this._credentialName = Optional.fromNullable(builder._credentialName);
    this._password = Optional.fromNullable(builder._password);

    this._avroSchemaPath = Optional.fromNullable(builder._avroSchemaPath);
    this._avroSchemaInline = Optional.fromNullable(builder._avroSchemaInline);

    this._numMappers = Integer.toString(builder._numMappers);
    this._otherProperties = Optional.fromNullable(builder._otherProperties);
    this._tdchType = builder._tdchType;

    this._sourceHdfsPath = builder._sourceHdfsPath;
    this._targetTdTableName = builder._targetTdTableName;
    this._targetTdDatabaseName = Optional.fromNullable(builder._targetTdDatabaseName);
    this._tdErrorDatabase = Optional.fromNullable(builder._tdErrorDatabase);
    this._tdErrorTableName = Optional.fromNullable(builder._tdErrorTableName);
    this._tdInsertMethod = Optional.fromNullable(builder._tdInsertMethod);  //Default by TDCH is batch.insert

    this._sourceQuery = Optional.fromNullable(builder._sourceQuery);
    this._sourceTdTableName = Optional.fromNullable(builder._sourceTdTableName);
    this._targetHdfsPath = builder._targetHdfsPath;
    this._tdRetrieveMethod = Optional.fromNullable(builder._tdRetrieveMethod);

    this._hiveSourceDatabase = builder._hiveSourceDatabase;
    this._hiveSourceTable = builder._hiveSourceTable;
    this._hiveConfFile = Optional.fromNullable(builder._hiveconfFile);
  }

  private enum TdchType {
    HDFS_TO_TERADATA,
    HIVE_TO_TERADATA,
    TERADATA_TO_HDFS
  }

  public static class Builder {
    private List<String> _mrParams;
    private String _libJars;
    private String _tdJdbcClassName;
    private String _tdHostName;
    private String _tdCharSet;
    private String _tdUrl;
    private String _fileFormat;
    private String _fieldSeparator;
    private String _jobType;
    private String _userName;
    private String _credentialName;
    private String _password;
    private String _avroSchemaPath;
    private String _avroSchemaInline;
    private int _numMappers;
    private Config _otherProperties;

    private TdchType _tdchType;

    private String _sourceHdfsPath;
    private String _targetTdDatabaseName;
    private String _targetTdTableName;
    private String _tdErrorDatabase;
    private String _tdErrorTableName;
    private String _tdInsertMethod;

    private String _sourceQuery;
    private String _sourceTdTableName;
    private String _targetHdfsPath;
    private String _tdRetrieveMethod;

    private String _hiveSourceDatabase;
    private String _hiveSourceTable;
    private String _hiveconfFile;

    public Builder mrParams(Collection<String> mrParams) {
      this._mrParams = ImmutableList.<String>builder().addAll(mrParams).build();
      return this;
    }

    public Builder libJars(String libJars) {
      Collection<String> filePaths = FileUtils.listFiles(libJars, TdchConstants.LIB_JAR_DELIMITER);
      this._libJars = Joiner.on(TdchConstants.LIB_JAR_DELIMITER).skipNulls().join(filePaths);
      return this;
    }

    public Builder tdJdbcClassName(String tdJdbcClassName) {
      this._tdJdbcClassName = tdJdbcClassName;
      return this;
    }

    public Builder teradataHostname(String hostname) {
      this._tdHostName = hostname;
      return this;
    }

    public Builder teradataCharset(String charSet) {
      this._tdCharSet = charSet;
      return this;
    }

    public Builder fileFormat(String fileFormat) {
      this._fileFormat = fileFormat;
      return this;
    }

    public Builder fieldSeparator(String fieldSeparator) {
      this._fieldSeparator = fieldSeparator;
      return this;
    }

    public Builder jobType(String jobType) {
      this._jobType = jobType;
      return this;
    }

    public Builder tdInsertMethod(String tdInsertMethod) {
      this._tdInsertMethod = tdInsertMethod;
      return this;
    }

    public Builder userName(String userName) {
      this._userName = userName;
      return this;
    }

    @Deprecated
    public Builder credentialName(String credentialName) {
      this._credentialName = credentialName;
      return this;
    }

    public Builder password(String password) {
      this._password = password;
      return this;
    }

    public Builder avroSchemaPath(String avroSchemaPath) {
      this._avroSchemaPath = avroSchemaPath;
      return this;
    }

    public Builder avroSchemaInline(String avroSchemaInline) {
      this._avroSchemaInline = avroSchemaInline;
      return this;
    }

    public Builder numMapper(int numMappers) {
      this._numMappers = numMappers;
      return this;
    }

    /**
     * Takes HOCON notation: https://github.com/typesafehub/config/blob/master/HOCON.md
     * To override TDCH parameter, add "key1=value1,key2=value2" at hoconInput where key should be the one
     * supported by TDCH itself.
     * To remove TDCH parameter, add key="" into hoconInput.
     *
     * Override and removal provides total control the final input parameter that TDCH receives.
     *
     * @param hoconInput
     * @return
     */
    public Builder otherProperties(String hoconInput) {
      if (StringUtils.isEmpty(hoconInput)) {
        return this;
      }
      this._otherProperties = ConfigFactory.parseString(hoconInput);
      return this;
    }

    public Builder sourceHdfsPath(String sourceHdfsPath) {
      this._sourceHdfsPath = sourceHdfsPath;
      return this;
    }

    public Builder targetTdTableName(String targetTdTableName) {
      DatabaseTable dbTbl = new DatabaseTable(targetTdTableName);
      this._targetTdTableName = dbTbl.table;
      this._targetTdDatabaseName = dbTbl.database.orNull();
      return this;
    }

    public Builder errorTdDatabase(String tdErrorDatabase) {
      this._tdErrorDatabase = tdErrorDatabase;
      return this;
    }

    public Builder errorTdTableName(String errorTdTableName) {
      this._tdErrorTableName = errorTdTableName;
      return this;
    }

    public Builder sourceQuery(String sourceQuery) {
      this._sourceQuery = sourceQuery;
      return this;
    }

    public Builder sourceTdTableName(String sourceTdTableName) {
      this._sourceTdTableName = sourceTdTableName;
      return this;
    }

    public Builder targetHdfsPath(String targetHdfsPath) {
      this._targetHdfsPath = targetHdfsPath;
      return this;
    }

    public Builder tdRetrieveMethod(String tdRetrieveMethod) {
      this._tdRetrieveMethod = tdRetrieveMethod;
      return this;
    }

    public Builder hiveSourceDatabase(String hiveSourceDatabase) {
      this._hiveSourceDatabase = hiveSourceDatabase;
      return this;
    }

    public Builder hiveSourceTable(String hiveSourceTable) {
      this._hiveSourceTable = hiveSourceTable;
      return this;
    }

    public Builder hiveConfFile(String hiveConfFile) {
      this._hiveconfFile = hiveConfFile;
      return this;
    }

    public TdchParameters build() {
      validate();
      if (TdchType.HDFS_TO_TERADATA.equals(_tdchType)
          || TdchType.HIVE_TO_TERADATA.equals(_tdchType)) {
        assignErrorTbl();
      }
      return new TdchParameters(this);
    }

    /**
     * Unless error table name is specified, it will use target table name as prefix of error table where TDCH will add suffix into it.
     * If target table name is too long, it will leave error table as blank so that TDCH will choose random error table name.
     */
    private void assignErrorTbl() {
      if (!StringUtils.isEmpty(_tdErrorTableName)) {
        return;
      }

      if (_targetTdTableName.length() <= ERROR_TABLE_NAME_LENGTH_LIMIT) {
        _tdErrorTableName = _targetTdTableName; //TDCH will add suffix into it.
      } else {
        _logger.info("Error table will be randomly decided by Teradata because " + TdchConstants.ERROR_TABLE_KEY
            + " is not defined and " + TdchConstants.TARGET_TD_TABLE_NAME_KEY + " is longer than "
            + ERROR_TABLE_NAME_LENGTH_LIMIT
            + " so that it cannot be used as a prefix of the error table. Please specify "
            + TdchConstants.ERROR_TABLE_KEY);
      }
    }

    private void validate() {
      ValidationUtils.validateNotEmpty(_tdJdbcClassName, "tdJdbcClassName");
      ValidationUtils.validateNotEmpty(_jobType, "jobType");
      ValidationUtils.validateNotEmpty(_userName, "userName");
      ValidationUtils.validateNotEmpty(_tdHostName, "teradata host name");

      if (!StringUtils.isEmpty(_credentialName) && !StringUtils.isEmpty(_password)) {
        throw new IllegalArgumentException("Please use either credential name or password, not all of them.");
      }

      if (StringUtils.isEmpty(_credentialName) && StringUtils.isEmpty(_password)) {
        throw new IllegalArgumentException("Password is required.");
      }

      if(StringUtils.isEmpty(_fileFormat)) {
        _fileFormat = TdchConstants.AVRO_FILE_FORMAT;
      } else {
        ValidationUtils.validateNotEmpty(_fileFormat, "fileFormat");
      }

      if(TdchConstants.AVRO_FILE_FORMAT.equals(_fileFormat)) {
        //Validate the existence of avro schema, but confirm only one of them exist.
        if(StringUtils.isEmpty(_avroSchemaPath) && StringUtils.isEmpty(_avroSchemaInline)) {
          throw new IllegalArgumentException("Either " + TdchConstants.AVRO_SCHEMA_PATH_KEY + " or "
                                             + TdchConstants.AVRO_SCHEMA_INLINE_KEY + " should be provided");
        }

        if(!StringUtils.isEmpty(_avroSchemaPath) && !StringUtils.isEmpty(_avroSchemaInline)) {
          throw new IllegalArgumentException("Only one of " + TdchConstants.AVRO_SCHEMA_PATH_KEY + " and "
                                             + TdchConstants.AVRO_SCHEMA_INLINE_KEY + " should be provided");
        }
      }

      if(_numMappers <= 0) {
        throw new IllegalArgumentException("Number of mappers needs to be defined and has to be greater than 0.");
      }

      String charSet = StringUtils.isEmpty(_tdCharSet) ? DEFAULT_CHARSET : _tdCharSet;
      _tdUrl = TERADATA_JDBC_URL_PREFIX + _tdHostName + TERADATA_JDBC_URL_CHARSET_KEY + charSet;

      validateJobtype();

      if (!StringUtils.isEmpty(_tdErrorTableName)) {
        Preconditions.checkArgument(!new DatabaseTable(_tdErrorTableName).database.isPresent(),
                                    "Error table name cannot have database prefix. Use " + TdchConstants.ERROR_DB_KEY);
        Preconditions.checkArgument(_tdErrorTableName.length() <= ERROR_TABLE_NAME_LENGTH_LIMIT,
            "Error table name cannot exceed " + ERROR_TABLE_NAME_LENGTH_LIMIT + " chracters.");
      }
    }

    private void validateJobtype() {
      if ("hdfs".equals(_jobType)) {
        validateHdfsJobtype();
      } else if ("hive".equals(_jobType)) {
        validateHiveJobtype();
      } else {
        throw new IllegalArgumentException("Job type " + _jobType + " is not supported");
      }

    }

    private void validateHiveJobtype() {
      ValidationUtils.validateNotEmpty(_targetTdTableName, "targetTdTableName");
      ValidationUtils.validateNotEmpty(_hiveSourceDatabase, "hiveSourceDatabase");
      ValidationUtils.validateNotEmpty(_hiveSourceTable, "hiveSourceTable");
      Preconditions.checkArgument(StringUtils.isEmpty(_sourceHdfsPath), "sourceHdfsPath should be empty for hive job.");
      _tdchType = TdchType.HIVE_TO_TERADATA;
    }

    private void validateHdfsJobtype() {
      boolean isHdfsToTd = !StringUtils.isEmpty(_sourceHdfsPath) && !StringUtils.isEmpty(_targetTdTableName);
      boolean isTdToHdfs = !(StringUtils.isEmpty(_sourceTdTableName) && StringUtils.isEmpty(_sourceQuery))
                           && !StringUtils.isEmpty(_targetHdfsPath);

      if(!(isHdfsToTd || isTdToHdfs)) {
        throw new IllegalArgumentException("Source and target are not defined. " + this);
      }

      if(isHdfsToTd && isTdToHdfs) {
        throw new IllegalArgumentException("Cannot choose multiple source and multiple target. " + this);
      }

      if(isHdfsToTd) {
        ValidationUtils.validateNotEmpty(_sourceHdfsPath, "tdInsertMethod");
        ValidationUtils.validateNotEmpty(_targetTdTableName, "tdInsertMethod");
      }

      if(isTdToHdfs) {
        if(!StringUtils.isEmpty(_sourceTdTableName) && !StringUtils.isEmpty(_sourceQuery)) {
          throw new IllegalArgumentException("Cannot choose multiple source");
        }
      }

      if(isHdfsToTd) {
        _tdchType = TdchType.HDFS_TO_TERADATA;
      } else {
        _tdchType = TdchType.TERADATA_TO_HDFS;
      }
    }
  }

  public String[] toTdchParams() {
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    if(_mrParams != null && !_mrParams.isEmpty()) {
      listBuilder.addAll(_mrParams);
    }

    Map<String, String> keyValParams = buildKeyValParams();
    for(Map.Entry<String, String> entry : keyValParams.entrySet()) {
      listBuilder.add(entry.getKey()).add(entry.getValue());
    }

    List<String> paramList = listBuilder.build();
    String[] params = paramList.toArray(new String[paramList.size()]);
    return params;
  }

  private Map<String, String> buildKeyValParams() {
    Map<String, String> map = new LinkedHashMap<>();

    if(!StringUtils.isEmpty(_libJars)) {
      map.put("-libjars", _libJars);
    }

    map.put("-url", _tdUrl);
    map.put("-classname", _tdJdbcClassName);
    map.put("-fileformat", _fileFormat);
    map.put("-jobtype", _jobType);
    map.put("-username", _userName);
    map.put("-nummappers", _numMappers);

    if(_password.isPresent()) {
      map.put("-password", _password.get());
    } else {
      _logger.warn(TdchConstants.TD_CREDENTIAL_NAME_KEY + " is deprecated. Please use " + TdchConstants.TD_ENCRYPTED_CREDENTIAL_KEY);
      map.put("-password", String.format(TdchConstants.TD_WALLET_FORMAT, _credentialName.get()));
    }

    if(_avroSchemaPath.isPresent()) {
      map.put("-avroschemafile", _avroSchemaPath.get());
    }

    if(_avroSchemaInline.isPresent()) {
      map.put("-avroschema", _avroSchemaInline.get());
    }

    if(_fieldSeparator.isPresent()) {
      map.put("-separator", _fieldSeparator.get());
    }

    if(TdchType.HDFS_TO_TERADATA.equals(_tdchType) || TdchType.HIVE_TO_TERADATA.equals(_tdchType)) {
      if(_targetTdDatabaseName.isPresent()) {
        map.put("-targettable", String.format(TeradataCommands.DATABASE_TABLE_FORMAT,
                                              _targetTdDatabaseName.get(), _targetTdTableName));
      } else {
        map.put("-targettable", _targetTdTableName);
      }

      if(_tdErrorDatabase.isPresent()) {
        map.put("-errortabledatabase", _tdErrorDatabase.get());
      }

      if(_tdErrorTableName.isPresent()) {
        map.put("-errortablename", _tdErrorTableName.get());
      }

      if(_tdInsertMethod.isPresent()) {
        map.put("-method", _tdInsertMethod.get());
      }

      if (TdchType.HDFS_TO_TERADATA.equals(_tdchType)) {
        map.put("-sourcepaths", _sourceHdfsPath);
      } else {
        map.put("-sourcedatabase", _hiveSourceDatabase);
        map.put("-sourcetable", _hiveSourceTable);
        if (_hiveConfFile.isPresent()) {
          map.put("-hiveconf", _hiveConfFile.get());
        }
      }
    } else if (TdchType.TERADATA_TO_HDFS.equals(_tdchType)){
      map.put("-targetpaths",_targetHdfsPath);

      if(_sourceTdTableName.isPresent()) {
        map.put("-sourcetable", _sourceTdTableName.get());

        if (_tdRetrieveMethod.isPresent()) {
          map.put("-method", _tdRetrieveMethod.get());
        } else {
          map.put("-method", DEFAULT_RETRIEVE_METHOD);
        }

      } else if (_sourceQuery.isPresent()) {
        map.put("-sourcequery", _sourceQuery.get());
      } else {
        throw new IllegalArgumentException("No source defined."); //This should not happen as it shouldn't have been instantiated by builder.
      }
    } else {
      throw new UnsupportedOperationException("Unsupported TDCH type: " + _tdchType);
    }

    if(_otherProperties.isPresent()) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(_otherProperties.get().root().render(ConfigRenderOptions.concise()));

        Iterator<Map.Entry<String, JsonNode>> it = json.fields();
        while (it.hasNext()) {
          Map.Entry<String, JsonNode> entry = it.next();
          String key = "-" + entry.getKey();
          if (map.containsKey(key)) {
            _logger.info("Duplicate entry detected on key: " + entry.getKey()
                         + " . Overwriting value from " + map.get(key) + " to " + entry.getValue().asText());
          }
          if (StringUtils.isEmpty(entry.getValue().asText())) {
            map.remove(key);
          } else {
            map.put(key, entry.getValue().asText());
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return map;
  }

  private String getMaskedVal(Optional<String> val) {
    if(!val.isPresent()) {
      return null;
    }

    StringBuilder maskedVal = new StringBuilder(val.get().length());
    for (int i = 0; i < val.get().length(); i++) {
      maskedVal.append("*");
    }
    return maskedVal.toString();
  }

  public String getTdJdbcClassName() {
    return _tdJdbcClassName;
  }

  public String getTdUrl() {
    return _tdUrl;
  }

  public String getUserName() {
    return _userName;
  }

  public Optional<String> getPassword() {
    return _password;
  }


  public Optional<String> getTargetTdDatabase() {
    return _targetTdDatabaseName;
  }

  public String getTargetTdTableName() {
    return _targetTdTableName;
  }

  public Optional<String> getTdErrorTableName() {
    return _tdErrorTableName;
  }

  public Optional<String> getTdErrorDatabase() {
    return _tdErrorDatabase;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("TdchParameters [_mrParams=").append(_mrParams)
            .append(", _libJars=").append(_libJars)
            .append(", _tdJdbcClassName=").append(_tdJdbcClassName)
            .append(", _tdUrl=").append(_tdUrl)
            .append(", _fileFormat=").append(_fileFormat)
            .append(", _fieldSeparator=").append(_fieldSeparator)
            .append(", _jobType=").append(_jobType)
            .append(", _userName=").append(_userName)
            .append(", _credentialName=").append(getMaskedVal(_credentialName))
            .append(", _password=").append(getMaskedVal(_password))
            .append(", _avroSchemaPath=").append(_avroSchemaPath)
            .append(", _avroSchemaInline=").append(_avroSchemaInline)
            .append(", _numMappers=").append(_numMappers)
            .append(", _tdchType=").append(_tdchType)
            .append(", _sourceHdfsPath=").append(_sourceHdfsPath)
            .append(", _targetTdDatabaseName=").append(_targetTdDatabaseName)
            .append(", _targetTdTableName=").append(_targetTdTableName)
            .append(", _tdErrorDatabase=").append(_tdErrorDatabase)
            .append(", _tdErrorTableName=").append(_tdErrorTableName)
            .append(", _tdInsertMethod=").append(_tdInsertMethod)
            .append(", _sourceQuery=").append(_sourceQuery)
            .append(", _sourceTdTableName=").append(_sourceTdTableName)
            .append(", _tdRetrieveMethod=").append(_tdRetrieveMethod)
            .append(", _targetHdfsPath=").append(_targetHdfsPath)
            .append(", _hiveSourceDatabase=").append(_hiveSourceDatabase)
            .append(", _hiveSourceTable=").append(_hiveSourceTable)
            .append(", _hiveConfFile=").append(_hiveConfFile)
            .append(", _otherProperties").append(_otherProperties)
            .append("]");
    return builder.toString();
  }

  static class DatabaseTable {
    private final Optional<String> database;
    private final String table;

    public DatabaseTable(String dbTblStr) {
      int idx = dbTblStr.indexOf(".");
      if (idx < 0) {
        this.table = dbTblStr;
        this.database = Optional.absent();
        return;
      }

      this.database = Optional.of(dbTblStr.substring(0, idx));
      this.table = dbTblStr.substring(idx + 1);
    }

    Optional<String> getDatabase() {
      return database;
    }

    String getTable() {
      return table;
    }

    @Override
    public String toString() {
      return "DatabaseTable [database=" + database + ", table=" + table + "]";
    }
  }
}
