/*
 * Copyright (C) 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package jobtype.connectors.teradata;

import static azkaban.jobtype.connectors.teradata.TdchConstants.*;

import azkaban.jobtype.connectors.teradata.TdchConstants;
import azkaban.jobtype.connectors.teradata.TdchParameters;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import azkaban.jobtype.connectors.teradata.TdchParameters.Builder;

//@Ignore
public class TestTdchParameters {
  private static String PASSWORD = "password";
  private Builder builder;

  @Before
  public void setup() {
    builder = TdchParameters.builder()
                            .mrParams(Collections.emptyList())
                            .tdJdbcClassName(TdchConstants.TERADATA_JDBCDRIVER_CLASSNAME)
                            .teradataHostname("teradataHostname")
                            .fileFormat(AVRO_FILE_FORMAT)
                            .jobType(TdchConstants.DEFAULT_TDCH_JOB_TYPE)
                            .userName("userName")
                            .avroSchemaPath("avroSchemaPath")
                            .sourceHdfsPath("sourceHdfsPath")
                            .numMapper(TdchConstants.DEFAULT_NO_MAPPERS);
  }

  @Test
  public void testToTdchParam() {
    String targetTableName = "db.target";
    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .password(PASSWORD)
                                   .build();

    List<String> expected = getExpectedTdchParams();

    assertEqual(expected, Arrays.asList(params.toTdchParams()));
  }

  @Test
  public void testHiveToTdchParam() {
    builder = TdchParameters.builder()
        .mrParams(Collections.emptyList())
        .tdJdbcClassName(TdchConstants.TERADATA_JDBCDRIVER_CLASSNAME)
        .teradataHostname("teradataHostname")
        .fileFormat("orcfile")
        .jobType(TdchConstants.TDCH_HIVE_JOB_TYPE)
        .userName("userName")
        .hiveSourceDatabase("hive_database")
        .hiveSourceTable("hive_table")
        .hiveConfFile("hive_conf")
        .numMapper(TdchConstants.DEFAULT_NO_MAPPERS);

    String targetTableName = "db.target";
    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .password(PASSWORD)
                                   .build();

    List<String> expected = getExpectedTdchParams("-avroschemafile", "-sourcepaths", "-jobtype", "-fileformat");
    expected.add("-sourcedatabase");
    expected.add("hive_database");
    expected.add("-sourcetable");
    expected.add("hive_table");
    expected.add("-hiveconf");
    expected.add("hive_conf");
    expected.add("-jobtype");
    expected.add("hive");
    expected.add("-fileformat");
    expected.add("orcfile");

    assertEqual(expected, Arrays.asList(params.toTdchParams()));
  }

  private List<String> getExpectedTdchParams(String... exceptKeys) {
    List<String> expected = Lists.newArrayList("-url" ,
                                               "jdbc:teradata://teradataHostname/CHARSET=UTF8",
                                               "-classname",
                                               "com.teradata.jdbc.TeraDriver",
                                               "-fileformat",
                                               "avrofile",
                                               "-jobtype",
                                               "hdfs",
                                               "-username",
                                               "userName",
                                               "-nummappers",
                                               Integer.toString(DEFAULT_NO_MAPPERS),
                                               "-password",
                                               "password",
                                               "-avroschemafile",
                                               "avroSchemaPath",
                                               "-sourcepaths",
                                               "sourceHdfsPath",
                                               "-targettable",
                                               "db.target",
                                               "-errortablename",
                                               "target");

    if (exceptKeys != null) {
      Set<String> removalKeys = Sets.newHashSet(exceptKeys);
      Iterator<String> it = expected.iterator();
      expected = Lists.newArrayList();
      while (it.hasNext()) {
        String s = it.next();
        if (removalKeys.contains(s)) {
          it.next();
          continue;
        }
        expected.add(s);
      }
    }
    return expected;
  }

  @Test
  public void testOtherParams() {
    String targetTableName = "db.target";
    String otherParams = "testKey1=testVal1,nummappers=24"; //nummappers is already assigned, thus it should be ignored.

    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .password(PASSWORD)
                                   .otherProperties(otherParams)
                                   .build();

    List<String> expected = getExpectedTdchParams("-nummappers");
    expected = ImmutableList.<String>builder().addAll(expected)
                                              .add("-testKey1")
                                              .add("testVal1")
                                              .add("-nummappers")
                                              .add("24")
                                              .build();

    assertEqual(expected, Arrays.asList(params.toTdchParams()));
  }

  @Test
  public void testOtherParamsWithDuplicateKey() {
    String targetTableName = "db.target";
    String otherParams = "testKey1=testVal1,testKey2=testVal2";

    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .password(PASSWORD)
                                   .otherProperties(otherParams)
                                   .build();

    List<String> expected = getExpectedTdchParams();
    expected = ImmutableList.<String>builder().addAll(expected)
                                              .add("-testKey1")
                                              .add("testVal1")
                                              .add("-testKey2")
                                              .add("testVal2")
                                              .build();

    assertEqual(expected, Arrays.asList(params.toTdchParams()));
  }

  @Test
  public void testOtherParamsForRemoval() {
    String targetTableName = "db.target";
    String otherParams = "testKey1=testVal1,testKey2=testVal2,errortablename=\"\"";

    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .password(PASSWORD)
                                   .otherProperties(otherParams)
                                   .build();

    List<String> expected = getExpectedTdchParams("-errortablename");
    expected = ImmutableList.<String>builder().addAll(expected)
                                              .add("-testKey1")
                                              .add("testVal1")
                                              .add("-testKey2")
                                              .add("testVal2")
                                              .build();

    assertEqual(expected, Arrays.asList(params.toTdchParams()));
  }

  @Test
  public void testErrorTblDerivedFromTargetTbl() {
    String targetTableName = "db.target";
    String expected = "target";

    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .password(PASSWORD)
                                   .build();

    Assert.assertEquals(expected, params.getTdErrorTableName().get());
  }

  @Test
  public void testErrorTblFromInput() {
    String targetTableName = "db.target";
    String expected = "errTbl";

    TdchParameters params = builder.targetTdTableName(targetTableName)
                                   .errorTdTableName(expected)
                                   .password(PASSWORD)
                                   .build();

    Assert.assertEquals(expected, params.getTdErrorTableName().get());
  }

  @Test
  public void testLongErrorTblInput() {
    String targetTblName = "db.target";
    String errTblName = "errTbl_errTbl_errTbl_errTbl_errTbl_errTbl_errTbl_errTbl_errTbl_errTbl";

    try {
      builder.targetTdTableName(targetTblName)
             .errorTdTableName(errTblName)
             .password(PASSWORD)
             .build();
      Assert.fail("Should have failed with long error table name");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
      Assert.assertEquals("Error table name cannot exceed 24 chracters.", e.getMessage());
    }
  }

  @Test
  public void testFailWithoutCredential() {
    try {
      builder.build();
      Assert.fail("Should have failed with no credentials");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
      Assert.assertEquals("Password is required.", e.getMessage());
    }
  }

  @Test
  public void testFailWithMultipleCredentials() {
    try {
      builder.password(PASSWORD)
             .credentialName("test")
             .build();
      Assert.fail("Should have failed with more than one credentials");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
      Assert.assertEquals("Please use either credential name or password, not all of them.", e.getMessage());
    }
  }

  private void assertEqual(List<String> expected, List<String> actual) {
    Assert.assertTrue("Should not be null. Expected: " + expected + " , actual: " + actual, expected != null && actual != null);
    Assert.assertTrue("Expected: " + expected + " , actual: " + actual, expected.size() == actual.size() && expected.containsAll(actual));
  }
}
