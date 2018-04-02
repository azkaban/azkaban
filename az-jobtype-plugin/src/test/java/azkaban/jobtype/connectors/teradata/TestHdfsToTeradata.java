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
package azkaban.jobtype.connectors.teradata;

import static org.mockito.Mockito.*;
import static azkaban.jobtype.connectors.teradata.TdchConstants.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import com.google.common.base.Optional;

import azkaban.crypto.Decryptions;
import azkaban.jobtype.connectors.jdbc.JdbcCommands;

public class TestHdfsToTeradata {

  private Properties properties;
  private Decryptions decryptions;

  @Before
  public void initialize() throws IOException {
    properties = new Properties();
    properties.put(TD_HOSTNAME_KEY, "test");
    properties.put(TD_USERID_KEY, "test");
    properties.put(TD_ENCRYPTED_CREDENTIAL_KEY, "test");
    properties.put(TD_CRYPTO_KEY_PATH_KEY, "test");
    properties.put(AVRO_SCHEMA_PATH_KEY, "test");
    properties.put(SOURCE_HDFS_PATH_KEY, "test");
    properties.put(LIB_JARS_KEY, "test");

    decryptions = mock(Decryptions.class);
    when(decryptions.decrypt(any(), any(), any())).thenReturn("password");
  }

  @Test
  public void testDropErrorTbl() throws FileNotFoundException, IOException, InterruptedException, SQLException {
    properties.put(TARGET_TD_TABLE_NAME_KEY, "db.target_table");
    properties.put(DROP_ERROR_TABLE_KEY, Boolean.toString(true));

    HdfsToTeradataJobRunnerMain job = spy(new HdfsToTeradataJobRunnerMain(properties, decryptions));

    Connection conn = mock(Connection.class);
    doReturn(conn).when(job).newConnection();

    JdbcCommands commands = mock(JdbcCommands.class);
    doReturn(commands).when(job).newTeradataCommands(any());
    doReturn(true).when(commands).doesExist(any(), any());

    doNothing().when(job).copyHdfsToTd();
    job.run();

    InOrder inOrder = inOrder(conn, commands);

    inOrder.verify(commands, times(1)).doesExist(any(), any());
    inOrder.verify(commands, times(1)).dropTable("target_table_ERR_1", Optional.of("db"));
    inOrder.verify(commands, times(1)).doesExist(any(), any());
    inOrder.verify(commands, times(1)).dropTable("target_table_ERR_2", Optional.of("db"));
    inOrder.verify(commands, never()).truncateTable(any(), any());
    inOrder.verify(conn, times(1)).close();
  }

  @Test
  public void skipDropErrorTbl() throws FileNotFoundException, IOException, InterruptedException, SQLException {
    properties.put(TARGET_TD_TABLE_NAME_KEY, "db.target_table");
    properties.put(DROP_ERROR_TABLE_KEY, Boolean.toString(false));

    HdfsToTeradataJobRunnerMain job = spy(new HdfsToTeradataJobRunnerMain(properties, decryptions));

    Connection conn = mock(Connection.class);
    doReturn(conn).when(job).newConnection();

    JdbcCommands commands = mock(JdbcCommands.class);
    doReturn(commands).when(job).newTeradataCommands(any());
    doReturn(true).when(commands).doesExist(any(), any());

    doNothing().when(job).copyHdfsToTd();
    job.run();

    InOrder inOrder = inOrder(conn, commands);

    inOrder.verify(commands, never()).doesExist(any(), any());
    inOrder.verify(commands, never()).dropTable("target_table_ERR_1", Optional.of("db"));
    inOrder.verify(commands, never()).truncateTable(any(), any());
    inOrder.verify(conn, times(1)).close();
  }

  @Test
  public void errorTblNotExist() throws FileNotFoundException, IOException, InterruptedException, SQLException {
    properties.put(TARGET_TD_TABLE_NAME_KEY, "db.target_table");
    properties.put(DROP_ERROR_TABLE_KEY, Boolean.toString(true));

    HdfsToTeradataJobRunnerMain job = spy(new HdfsToTeradataJobRunnerMain(properties, decryptions));

    Connection conn = mock(Connection.class);
    doReturn(conn).when(job).newConnection();

    JdbcCommands commands = mock(JdbcCommands.class);
    doReturn(commands).when(job).newTeradataCommands(any());
    doReturn(false).when(commands).doesExist(any(), any());

    doNothing().when(job).copyHdfsToTd();
    job.run();

    InOrder inOrder = inOrder(conn, commands);

    inOrder.verify(commands, times(2)).doesExist(any(), any());
    inOrder.verify(commands, never()).dropTable(any(), any());
    inOrder.verify(commands, never()).truncateTable(any(), any());
    inOrder.verify(conn, times(1)).close();
  }

  @Test
  public void truncateTable() throws FileNotFoundException, IOException, InterruptedException, SQLException {
    properties.put(TARGET_TD_TABLE_NAME_KEY, "db.target_table");
    properties.put(REPLACE_TARGET_TABLE_KEY, Boolean.toString(true));

    HdfsToTeradataJobRunnerMain job = spy(new HdfsToTeradataJobRunnerMain(properties, decryptions));

    Connection conn = mock(Connection.class);
    doReturn(conn).when(job).newConnection();

    JdbcCommands commands = mock(JdbcCommands.class);
    doReturn(commands).when(job).newTeradataCommands(any());
    doReturn(true).when(commands).doesExist(any(), any());

    doNothing().when(job).copyHdfsToTd();
    job.run();

    InOrder inOrder = inOrder(conn, commands);

    inOrder.verify(commands, never()).doesExist(any(), any());
    inOrder.verify(commands, never()).dropTable(any(), any());
    inOrder.verify(commands, times(1)).truncateTable("target_table", Optional.of("db"));
    inOrder.verify(conn, times(1)).close();
  }
}
