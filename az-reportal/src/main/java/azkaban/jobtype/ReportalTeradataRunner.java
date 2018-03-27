/*
 * Copyright 2012 LinkedIn Corp.
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
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;

import azkaban.reportal.util.CompositeException;

public class ReportalTeradataRunner extends ReportalAbstractRunner {

  public ReportalTeradataRunner(String jobName, Properties props) {
    super(props);
  }

  @Override
  protected void runReportal() throws Exception {
    System.out.println("Reportal Teradata: Setting up Teradata");
    List<Exception> exceptions = new ArrayList<Exception>();

    Class.forName("com.teradata.jdbc.TeraDriver");
    String connectionString =
        props.getString("reportal.teradata.connection.string", null);

    String user = props.getString("reportal.teradata.username", null);
    String pass = props.getString("reportal.teradata.password", null);

    Map<String,String> queryBandProperties = new HashMap<>();
    queryBandProperties.put("USER",proxyUser);
    queryBandProperties.put(CommonJobProperties.EXEC_ID,props.getString(CommonJobProperties.EXEC_ID));
    queryBandProperties.put(CommonJobProperties.PROJECT_NAME,props.getString(CommonJobProperties.PROJECT_NAME));
    queryBandProperties.put(CommonJobProperties.FLOW_ID,props.getString(CommonJobProperties.FLOW_ID));
    queryBandProperties.put(CommonJobProperties.JOB_ID,props.getString(CommonJobProperties.JOB_ID));
    String attemptUrl = props.getString(CommonJobProperties.ATTEMPT_LINK);
    queryBandProperties.put(CommonJobProperties.ATTEMPT_LINK,attemptUrl);
    URI attemptUri = new URI(attemptUrl);
    queryBandProperties.put("azkaban.server",attemptUri.getHost());

    if (user == null) {
      System.out.println("Reportal Teradata: Configuration incomplete");
      throw new RuntimeException(
          "The reportal.teradata.username variable was not defined.");
    }
    if (pass == null) {
      System.out.println("Reportal Teradata: Configuration incomplete");
      throw new RuntimeException(
          "The reportal.teradata.password variable was not defined.");
    }

    DataSource teraDataSource =
        new TeradataDataSource(connectionString, user, pass);
    Connection conn = teraDataSource.getConnection();

    String sqlQueries[] = cleanAndGetQueries(jobQuery, queryBandProperties);

    int numQueries = sqlQueries.length;

    for (int i = 0; i < numQueries; i++) {
      try {
        String queryLine = sqlQueries[i];

        // Only store results from the last statement
        if (i == numQueries - 1) {
          PreparedStatement stmt = prepareStatement(conn, queryLine);
          stmt.execute();
          ResultSet rs = stmt.getResultSet();
          outputQueryResult(rs, outputStream);
          stmt.close();
        } else {
          try {
            PreparedStatement stmt = prepareStatement(conn, queryLine);
            stmt.execute();
            stmt.close();
          } catch (NullPointerException e) {
            // An empty query (or comment) throws a NPE in JDBC. Yay!
            System.err
                .println("Caught NPE in execute call because report has a NOOP query: "
                    + queryLine);
          }
        }
      } catch (Exception e) {
        // Catch and continue. Delay exception throwing until we've run all
        // queries in this task.
        System.out.println("Reportal Teradata: SQL query failed. "
            + e.getMessage());
        e.printStackTrace();
        exceptions.add(e);
      }
    }

    if (exceptions.size() > 0) {
      throw new CompositeException(exceptions);
    }

    System.out.println("Reportal Teradata: Ended successfully");
  }

  protected String[] cleanAndGetQueries(String sqlQuery, Map<String, String> queryBandProperties) {

    /**
     * Teradata's SET Query_Band allows use to "proxy" to an LDAP user. This
     * makes queries appear to admins as though it's being issued by the owner
     * of the report, rather than the 'Reportal' user. Tables will still be
     * "owned" by Reportal, but admins will be able to send angry emails to the
     * proper user when a reportal query is impacting the system negatively.
     * Best we could do.
     */

    StringBuilder queryBandBuilder = new StringBuilder();
    for(Map.Entry<String,String> pair : queryBandProperties.entrySet()){
      queryBandBuilder.append(""+pair.getKey()+"=" + pair.getValue() + ";");
    }

    String queryBand =
        "SET Query_Band = '" + queryBandBuilder.toString()
            + "' FOR SESSION;";
    ArrayList<String> injectedQueries = new ArrayList<String>();

    injectedQueries.add(queryBand);
    String[] queries = StringUtils.split(sqlQuery.trim(), ";");
    for (String query : queries) {
      query = cleanQueryLine(query);
      if (query == null || query.isEmpty()) {
        continue;
      }
      injectedQueries.add(query);
    }

    return injectedQueries.toArray(new String[] {});
  }

  private String cleanQueryLine(String line) {
    if (line != null) {
      return line.trim();
    }
    return null;
  }

  private void outputQueryResult(ResultSet result, OutputStream outputStream)
      throws SQLException {
    final PrintStream outFile = new PrintStream(outputStream);
    final String delim = ",";
    boolean isHeaderPending = true;
    if (result != null) {
      while (result.next()) {
        int numColumns = result.getMetaData().getColumnCount();
        StringBuilder dataString = new StringBuilder();

        if (isHeaderPending) {
          StringBuilder headerString = new StringBuilder();
          for (int j = 1; j <= numColumns; j++) {
            String colName = formatValue(result.getMetaData().getColumnName(j));
            if (j > 1) {
              headerString.append(delim).append(colName);
            } else {
              headerString.append(colName);
            }
          }
          isHeaderPending = false;
          outFile.println(headerString.toString());
        }

        for (int j = 1; j <= numColumns; j++) {
          String colVal = result.getString(j);

          if (colVal == null) {
            colVal = "\"null\"";
          } else {
            colVal = formatValue(colVal);
          }

          if (j > 1) {
            dataString.append(delim).append(colVal);
          } else {
            dataString.append(colVal);
          }
        }

        outFile.println(dataString.toString());
      }
    }
    outFile.close();
  }

  private String formatValue(String value) {
    return "\"" + value.replace("\"", "") + "\"";
  }

  private PreparedStatement prepareStatement(Connection conn, String line)
      throws SQLException {
    line = injectVariables(line);

    // For some reason, teradata's adapter can't seem to handle this well
    // List<String> variableReplacements = new ArrayList<String>();
    //
    // for (Entry<String, String> entry : variables.entrySet()) {
    // String key = ":" + entry.getKey();
    // int index;
    // while ((index = line.indexOf(key)) != -1) {
    // line = line.substring(0, index) + "?" + line.substring(index +
    // key.length());
    // variableReplacements.add(entry.getValue());
    // }
    // }

    // StringBuilder sb = new StringBuilder();
    PreparedStatement stmt = conn.prepareStatement(line);
    // for (int i = 0; i < variableReplacements.size(); i++) {
    // stmt.setString(i + 1, variableReplacements.get(i));
    // sb.append(variableReplacements.get(i)).append(",");
    // }

    System.out.println("Reportal Teradata: Teradata query: " + line);
    // System.out.println("Reportal Teradata: Variables: " + sb.toString());
    return stmt;
  }

  private static class TeradataDataSource extends BasicDataSource {
    private TeradataDataSource(String connectionString, String user,
        String password) {
      super();
      setDriverClassName("com.teradata.jdbc.TeraDriver");
      setUrl(connectionString);
      setUsername(user);
      setPassword(password);
    }
  }
}
