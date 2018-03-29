/*
 * Copyright 2018 LinkedIn Corp.
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
import azkaban.reportal.util.CompositeException;
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

public class ReportalTeradataRunner extends ReportalAbstractRunner {

  public ReportalTeradataRunner(final String jobName, final Properties props) {
    super(props);
  }

  @Override
  protected void runReportal() throws Exception {
    System.out.println("Reportal Teradata: Setting up Teradata");
    final List<Exception> exceptions = new ArrayList<>();

    Class.forName("com.teradata.jdbc.TeraDriver");
    final String connectionString =
        this.props.getString("reportal.teradata.connection.string", null);

    final String user = this.props.getString("reportal.teradata.username", null);
    final String pass = this.props.getString("reportal.teradata.password", null);

    final Map<String, String> queryBandProperties = new HashMap<>();
    queryBandProperties.put("USER", this.proxyUser);
    queryBandProperties
        .put(CommonJobProperties.EXEC_ID, this.props.getString(CommonJobProperties.EXEC_ID));
    queryBandProperties
        .put(CommonJobProperties.PROJECT_NAME,
            this.props.getString(CommonJobProperties.PROJECT_NAME));
    queryBandProperties
        .put(CommonJobProperties.FLOW_ID, this.props.getString(CommonJobProperties.FLOW_ID));
    queryBandProperties
        .put(CommonJobProperties.JOB_ID, this.props.getString(CommonJobProperties.JOB_ID));
    final String attemptUrl = this.props.getString(CommonJobProperties.ATTEMPT_LINK);
    queryBandProperties.put(CommonJobProperties.ATTEMPT_LINK, attemptUrl);
    final URI attemptUri = new URI(attemptUrl);
    queryBandProperties.put("azkaban.server", attemptUri.getHost());

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

    final DataSource teraDataSource =
        new TeradataDataSource(connectionString, user, pass);
    final Connection conn = teraDataSource.getConnection();

    final String[] sqlQueries = cleanAndGetQueries(this.jobQuery, queryBandProperties);

    final int numQueries = sqlQueries.length;

    for (int i = 0; i < numQueries; i++) {
      try {
        final String queryLine = sqlQueries[i];

        // Only store results from the last statement
        if (i == numQueries - 1) {
          final PreparedStatement stmt = prepareStatement(conn, queryLine);
          stmt.execute();
          final ResultSet rs = stmt.getResultSet();
          outputQueryResult(rs, this.outputStream);
          stmt.close();
        } else {
          try {
            final PreparedStatement stmt = prepareStatement(conn, queryLine);
            stmt.execute();
            stmt.close();
          } catch (final NullPointerException e) {
            // An empty query (or comment) throws a NPE in JDBC. Yay!
            System.err
                .println("Caught NPE in execute call because report has a NOOP query: "
                    + queryLine);
          }
        }
      } catch (final Exception e) {
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

  protected String[] cleanAndGetQueries(final String sqlQuery,
      final Map<String, String> queryBandProperties) {

    /**
     * Teradata's SET Query_Band allows use to "proxy" to an LDAP user. This
     * makes queries appear to admins as though it's being issued by the owner
     * of the report, rather than the 'Reportal' user. Tables will still be
     * "owned" by Reportal, but admins will be able to send angry emails to the
     * proper user when a reportal query is impacting the system negatively.
     * Best we could do.
     */

    final StringBuilder queryBandBuilder = new StringBuilder();
    for (final Map.Entry<String, String> pair : queryBandProperties.entrySet()) {
      queryBandBuilder.append("" + pair.getKey() + "=" + pair.getValue() + ";");
    }

    final String queryBand =
        "SET Query_Band = '" + queryBandBuilder.toString()
            + "' FOR SESSION;";
    final ArrayList<String> injectedQueries = new ArrayList<>();

    injectedQueries.add(queryBand);
    final String[] queries = StringUtils.split(sqlQuery.trim(), ";");
    for (String query : queries) {
      query = cleanQueryLine(query);
      if (query == null || query.isEmpty()) {
        continue;
      }
      injectedQueries.add(query);
    }

    return injectedQueries.toArray(new String[]{});
  }

  private String cleanQueryLine(final String line) {
    if (line != null) {
      return line.trim();
    }
    return null;
  }

  private void outputQueryResult(final ResultSet result, final OutputStream outputStream)
      throws SQLException {
    final PrintStream outFile = new PrintStream(outputStream);
    final String delim = ",";
    boolean isHeaderPending = true;
    if (result != null) {
      while (result.next()) {
        final int numColumns = result.getMetaData().getColumnCount();
        final StringBuilder dataString = new StringBuilder();

        if (isHeaderPending) {
          final StringBuilder headerString = new StringBuilder();
          for (int j = 1; j <= numColumns; j++) {
            final String colName = formatValue(result.getMetaData().getColumnName(j));
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

  private String formatValue(final String value) {
    return "\"" + value.replace("\"", "") + "\"";
  }

  private PreparedStatement prepareStatement(final Connection conn, String line)
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
    final PreparedStatement stmt = conn.prepareStatement(line);
    // for (int i = 0; i < variableReplacements.size(); i++) {
    // stmt.setString(i + 1, variableReplacements.get(i));
    // sb.append(variableReplacements.get(i)).append(",");
    // }

    System.out.println("Reportal Teradata: Teradata query: " + line);
    // System.out.println("Reportal Teradata: Variables: " + sb.toString());
    return stmt;
  }

  private static class TeradataDataSource extends BasicDataSource {

    private TeradataDataSource(final String connectionString, final String user,
        final String password) {
      super();
      setDriverClassName("com.teradata.jdbc.TeraDriver");
      setUrl(connectionString);
      setUsername(user);
      setPassword(password);
    }
  }
}
