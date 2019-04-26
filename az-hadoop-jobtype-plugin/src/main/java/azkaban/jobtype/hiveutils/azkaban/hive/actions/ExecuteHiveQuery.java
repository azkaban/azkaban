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
package azkaban.jobtype.hiveutils.azkaban.hive.actions;

import azkaban.jobtype.hiveutils.HiveQueryExecutionException;
import azkaban.jobtype.hiveutils.HiveQueryExecutor;
import azkaban.jobtype.hiveutils.azkaban.Utils;
import azkaban.jobtype.hiveutils.azkaban.HiveAction;
import azkaban.jobtype.hiveutils.azkaban.HiveViaAzkabanException;
import azkaban.jobtype.hiveutils.util.AzkabanJobPropertyDescription;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Execute the provided Hive query. Queries can be specified to Azkaban either
 * directly or as a pointer to a file provided with workflow.
 */
public class ExecuteHiveQuery implements HiveAction {

  private final static Logger LOG = LoggerFactory
      .getLogger("com.linkedin.hive.azkaban.hive.actions.ExecuteHiveQuery");
  @AzkabanJobPropertyDescription("Verbatim query to execute. Can also specify hive.query.nn where nn is a series of padded numbers, which will be executed in order")
  public static final String HIVE_QUERY = "hive.query";
  @AzkabanJobPropertyDescription("File to load query from.  Should be in same zip.")
  public static final String HIVE_QUERY_FILE = "hive.query.file";
  @AzkabanJobPropertyDescription("URL to retrieve the query from.")
  public static final String HIVE_QUERY_URL = "hive.query.url";

  private final HiveQueryExecutor hqe;
  private final String q;

  public ExecuteHiveQuery(Properties properties, HiveQueryExecutor hqe)
      throws HiveViaAzkabanException {
    String singleLine = properties.getProperty(HIVE_QUERY);
    String multiLine = extractMultilineQuery(properties);
    String queryFile = extractQueryFromFile(properties);
    String queryURL = extractQueryFromURL(properties);

    this.q = determineQuery(singleLine, multiLine, queryFile, queryURL);
    this.hqe = hqe;
  }

  @SuppressWarnings("Finally")
  private String extractQueryFromFile(Properties properties)
      throws HiveViaAzkabanException {
    String file = properties.getProperty(HIVE_QUERY_FILE);

    if (file == null) {
      return null;
    }

    LOG.info("Attempting to read query from file: " + file);

    StringBuilder contents = new StringBuilder();
    BufferedReader br = null;
    try {
//      br = new BufferedReader(new FileReader(file));
      br = new BufferedReader(new InputStreamReader(
          new FileInputStream(file), StandardCharsets.UTF_8));

      String line;

      while ((line = br.readLine()) != null) {
        contents.append(line);
        contents.append(System.getProperty("line.separator"));
      }

    } catch (IOException e) {
      throw new HiveViaAzkabanException(e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          // TODO: Just throw IOException and catch-wrap in the constructor...
          throw new HiveViaAzkabanException(e);
        }
      }
    }

    return contents.toString();
  }

  @SuppressWarnings("Finally")
  private String extractQueryFromURL(Properties properties)
      throws HiveViaAzkabanException {
    String url = properties.getProperty(HIVE_QUERY_URL);

    if (url == null) {
      return null;
    }

    LOG.info("Attempting to retrieve query from URL: " + url);

    StringBuilder contents = new StringBuilder();
    BufferedReader br = null;

    try {
      URL queryURL = new URL(url);

      br = new BufferedReader(new InputStreamReader(queryURL.openStream(), StandardCharsets.UTF_8));
      String line;

      while ((line = br.readLine()) != null) {
        contents.append(line);
        contents.append(System.getProperty("line.separator"));
      }
    } catch (IOException e) {
      throw new HiveViaAzkabanException(e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          // TODO: Just throw IOException and catch-wrap in the constructor...
          throw new HiveViaAzkabanException(e);
        }
      }
    }

    return contents.toString();
  }

  private String determineQuery(String singleLine, String multiLine,
      String queryFromFile, String queryFromURL) throws HiveViaAzkabanException {
    int specifiedValues = 0;

    for (String s : new String[]{singleLine, multiLine, queryFromFile,
        queryFromURL}) {
      if (s != null) {
        specifiedValues++;
      }
    }

    if (specifiedValues == 0) {
      throw new HiveViaAzkabanException("Must specify " + HIVE_QUERY + " xor "
          + HIVE_QUERY + ".nn xor " + HIVE_QUERY_FILE + " xor "
          + HIVE_QUERY_URL + " in properties. Exiting.");
    }

    if (specifiedValues != 1) {
      throw new HiveViaAzkabanException("Must specify only " + HIVE_QUERY
          + " or " + HIVE_QUERY + ".nn or " + HIVE_QUERY_FILE + " or "
          + HIVE_QUERY_URL + " in properties, not more than one. Exiting.");
    }

    if (singleLine != null) {
      LOG.info("Returning " + HIVE_QUERY + " = " + singleLine);
      return singleLine;
    } else if (multiLine != null) {
      LOG.info("Returning consolidated " + HIVE_QUERY + ".nn = " + multiLine);
      return multiLine;
    } else if (queryFromFile != null) {
      LOG.info("Returning query from file " + queryFromFile);
      return queryFromFile;
    } else {
      LOG.info("Returning query from URL " + queryFromURL);
      return queryFromURL;
    }
  }

  private String extractMultilineQuery(Properties properties) {
    ArrayList<String> lines = new ArrayList<String>();

    for (int i = 0; i < 100; i++) {
      String padded = String.format("%02d", i);
      String value = properties.getProperty(HIVE_QUERY + "." + padded);
      if (value != null) {
        lines.add(value);
      }
    }

    return Utils.joinNewlines(lines);
  }

  @Override
  public void execute() throws HiveViaAzkabanException {
    try {
      hqe.executeQuery(q);
    } catch (HiveQueryExecutionException e) {
      throw new HiveViaAzkabanException(e);
    }
  }
}
