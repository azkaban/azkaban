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

package azkaban.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;

import azkaban.user.UserManager;
import azkaban.utils.Props;
import azkaban.server.session.SessionCache;

public abstract class AzkabanServer {
  private static final Logger logger = Logger.getLogger(AzkabanServer.class);
  public static final String AZKABAN_PROPERTIES_FILE = "azkaban.properties";
  public static final String AZKABAN_PRIVATE_PROPERTIES_FILE =
      "azkaban.private.properties";
  public static final String DEFAULT_CONF_PATH = "conf";
  private static Props azkabanProperties = null;

  public static Props loadProps(String[] args) {
    azkabanProperties = loadProps(args, new OptionParser());
    return azkabanProperties;
  }

  public static Props getAzkabanProperties() {
    return azkabanProperties;
  }

  public static Props loadProps(String[] args, OptionParser parser) {
    ;
    OptionSpec<String> configDirectory =
        parser
            .acceptsAll(Arrays.asList("c", "conf"),
                "The conf directory for Azkaban.").withRequiredArg()
            .describedAs("conf").ofType(String.class);

    // Grabbing the azkaban settings from the conf directory.
    Props azkabanSettings = null;
    OptionSet options = parser.parse(args);

    if (options.has(configDirectory)) {
      String path = options.valueOf(configDirectory);
      logger.info("Loading azkaban settings file from " + path);
      File dir = new File(path);
      if (!dir.exists()) {
        logger.error("Conf directory " + path + " doesn't exist.");
      } else if (!dir.isDirectory()) {
        logger.error("Conf directory " + path + " isn't a directory.");
      } else {
        azkabanSettings = loadAzkabanConfigurationFromDirectory(dir);
      }
    } else {
      logger
          .info("Conf parameter not set, attempting to get value from AZKABAN_HOME env.");
      azkabanSettings = loadConfigurationFromAzkabanHome();
    }

    return azkabanSettings;
  }

  private static Props loadAzkabanConfigurationFromDirectory(File dir) {
    File azkabanPrivatePropsFile =
        new File(dir, AZKABAN_PRIVATE_PROPERTIES_FILE);
    File azkabanPropsFile = new File(dir, AZKABAN_PROPERTIES_FILE);

    Props props = null;
    try {
      // This is purely optional
      if (azkabanPrivatePropsFile.exists() && azkabanPrivatePropsFile.isFile()) {
        logger.info("Loading azkaban private properties file");
        props = new Props(null, azkabanPrivatePropsFile);
      }

      if (azkabanPropsFile.exists() && azkabanPropsFile.isFile()) {
        logger.info("Loading azkaban properties file");
        props = new Props(props, azkabanPropsFile);
      }
    } catch (FileNotFoundException e) {
      logger.error("File not found. Could not load azkaban config file", e);
    } catch (IOException e) {
      logger.error(
          "File found, but error reading. Could not load azkaban config file",
          e);
    }

    return props;
  }

  /**
   * Loads the Azkaban property file from the AZKABAN_HOME conf directory
   *
   * @return
   */
  private static Props loadConfigurationFromAzkabanHome() {
    String azkabanHome = System.getenv("AZKABAN_HOME");

    if (azkabanHome == null) {
      logger.error("AZKABAN_HOME not set. Will try default.");
      return null;
    }

    if (!new File(azkabanHome).isDirectory()
        || !new File(azkabanHome).canRead()) {
      logger.error(azkabanHome + " is not a readable directory.");
      return null;
    }

    File confPath = new File(azkabanHome, DEFAULT_CONF_PATH);
    if (!confPath.exists() || !confPath.isDirectory() || !confPath.canRead()) {
      logger
          .error(azkabanHome + " does not contain a readable conf directory.");
      return null;
    }

    return loadAzkabanConfigurationFromDirectory(confPath);
  }

  public abstract Props getServerProps();

  public abstract SessionCache getSessionCache();

  public abstract VelocityEngine getVelocityEngine();

  public abstract UserManager getUserManager();

}
