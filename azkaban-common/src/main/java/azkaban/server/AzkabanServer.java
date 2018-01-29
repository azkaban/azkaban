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

import static azkaban.Constants.DEFAULT_PORT_NUMBER;
import static azkaban.Constants.DEFAULT_SSL_PORT_NUMBER;

import azkaban.Constants;
import azkaban.executor.mail.MailCreatorRegistry;
import azkaban.executor.mail.TemplateBasedMailCreator;
import azkaban.server.session.SessionCache;
import azkaban.user.UserManager;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;


public abstract class AzkabanServer {

  private static final Logger logger = Logger.getLogger(AzkabanServer.class);
  private static Props azkabanProperties = null;

  public static Props loadProps(final String[] args) {
    azkabanProperties = loadProps(args, new OptionParser());
    return azkabanProperties;
  }

  public static Props getAzkabanProperties() {
    return azkabanProperties;
  }

  private static Optional<Path> azkabanPath(String scope, String pathBase, String... pathSegments) {
    logger.info(scope + ": loading from path " + pathBase);
    final Path path = Paths.get(pathBase, pathSegments);
    if (!Files.exists(path)) {
      logger.error(scope + ": path " + path + " does not exist.");
    } else if (!Files.isDirectory(path) || !Files.isReadable(path)) {
      logger.error(scope + ": path " + path + " is not a readable directory.");
    } else {
      return Optional.of(path);
    }
    return Optional.empty();
  }

  public static Props loadProps(final String[] args, final OptionParser parser) {
    logger.info("loading properties...");

    final OptionSpec<String> configDirectory =
        parser
            .acceptsAll(Arrays.asList("c", "conf"), "The conf directory for Azkaban.")
            .withRequiredArg()
            .describedAs("conf")
            .ofType(String.class);

    final OptionSpec<String> emailDirectory =
        parser
            .acceptsAll(Arrays.asList("e", "email"),
                "The path to email templates for " + TemplateBasedMailCreator.class.getSimpleName()
                    + ".")
            .withOptionalArg()
            .describedAs("email")
            .ofType(String.class);
    final OptionSet options = parser.parse(args);

    final String azkabanHome = System.getenv("AZKABAN_HOME");

    // Grabbing the Azkaban settings from the conf directory.
    final Optional<Path> configPath;
    if (options.has(configDirectory)) {
      final String confHome = options.valueOf(configDirectory);
      logger.info("Conf parameter set, attempting to get value from " + confHome);
      configPath = azkabanPath("configuration", confHome);
    } else {
      logger.info("Conf parameter not set, attempting to get value from AZKABAN_HOME=" +
        azkabanHome +", DEFAULT_CONF_PATH=" + Constants.DEFAULT_CONF_PATH + ".");
      configPath = azkabanPath("configuration", azkabanHome, Constants.DEFAULT_CONF_PATH);
    }
    final Optional<Props> azkabanSettings = configPath.map(path ->
        loadConfigurationFromPath(path));

    final Optional<Path> emailPath;
    if (options.has(emailDirectory)) {
      final String emailHome = options.valueOf(emailDirectory);
      logger.info("Email parameter set, attempting to get value from " + emailHome);
      emailPath = azkabanPath("email template", emailHome);
    } else {
      logger.info("Email parameter not set, attempting to get value from " + azkabanHome + "/" +
          Constants.DEFAULT_EMAIL_TEMPLATE_PATH + ".");
      emailPath = azkabanPath("email template", azkabanHome, Constants.DEFAULT_EMAIL_TEMPLATE_PATH);
    }
    emailPath
      .flatMap(path -> TemplateBasedMailCreator.fromPath(path))
      .ifPresent(creator -> MailCreatorRegistry.registerCreator(creator));

    azkabanSettings.ifPresent(settings -> updateDerivedConfigs(settings));
    return azkabanSettings.orElse(null);
  }

  private static void updateDerivedConfigs(final Props azkabanSettings) {
    final boolean isSslEnabled = azkabanSettings.getBoolean("jetty.use.ssl", true);
    final int port = isSslEnabled
        ? azkabanSettings.getInt("jetty.ssl.port", DEFAULT_SSL_PORT_NUMBER)
        : azkabanSettings.getInt("jetty.port", DEFAULT_PORT_NUMBER);

    // setting stats configuration for connectors
    final String hostname = azkabanSettings.getString("jetty.hostname", "localhost");
    azkabanSettings.put("server.hostname", hostname);
    azkabanSettings.put("server.port", port);
    azkabanSettings.put("server.useSSL", String.valueOf(isSslEnabled));
  }

  public static Props loadConfigurationFromPath(final Path path) {
    File dir = path.toFile();
    final File azkabanPrivatePropsFile = new File(dir, Constants.AZKABAN_PRIVATE_PROPERTIES_FILE);
    final File azkabanPropsFile = new File(dir, Constants.AZKABAN_PROPERTIES_FILE);

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
    } catch (final FileNotFoundException e) {
      logger.error("File not found. Could not load azkaban config file", e);
    } catch (final IOException e) {
      logger.error("File found, but error reading. Could not load azkaban config file", e);
    }
    return props;
  }

  public abstract Props getServerProps();

  public abstract SessionCache getSessionCache();

  public abstract VelocityEngine getVelocityEngine();

  public abstract UserManager getUserManager();

}
