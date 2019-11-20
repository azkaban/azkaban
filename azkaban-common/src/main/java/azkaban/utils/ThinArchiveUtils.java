/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.utils;

import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.Storage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class ThinArchiveUtils {
  // Root of folder in storage containing startup dependencies
  public static final String DEPENDENCY_STORAGE_ROOT_PATH_PROP = "dependency.storage.path.prefix";

  public static File getStartupDependenciesFile(final File projectFolder) {
    return new File(projectFolder.getPath() + "/app-meta/startup-dependencies.json");
  }

  public static DependencyFile getDependencyFile(final File projectFolder, final Dependency d) {
    return d.makeDependencyFile(new File(projectFolder, d.getDestination() + File.separator + d.getFileName()));
  }

  public static Set<Dependency> parseStartupDependencies(final String rawJson) throws IOException, InvalidHashException {
    if (rawJson.isEmpty()) {
      return new HashSet<>();
    }

    List<Map<String, String>> rawParseResult =
        ((HashMap<String, List<Map<String, String>>>) JSONUtils.parseJSONFromString(rawJson)).get("dependencies");

    if (rawParseResult == null) {
      throw new IOException("Could not find 'dependencies' key in startup-dependencies.json file.");
    }

    Set<Dependency> finalDependencies = new HashSet<>();
    for (Map<String, String> rawDependency : rawParseResult) {
      finalDependencies.add(new Dependency(rawDependency));
    }

    return finalDependencies;
  }

  public static Set<Dependency> parseStartupDependencies(final File f) throws IOException, InvalidHashException {
    return parseStartupDependencies(FileUtils.readFileToString(f));
  }

  public static void writeStartupDependencies(final File f,
      final Set<Dependency> dependencies) throws IOException {
    Map<String, Set<Dependency>> outputFormat = new HashMap<>();
    outputFormat.put("dependencies", dependencies);
    FileUtils.writeStringToFile(f, JSONUtils.toJSON(outputFormat));
  }

  public static String convertIvyCoordinateToPath(final Dependency dep) {
    String[] coordinateParts = dep.getIvyCoordinates().split(":");
    return coordinateParts[0].replace(".", "/") + "/"
        + coordinateParts[1] + "/"
        + coordinateParts[2] + "/"
        + dep.getFileName();
  }

  /**
   * Taking a string with comma seperated file paths of jars within a project folder, if the project has a
   * startup-dependencies.json file (therefore is from a thin archive) each file path will be compared against the
   * cached dependencies listed in startup-dependencies.json. If a match is found, the file path will be replaced
   * with a hdfs:// path to the cached dependency. If a match is not found, the original local file path will be
   * included in the returned comma separated list. IF the project does not have a startup-dependencies.json file
   * (is not a thin archive) - the string of file paths passed in will be returned without modification.
   *
   * @param projectFolder root folder of uncompressed project
   * @param localJarSpec string of comma separated file paths to jar dependencies within the project folder
   *                     MUST BE RELATIVE PATHS!!!
   * @param jobProps job properties
   * @return list of file path strings and hdfs:// path strings, one for each dependency
   */
  public static String replaceLocalPathsWithStoragePaths(final File projectFolder,
      String localJarSpec, final Props jobProps, final Logger log) {
    File startupDependenciesFile = getStartupDependenciesFile(projectFolder);
    String baseDependencyPath = jobProps.get(DEPENDENCY_STORAGE_ROOT_PATH_PROP);
    if (!startupDependenciesFile.exists() || baseDependencyPath == null) {
      // This is not a thin archive OR we don't have a baseDependencyPath - so we can't do any replacing
      log.debug("Skipping replacing dependency paths with common HDFS startup dependencies.");
      return localJarSpec;
    }

    String[] localDependencies = localJarSpec.split(",");

    try {
      Set<Dependency> startupDeps = parseStartupDependencies(startupDependenciesFile);

      Map<String, Dependency> pathToDep = new HashMap<>();
      for (Dependency dep : startupDeps) {
        pathToDep.put(getDependencyFile(projectFolder, dep).getFile().getCanonicalPath(), dep);
      }

      List<String> finalDependencies = new ArrayList<>();
      for (String localDepPath : localDependencies) {
        final String localDepCanonicalPath = new File(projectFolder, localDepPath).getCanonicalPath();

        if (pathToDep.containsKey(localDepCanonicalPath)) {
          // This dependency was listed in startup-dependencies.json so we can replace its local filepath
          // with a storage path!
          if (baseDependencyPath.endsWith("/")) {
            baseDependencyPath = baseDependencyPath.substring(0, baseDependencyPath.length() - 1);
          }

          String pathToDependencyInStorage =
              baseDependencyPath + "/" + convertIvyCoordinateToPath(pathToDep.get(localDepCanonicalPath));

          finalDependencies.add(pathToDependencyInStorage);
        } else {
          // This dependency was not found in startup-dependencies.json so just keep it's original local filepath
          // entry
          finalDependencies.add(localDepPath);
        }
      }

      String localAndRemoteJarSpec = String.join(",", finalDependencies);
      log.debug("replaceLocalPathsWithStoragePaths: localAndRemoteJarSpec: " + localAndRemoteJarSpec);
      return localAndRemoteJarSpec;
    } catch (IOException | InvalidHashException e) {
      // If something goes wrong, swallow the error and just return the original string.
      log.warn("Error while opening and parsing startup dependencies file " + startupDependenciesFile.getAbsolutePath());
      return localJarSpec;
    }
  }

  public static void validateDependencyHash(final DependencyFile f)
      throws HashNotMatchException {
    validateDependencyHash(f.getFile(), f);
  }

  public static void validateDependencyHash(final File f, final Dependency d)
      throws HashNotMatchException {
    try {
      final byte[] actualFileHash = HashUtils.SHA1.getHashBytes(f);
      if (!HashUtils.isSameHash(d.getSHA1(), actualFileHash)) {
        throw new HashNotMatchException(String.format("SHA1 Dependency hash check failed. File: %s Expected: %s Actual: %s",
            d.getFileName(),
            d.getSHA1(),
            HashUtils.bytesHashToString(actualFileHash)));
      }
    } catch (DecoderException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
