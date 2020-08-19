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

package azkaban.project;

import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.FileValidationStatus;
import azkaban.utils.DependencyTransferException;
import azkaban.utils.DependencyTransferManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.InvalidHashException;
import azkaban.utils.Props;
import azkaban.utils.ValidatorUtils;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static azkaban.utils.ThinArchiveUtils.*;

/**
 * Handles processing of uploaded Thin Archives to the web server.
 *
 * This class exposes one public method, validateThinProject() which provides
 * the all the meat of the processing for Thin Archives. In summary it will:
 *
 * 1. Parse the startup-dependencies.json file
 *
 * 2. Generate a validation key from the project validators to use for querying the database. For any two projects
 * that produce the same validation key, the validator is GUARANTEED to produce the same result for any given
 * unique JAR that is in both the projects. In other words if mylib-1.0.0.jar is present in ProjectA and THE EXACT SAME FILE
 * (mylib-1.0.0.jar) is also present in ProjectB and generating a validation key for each project results in an IDENTICAL key,
 * the validation results for both JARs will ALSO be IDENTICAL. If the validation keys for the projects are different, the
 * validation results for the JARs (despite them being the same JAR) may or may not be identical, there is no guarantee in
 * that case.
 *
 * 3. Query the database to determine which dependencies have already been validated for the given validation key.
 * 4. Download NEW dependencies (not listed in the database) from the REMOTE origin.
 * 5. Validate the whole project with the NEW dependencies included.
 * 6. If the project failed validation with ValidationStatus.ERROR, return the report and stop there. Otherwise...
 * 7. Update the database to cache the results of the validator, specifically: the newly downloaded JARs the validator
 * removed (REMOVED), and the newly downloaded JARs that were unmodified (VALID).
 * 8. Keep any files modified by the validator in the project archive.
 * 9. Remove entries from the startup-dependencies.json file for JARs that were modified during validation OR JARs that
 * were removed during validation OR JARs that were cached as ValidationStatus.REMOVED in the database (from the query in
 * step 2).
 * 10. Return the validation reports, including an additional validation report specifically for actions taken based on
 * cached validation actions (i.e. removing JARs that were cached as REMOVED in the database).
 * 11. Celebrate, we're done! :)
 */
@Singleton
public class ArchiveUnthinner {
  private static final Logger log = LoggerFactory.getLogger(ArchiveUnthinner.class);

  public static final String CACHED_VALIDATOR_REPORT_NAME = "Cached Validator Actions";

  private final JdbcDependencyManager jdbcDependencyManager;
  private final DependencyTransferManager dependencyTransferManager;
  private final ValidatorUtils validatorUtils;

  private final boolean isEnabled;

  @Inject
  public ArchiveUnthinner(final ValidatorUtils validatorUtils, final JdbcDependencyManager jdbcDependencyManager,
      final DependencyTransferManager dependencyTransferManager) {
    this.validatorUtils = validatorUtils;
    this.jdbcDependencyManager = jdbcDependencyManager;
    this.dependencyTransferManager = dependencyTransferManager;

    // The dependency transfer manager must be enabled for thin archive to be supported
    this.isEnabled = dependencyTransferManager.isEnabled();
  }

  /**
   * @param project current project
   * @param projectFolder current project folder
   * @param startupDependenciesFile startup-dependencies.json file for this project
   * @param additionalValidatorProps additional props for the validator
   *
   * @return Map of Report Name -> Validation Report
   */
  public Map<String, ValidationReport> validateThinProject(final Project project,
      final File projectFolder, final File startupDependenciesFile, final Props additionalValidatorProps) {
    if (!isEnabled) {
      throw new ProjectManagerException("Thin archive support is not yet enabled on this cluster.");
    }

    Set<Dependency> dependencies = getDependenciesFromSpec(startupDependenciesFile);

    String validationKey = this.validatorUtils.getCacheKey(project, projectFolder, additionalValidatorProps);

    // Find the cached validation status (or NEW if the dep isn't cached) for each dependency.
    Map<Dependency, FileValidationStatus> depsToValidationStatus = getValidationStatuses(dependencies, validationKey);
    // removedCachedDeps: dependencies that have been processed before and are blacklisted (so should be removed)
    Set<Dependency> removedCachedDeps = filterValidationStatus(depsToValidationStatus, FileValidationStatus.REMOVED);
    // validCachedDeps: dependencies that are in storage and already verified to be valid
    Set<Dependency> validCachedDeps = filterValidationStatus(depsToValidationStatus, FileValidationStatus.VALID);
    // newDeps: dependencies that are not in storage and need to be verified
    Set<Dependency> newDeps = filterValidationStatus(depsToValidationStatus, FileValidationStatus.NEW);

    // Download the new dependencies
    final Set<DependencyFile> downloadedDeps = downloadDependencyFiles(projectFolder, newDeps);

    // Validate the project
    Map<String, ValidationReport> reports = this.validatorUtils.validateProject(project, projectFolder, additionalValidatorProps);
    if (reports.values().stream().anyMatch(r -> r.getStatus() == ValidationStatus.ERROR)) {
      // No point continuing, this project has been rejected, so just return the validation report
      // and don't waste any more time.
      return reports;
    }

    // Find which dependencies were removed, modified or untouched by the validator
    // pathToDownloadedDeps is created for performance reasons to allow getDepsFromReports to run in O(n) time
    // instead of O(n^2).
    Map<String, DependencyFile> pathToDownloadedDeps = getPathToDepFileMap(downloadedDeps);
    Set<DependencyFile> removedDownloadedDeps =
        getDepsFromReports(reports, pathToDownloadedDeps, ValidationReport::getRemovedFiles);
    Set<DependencyFile> modifiedDownloadedDeps =
        getDepsFromReports(reports, pathToDownloadedDeps, ValidationReport::getModifiedFiles);
    Set<DependencyFile> untouchedDownloadedDeps =
        Sets.difference(downloadedDeps, Sets.union(removedDownloadedDeps, modifiedDownloadedDeps));

    updateValidationStatuses(untouchedDownloadedDeps, removedDownloadedDeps, validationKey);

    // Create a new report that will include details of actions taken based on previous cached validation actions.
    ValidationReport cacheReport = new ValidationReport();
    // Add warnings for files removed due to a cached validation status of REMOVED. Note that we don't have to manually
    // add warnings for removedDownloadedDeps because the validator should already create warnings for them during
    // validation. The dependencies that have a cached validation status of REMOVED are not actually downloaded and
    // thus are not passed through the validator so no warnings will generated for them - so we have to add our own.
    cacheReport.addWarningMsgs(getWarningsFromRemovedDeps(removedCachedDeps));

    // See if any downloaded deps were modified/removed OR if there are any cached removed dependencies
    if (untouchedDownloadedDeps.size() < downloadedDeps.size() || removedCachedDeps.size() > 0) {
      // Either one or more of the dependencies we downloaded was removed/modified during validation
      // OR there are cached removed dependencies. Either way we need to remove them from the
      // startup-dependencies.json file.

      // Indicate in the cacheReport that we modified startup-dependencies.json
      cacheReport.addModifiedFile(startupDependenciesFile);

      // Get the final list of startup dependencies that will be downloadable from storage and update the
      // startup-dependencies.json file to include only them. Any dependencies originally listed in the
      // startup-dependencies.json file that will be removed during the update must have either been removed
      // by the validator - or will be included in the zip itself.
      //
      // Filter out null from the Set if present since the Set should only contain
      // dependency objects.
      Set<Dependency> finalDeps = filterNullFromDeps(Sets.union(validCachedDeps,
          untouchedDownloadedDeps));
      rewriteStartupDependencies(startupDependenciesFile, finalDeps);
    }

    // Delete from the project untouched downloaded dependencies
    untouchedDownloadedDeps.forEach(d -> d.getFile().delete());

    // Add the cacheReport to the list of reports
    reports.put(CACHED_VALIDATOR_REPORT_NAME, cacheReport);

    return reports;
  }

  private void rewriteStartupDependencies(final File startupDependenciesFile, final Set<Dependency> finalDependencies) {
    // Write this list back to the startup-dependencies.json file
    try {
      writeStartupDependencies(startupDependenciesFile, finalDependencies);
    } catch (IOException e) {
      throw new ProjectManagerException("Error while writing new startup-dependencies.json", e);
    }
  }
  private Set<Dependency> getDependenciesFromSpec(final File startupDependenciesFile) {
    try {
      return parseStartupDependencies(startupDependenciesFile);
    } catch (IOException e) {
      throw new ProjectManagerException("Unable to open or parse startup-dependencies.json. Please ensure "
                                        + "that the JSON contained is valid and properly conforms to the "
                                        + "spec format.", e);
    } catch (InvalidHashException e) {
      throw new ProjectManagerException("One or more of the SHA1 hashes in startup-dependencies.json was invalid", e);
    }
  }

  private Map<Dependency, FileValidationStatus> getValidationStatuses(final Set<Dependency> deps,
      final String validationKey) {
    try {
      return this.jdbcDependencyManager.getValidationStatuses(deps, validationKey);
    } catch (SQLException e) {
      throw new ProjectManagerException(
          String.format("Unable to query DB for validation statuses "
            + "for project with validationKey %s", validationKey));
    }
  }

  private void updateValidationStatuses(final Set<? extends Dependency> untouchedDeps,
      final Set<? extends Dependency> removedDeps, final String validationKey) {
    // untouchedDeps are new dependencies that we have just validated and found to be VALID.

    // removedDeps are new dependencies that we have just validated and found to be REMOVED.
    Map<Dependency, FileValidationStatus> depValidationStatuses = new HashMap<>();
    // NOTE: .map(Dependency::makeCopy) is to ensure our map keys are actually of type Dependency not DependencyFile
    untouchedDeps.stream().map(Dependency::copy).forEach(d -> depValidationStatuses.put(d, FileValidationStatus.VALID));
    removedDeps.stream().map(Dependency::copy).forEach(d -> depValidationStatuses.put(d, FileValidationStatus.REMOVED));
    try {
      this.jdbcDependencyManager.updateValidationStatuses(depValidationStatuses, validationKey);
    } catch (SQLException e) {
      throw new ProjectManagerException(
          String.format("Unable to update DB for validation statuses "
              + "for project with validationKey %s", validationKey));
    }
  }

  private Set<DependencyFile> downloadDependencyFiles(final File projectFolder,
      final Set<Dependency> toDownload) {
    final Set<DependencyFile> depFiles = toDownload.stream().map(d -> {
      File downloadedJar = new File(projectFolder, d.getDestination() + File.separator + d.getFileName());
      return d.makeDependencyFile(downloadedJar);
    }).collect(Collectors.toSet());

    try {
      this.dependencyTransferManager.downloadAllDependencies(depFiles);
    } catch (DependencyTransferException e) {
      throw new ProjectManagerException(e.getMessage(), e.getCause());
    }

    return depFiles;
  }

  private Set<DependencyFile> getDepsFromReports(final Map<String, ValidationReport> reports,
      final Map<String, DependencyFile> pathToDep, final Function<ValidationReport, Set<File>> fn) {
    return reports.values()
        .stream()
        .map(fn)
        .flatMap(Collection::stream)
        .map(f -> pathToDep.get(FileIOUtils.getCanonicalPath(f)))
        .filter(Objects::nonNull) // Some modified/removed files will not be a dependency (i.e. snapshot jar)
        .collect(Collectors.toSet());
  }

  private Map<String, DependencyFile> getPathToDepFileMap(final Set<DependencyFile> depFiles) {
    return depFiles
        .stream()
        .collect(Collectors.toMap(d -> FileIOUtils.getCanonicalPath(d.getFile()), e -> e));
  }


  private Set<Dependency> filterValidationStatus(final Map<Dependency, FileValidationStatus> validationStatuses,
      final FileValidationStatus status) {
    return validationStatuses
        .keySet()
        .stream()
        .filter(Objects::nonNull) // Filter out null since the Set should contain Dependency
        // objects only.
        .filter(d -> validationStatuses.get(d) == status)
        .collect(Collectors.toSet());
  }

  private Set<String> getWarningsFromRemovedDeps(final Set<? extends Dependency> removedDeps) {
    return removedDeps
        .stream()
        .map(d -> String.format("Removed blacklisted file %s", d.getFileName()))
        .collect(Collectors.toSet());
  }
}
