package azkaban.project.validator;

import java.io.File;

import azkaban.project.Project;
import azkaban.utils.Props;

/**
 * Interface to be implemented by plugins which are to be registered with Azkaban
 * as project validators that validate a project before uploaded into Azkaban.
 */
public interface ProjectValidator {

  /**
   * Initialize the validator using the given properties.
   *
   * @param configuration
   * @return
   */
  boolean initialize(Props configuration);

  /**
   * Return a user friendly name of the validator.
   *
   * @return
   */
  String getValidatorName();

  /**
   * Validate the project inside the given directory. The validator, using its own
   * validation logic, will generate a {@link ValidationReport} representing the result of
   * the validation.
   *
   * @param projectDir
   * @return
   */
  ValidationReport validateProject(Project project, File projectDir);
}
