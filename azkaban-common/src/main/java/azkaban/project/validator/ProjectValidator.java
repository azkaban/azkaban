package azkaban.project.validator;

import azkaban.project.Project;
import azkaban.utils.Props;
import java.io.File;

/**
 * Interface to be implemented by plugins which are to be registered with Azkaban as project
 * validators that validate a project before uploaded into Azkaban.
 */
public interface ProjectValidator {

  /**
   * Initialize the validator using the given properties.
   */
  boolean initialize(Props configuration);

  /**
   * Return a user friendly name of the validator.
   */
  String getValidatorName();

  /**
   * Validate the project inside the given directory. The validator, using its own validation logic,
   * will generate a {@link ValidationReport} representing the result of the validation.
   */
  ValidationReport validateProject(Project project, File projectDir);
}
