package azkaban.utils;

import azkaban.project.Project;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidatorManager;
import azkaban.project.validator.XmlValidatorManager;
import java.io.File;
import java.util.Map;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ValidatorUtils {
  private static final Logger logger = LoggerFactory.getLogger(ValidatorUtils.class);

  private final ValidatorManager validatorManager;

  @Inject
  public ValidatorUtils(final Props prop) {
    logger.info("Creating XmlValidatorManager instance (loading validators)...");
    this.validatorManager = new XmlValidatorManager(prop);
    logger.info("XmlValidatorManager instance created.");
  }

  public String getCacheKey(final Project project, final File folder, final Props props) {
    return this.validatorManager.getCacheKey(project, folder, props);
  }

  public Map<String, ValidationReport> validateProject(final Project project, final File folder, final Props props) {
    logger.info("Validating project " + project.getName()
        + " using the registered validators "
        + this.validatorManager.getValidatorsInfo().toString());
    return this.validatorManager.validate(project, folder, props);
  }
}
