package azkaban.project.validator;

import java.io.File;
import java.util.Properties;

public interface ProjectValidator {
  boolean initialize(Properties configuration);
  String getValidatorInfo();
  ValidationReport validateProject(File projectDir);
}
