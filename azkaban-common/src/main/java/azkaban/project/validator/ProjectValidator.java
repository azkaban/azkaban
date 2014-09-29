package azkaban.project.validator;

import java.io.File;

import azkaban.utils.Props;

public interface ProjectValidator {
  boolean initialize(Props configuration);
  String getValidatorInfo();
  ValidationReport validateProject(File projectDir);
}
