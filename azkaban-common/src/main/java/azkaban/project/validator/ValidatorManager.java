package azkaban.project.validator;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.utils.Props;

public interface ValidatorManager {
  void loadValidators(Props props, Logger logger);

  Map<String, ValidationReport> validate(File projectDir);

  ProjectValidator getDefaultValidator();

  List<String> getValidatorsInfo();
}
