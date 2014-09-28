package azkaban.project.validator;

import java.io.File;
import java.util.List;
import java.util.Map;

import azkaban.utils.Props;

public interface ValidatorManager {
  void loadValidators(Props props);

  Map<String, ValidationReport> validate(File projectDir);

  List<String> getValidatorsInfo();
}
