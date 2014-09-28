package azkaban.project.validator;

import java.util.Set;

public interface ValidationReport {
  void addPassMsgs(Set<String> msgs);

  void addWarningMsgs(Set<String> msgs);

  void addErrorMsgs(Set<String> msgs);

  Status getStatus();

  Set<String> getPassMsgs();

  Set<String> getWarningMsgs();

  Set<String> getErrorMsgs();

}
