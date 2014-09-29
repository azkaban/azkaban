package azkaban.project.validator;

import java.util.HashSet;
import java.util.Set;

public class ValidationReport {

  protected Status _status;
  protected Set<String> _passMsgs;
  protected Set<String> _warningMsgs;
  protected Set<String> _errorMsgs;

  public ValidationReport() {
    _status = Status.PASS;
    _passMsgs = new HashSet<String>();
    _warningMsgs = new HashSet<String>();
    _errorMsgs = new HashSet<String>();
  }

  public void addPassMsgs(Set<String> msgs) {
    _passMsgs.addAll(msgs);
  }

  public void addWarningMsgs(Set<String> msgs) {
    _warningMsgs.addAll(msgs);
    if (!msgs.isEmpty() && _errorMsgs.isEmpty()) {
      _status = Status.WARN;
    }
  }

  public void addErrorMsgs(Set<String> msgs) {
    _errorMsgs.addAll(msgs);
    if (!msgs.isEmpty()) {
      _status = Status.ERROR;
    }
  }

  public Status getStatus() {
    return _status;
  }

  public Set<String> getPassMsgs() {
    return _passMsgs;
  }

  public Set<String> getWarningMsgs() {
    return _warningMsgs;
  }

  public Set<String> getErrorMsgs() {
    return _errorMsgs;
  }

}
