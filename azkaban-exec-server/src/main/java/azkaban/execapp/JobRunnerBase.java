package azkaban.execapp;

import azkaban.event.EventHandler;
import azkaban.utils.Props;
import org.apache.log4j.Logger;

public class JobRunnerBase extends EventHandler {
  protected final Props props;
  protected Logger logger = Logger.getLogger(JobRunnerBase.class);

  public JobRunnerBase(Props props) {
    this.props = props;
  }

  public Props getProps() {
    return this.props;
  }

  public Logger getLogger() {
    return this.logger;
  }
}
