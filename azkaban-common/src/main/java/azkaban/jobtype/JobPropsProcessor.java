package azkaban.jobtype;

import azkaban.utils.Props;

/**
 * A pluggable job properties processor for jobtype plugins.
 */
public abstract class JobPropsProcessor {
  protected final Props pluginProps;

  public JobPropsProcessor(Props pluginProps) {
    this.pluginProps = pluginProps;
  }

  public abstract Props process(Props jobProps);
}
