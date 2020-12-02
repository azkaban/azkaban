package azkaban.jobtype;

import azkaban.utils.Props;

/**
 * A test implementation of {@link JobPropsProcessor} that injects a
 * new property into jobs.
 */
public class TestJobPropsProcessor extends JobPropsProcessor {

  public static final String INJECTED_ADDITION_PROP = "ADDITION_PROP";

  public TestJobPropsProcessor(Props pluginProps) {
    super(pluginProps);
  }

  @Override
  public Props process(Props jobProps) {
    jobProps.put(INJECTED_ADDITION_PROP, INJECTED_ADDITION_PROP);
    return jobProps;
  }
}
