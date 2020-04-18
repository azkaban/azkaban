package azkaban.ramppolicy;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableRamp;
import azkaban.utils.Props;


/**
 * Define a simple ramp policy to ramp by percentage
 */
public class SimpleRampPolicy extends AbstractRampPolicy {
  private static final int MAX_RAMP_STAGE = 100;

  public SimpleRampPolicy(Props sysProps, Props privateProps) {
    super(sysProps, privateProps);
  }

  @Override
  protected boolean isRampTestEnabled(ExecutableFlow flow, ExecutableRamp executableRamp) {
    int rampStage = executableRamp.getStage(); // scaled from 0 - 100 to represent the ramp percentage

    if (rampStage >= getMaxRampStage()) {
      return true;
    }

    return (getRampStage(flow) <= rampStage);
  }

  protected int getMaxRampStage() {
    return MAX_RAMP_STAGE;
  }

  protected int getRampStage(ExecutableFlow flow) {
    return flow.getRampPercentageId() + 1;
  }
}
