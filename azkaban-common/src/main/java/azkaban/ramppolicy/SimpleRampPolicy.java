package azkaban.ramppolicy;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableRamp;
import azkaban.utils.Props;


/**
 * Define a simple ramp policy to ramp by percentage
 */
public class SimpleRampPolicy extends AbstractRampPolicy {
  protected SimpleRampPolicy(Props sysProps, Props privateProps) {
    super(sysProps, privateProps);
  }

  @Override
  protected boolean isRampTestEnabled(ExecutableFlow flow, ExecutableRamp executableRamp) {
    int rampStage = executableRamp.getState().getRampStage(); // scaled from 0 - 100 to represent the ramp percentage

    if (rampStage >= getMaxRampStage()) {
      return true;
    }

    return (getRampStage(flow) <= rampStage);
  }

  protected int getMaxRampStage() {
    return 100;
  }

  protected int getRampStage(ExecutableFlow flow) {
    return Math.abs(flow.getId().hashCode() % 100) + 1;
  }
}
