/*
 * Copyright 2019 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.executor;

import com.sun.istack.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Object of Executable Ramp
 */
public class ExecutableRamp implements IRefreshable<ExecutableRamp> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutableRamp.class);

  public enum Action {
    IGNORED, SUCCEEDED, FAILED
  }

  public static class State implements IRefreshable<State> {
    private volatile boolean isSynchronized = true;

    private volatile long startTime = 0;
    private volatile long endTime = 0;
    private volatile long lastUpdatedTime = 0;
    private volatile int numOfTrail = 0;
    private volatile int numOfSuccess = 0;
    private volatile int numOfFailure = 0;
    private volatile int numOfIgnored = 0;
    private volatile boolean isPaused = false;
    private volatile int rampStage = 0;

    private volatile int cachedNumOfTrail = 0;
    private volatile int cachedNumOfSuccess = 0;
    private volatile int cachedNumOfFailure = 0;
    private volatile int cachedNumOfIgnored = 0;

    private volatile boolean isActive = true;

    public static final State createInstance(long startTime, long endTime, long lastUpdatedTime,
        int numTrail, int numSuccess, int numFailure, int numIgnored,
        boolean isPaused, int rampStage, boolean isActive) {
      return new State()
          .setStartTime(startTime)
          .setEndTime(endTime)
          .setLastUpdatedTime(lastUpdatedTime)
          .setNumOfTrail(numTrail)
          .setNumOfSuccess(numSuccess)
          .setNumOfFailure(numFailure)
          .setNumOfIgnored(numIgnored)
          .setPaused(isPaused)
          .setRampStage(rampStage)
          .setActive(isActive);
    }

    public long getStartTime() {
      return startTime;
    }

    public State setStartTime(long startTime) {
      this.startTime = startTime;
      return this;
    }

    public long getEndTime() {
      return endTime;
    }

    public State setEndTime(long endTime) {
      this.endTime = endTime;
      return this;
    }

    public long getLastUpdatedTime() {
      return lastUpdatedTime;
    }

    public State setLastUpdatedTime(long lastUpdatedTime) {
      this.lastUpdatedTime = lastUpdatedTime;
      return this;
    }

    public int getNumOfTrail() {
      return numOfTrail;
    }

    public int getCachedNumOfTrail() {
      return this.cachedNumOfTrail;
    }

    public State setNumOfTrail(int numOfTrail) {
      this.numOfTrail = numOfTrail;
      return this;
    }

    public int getNumOfSuccess() {
      return numOfSuccess;
    }

    public int getCachedNumOfSuccess() {
      return this.cachedNumOfSuccess;
    }

    public State setNumOfSuccess(int numOfSuccess) {
      this.numOfSuccess = numOfSuccess;
      return this;
    }

    public int getNumOfFailure() {
      return numOfFailure;
    }

    public int getCachedNumOfFailure() {
      return this.cachedNumOfFailure;
    }

    public State setNumOfFailure(int numOfFailure) {
      this.numOfFailure = numOfFailure;
      return this;
    }

    public int getNumOfIgnored() {
      return numOfIgnored;
    }

    public int getCachedNumOfIgnored() {
      return this.cachedNumOfIgnored;
    }

    public State setNumOfIgnored(int numOfIgnored) {
      this.numOfIgnored = numOfIgnored;
      return this;
    }

    public boolean isPaused() {
      return isPaused;
    }

    public State setPaused(boolean paused) {
      this.isPaused = paused;
      return this;
    }

    public int getRampStage() {
      return rampStage;
    }

    public boolean isRamping() {
      return (rampStage >= 0);
    }

    public State setRampStage(int rampStage) {
      this.rampStage = rampStage;
      return this;
    }

    public boolean isActive() {
      return isActive;
    }

    public State setActive(boolean active) {
      this.isActive = active;
      return this;
    }

    public boolean isSynchronized() {
      return isSynchronized;
    }

    private State markChanged() {
      this.isSynchronized = false;
      return this;
    }

    public State markDataSaved() {
      int aggNumOfTrail = this.numOfTrail + this.cachedNumOfTrail;
      int aggNumOfSuccess = this.numOfSuccess + this.cachedNumOfSuccess;
      int aggNumOfFailure = this.numOfFailure + this.cachedNumOfFailure;
      int aggNumOfIgnored = this.numOfIgnored + this.cachedNumOfIgnored;
      setNumOfTrail(aggNumOfTrail);
      setNumOfSuccess(aggNumOfSuccess);
      setNumOfFailure(aggNumOfFailure);
      setNumOfIgnored(aggNumOfIgnored);
      this.cachedNumOfTrail = 0;
      this.cachedNumOfSuccess = 0;
      this.cachedNumOfFailure = 0;
      this.cachedNumOfIgnored = 0;
      this.isSynchronized = true;
      return this;
    }

    @Override
    public State refresh(State source) {

      this.startTime = source.startTime;
      this.endTime = (source.endTime > this.endTime) ? source.endTime : this.endTime;
      this.lastUpdatedTime = (source.lastUpdatedTime > this.lastUpdatedTime) ? source.lastUpdatedTime : this.lastUpdatedTime;

      this.numOfTrail = source.numOfTrail;
      this.numOfSuccess = source.numOfSuccess;
      this.numOfFailure = source.numOfFailure;
      this.numOfIgnored = source.numOfFailure;

      this.isPaused = source.isPaused ? source.isPaused : this.isPaused;
      this.rampStage = (source.rampStage > this.rampStage) ? source.rampStage : this.rampStage;
      this.isActive = source.isActive;

      this.isSynchronized = true;
      return this;
    }

    @Override
    public State clone() {
      return null;
    }
  }

  public static class Metadata implements IRefreshable<Metadata> {

    private volatile int maxFailureToPause = 0;
    private volatile int maxFailureToRampDown = 0;
    private volatile boolean isPercentageScaleForMaxFailure = false;

    public Metadata() {
    }

    public static Metadata createInstance(int maxFailureToRampPause,
        int maxFailureToRampDown, boolean isPercentageScaleForMaxFailure) {
      return new Metadata()
          .setMaxFailureToPause(maxFailureToRampPause)
          .setMaxFailureToRampDown(maxFailureToRampDown)
          .setPercentageScaleForMaxFailure(isPercentageScaleForMaxFailure);
    }

    public int getMaxFailureToPause() {
      return maxFailureToPause;
    }

    public Metadata setMaxFailureToPause(int maxFailureToPause) {
      this.maxFailureToPause = maxFailureToPause;
      return this;
    }

    public int getMaxFailureToRampDown() {
      return maxFailureToRampDown;
    }

    public Metadata setMaxFailureToRampDown(int maxFailureToRampDown) {
      this.maxFailureToRampDown = maxFailureToRampDown;
      return this;
    }

    public boolean isPercentageScaleForMaxFailure() {
      return isPercentageScaleForMaxFailure;
    }

    public Metadata setPercentageScaleForMaxFailure(boolean percentageScaleForMaxFailure) {
      isPercentageScaleForMaxFailure = percentageScaleForMaxFailure;
      return this;
    }

    @Override
    public Metadata refresh(Metadata source) {

      this.maxFailureToPause = source.maxFailureToPause;
      this.maxFailureToRampDown = source.maxFailureToRampDown;
      this.isPercentageScaleForMaxFailure = source.isPercentageScaleForMaxFailure;
      return this;
    }

    @Override
    public Metadata clone() {
      return null;
    }
  }

  private volatile String id;
  private volatile String policy;
  private volatile Metadata metadata;
  private volatile State state;

  public ExecutableRamp() {

  }

  public static ExecutableRamp createInstance(@NotNull final String id, @NotNull final String policy,
      Metadata metadata, State state) {
    return new ExecutableRamp()
        .setId(id)
        .setPolicy(policy)
        .setMetadata(metadata)
        .setState(state);
  }

  public String getId() {
    return id;
  }

  public ExecutableRamp setId(String id) {
    this.id = id;
    return this;
  }

  public String getPolicy() {
    return policy;
  }

  public ExecutableRamp setPolicy(String policy) {
    this.policy = policy;
    return this;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public ExecutableRamp setMetadata(Metadata metadata) {
    this.metadata = metadata;
    return this;
  }

  public State getState() {
    return this.state;
  }

  public ExecutableRamp setState(State state) {
    this.state = state;
    return this;
  }

  public boolean isActive() {
    long diff = this.getState().startTime - System.currentTimeMillis();
    return this.getState().isActive && (!this.getState().isPaused) && (diff < 0);
  }

  synchronized public void cacheResult(Action action) {
    this.state.cachedNumOfTrail++;
    switch(action) {
      case SUCCEEDED:
        this.state.cachedNumOfSuccess++;
        break;
      case FAILED:
        this.state.cachedNumOfFailure++;
        break;
      default:
        this.state.cachedNumOfIgnored++;
        break;
    }
    this.state.lastUpdatedTime = System.currentTimeMillis();

    // verify the failure threshold
    int failure = this.metadata.isPercentageScaleForMaxFailure
        ?
        (int) (((this.state.numOfFailure + this.state.cachedNumOfFailure) * 100.0)
            / ((this.state.numOfTrail + this.state.cachedNumOfTrail) * 1.0))
        : (this.state.numOfFailure + this.state.cachedNumOfFailure);

    LOGGER.info(String.format("Cache Ramp Result : [id = %s, action: %s, %s failure: %d, numOfTrail (%d, %d), numOfSuccess: (%d, %d), numOfFailure: (%d, %d), numOfIgnore: (%d, %d)]"
        , this.id
        , action.name()
        , this.metadata.isPercentageScaleForMaxFailure ? "Percentage" : " "
        , failure
        , this.state.numOfTrail
        , this.state.cachedNumOfTrail
        , this.state.numOfSuccess
        , this.state.cachedNumOfSuccess
        , this.state.numOfFailure
        , this.state.cachedNumOfFailure
        , this.state.numOfIgnored
        , this.state.cachedNumOfIgnored
    ));
    if (failure > this.metadata.maxFailureToRampDown) {
      LOGGER.warn(String.format("Failure over the threshold to Ramp Down [id = %s, failure = %d, threshold = %d]", this.id, failure, this.metadata.maxFailureToRampDown));
      if (this.state.rampStage > 0) {
        this.state.rampStage--;
      }
    } else if (failure > this.metadata.maxFailureToPause) {
      LOGGER.warn(String.format("Failure over the threshold to Pause the Ramp [id = %s, failure = %d, threshold = %d]", this.id, failure, this.metadata.maxFailureToRampDown));
    }

    this.getState().markChanged();
  }

  synchronized public void cacheSaved() {
    this.getState().markDataSaved();
  }

  @Override
  public ExecutableRamp refresh(ExecutableRamp source) {
    if (source.getId().equalsIgnoreCase(this.id)) {
      this.policy = source.policy;
      this.getState().refresh(source.getState());
      this.getMetadata().refresh(source.getMetadata());
    }
    return this;
  }

  @Override
  public ExecutableRamp clone() {
    return null;
  }
}
