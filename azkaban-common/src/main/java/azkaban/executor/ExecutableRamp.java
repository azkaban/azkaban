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

import azkaban.utils.TimeUtils;
import com.sun.istack.NotNull;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Object of Executable Ramp
 */
public final class ExecutableRamp implements IRefreshable<ExecutableRamp> {
  private static final int ONE_DAY = 60 * 60 * 24;
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutableRamp.class);
  private ReentrantLock lock = new ReentrantLock();

  public enum Action {
    IGNORED, SUCCEEDED, FAILED
  }

  public enum CountType {
    TRAIL, SUCCESS, FAILURE, IGNORED
  }

  private static class State implements IRefreshable<State> {
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
    private volatile long lastRampDownTime = 0;

    private volatile int cachedNumOfTrail = 0;
    private volatile int cachedNumOfSuccess = 0;
    private volatile int cachedNumOfFailure = 0;
    private volatile int cachedNumOfIgnored = 0;

    private volatile boolean isActive = true;

    private State() {

    }

    private State(long startTime, long endTime, long lastUpdatedTime,
        int numTrail, int numSuccess, int numFailure, int numIgnored,
        boolean isPaused, int rampStage, boolean isActive) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.lastUpdatedTime = lastUpdatedTime;
      this.numOfTrail = numTrail;
      this.numOfSuccess = numSuccess;
      this.numOfFailure = numFailure;
      this.numOfIgnored = numIgnored;
      this.isPaused = isPaused;
      this.rampStage = rampStage;
      this.isActive = isActive;
    }

    public static final State createInstance(long startTime, long endTime, long lastUpdatedTime,
        int numTrail, int numSuccess, int numFailure, int numIgnored,
        boolean isPaused, int rampStage, boolean isActive) {
      return new State(startTime, endTime, lastUpdatedTime,
          numTrail, numSuccess, numFailure, numIgnored,
          isPaused, rampStage, isActive);
    }

    @Override
    public State refresh(State source) {
      this.startTime = source.startTime;
      this.endTime = source.endTime;
      this.lastUpdatedTime = source.lastUpdatedTime;

      this.numOfTrail = source.numOfTrail;
      this.numOfSuccess = source.numOfSuccess;
      this.numOfFailure = source.numOfFailure;
      this.numOfIgnored = source.numOfIgnored;

      this.isPaused = source.isPaused;
      if (source.rampStage > this.rampStage) {
        this.lastRampDownTime = 0;
      }
      this.rampStage = source.rampStage;
      this.isActive = source.isActive;

      this.isSynchronized = (this.cachedNumOfFailure == 0)
          && (this.cachedNumOfIgnored == 0)
          && (this.cachedNumOfSuccess == 0)
          && (this.cachedNumOfTrail == 0);

      return this;
    }

    @Override
    public State clone() {
      State cloned = new State();
      cloned.isSynchronized = this.isSynchronized;
      cloned.startTime = this.startTime;
      cloned.endTime = this.endTime;
      cloned.lastUpdatedTime = this.lastUpdatedTime;
      cloned.numOfTrail = this.numOfTrail;
      cloned.numOfSuccess = this.numOfSuccess;
      cloned.numOfFailure = this.numOfFailure;
      cloned.numOfIgnored = this.numOfIgnored;
      cloned.isPaused = this.isPaused;
      cloned.rampStage = this.rampStage;
      cloned.lastRampDownTime = this.lastRampDownTime;
      cloned.cachedNumOfTrail = this.cachedNumOfTrail;
      cloned.cachedNumOfSuccess = this.cachedNumOfSuccess;
      cloned.cachedNumOfFailure = this.cachedNumOfFailure;
      cloned.cachedNumOfIgnored = this.cachedNumOfIgnored;
      cloned.isActive = true;
      return cloned;
    }

    @Override
    public int elementCount() {
      return 1;
    }
  }

  private static class Metadata implements IRefreshable<Metadata> {

    private volatile int maxFailureToPause = 0;
    private volatile int maxFailureToRampDown = 0;
    private volatile boolean isPercentageScaleForMaxFailure = false;

    private Metadata() {

    }

    private Metadata(int maxFailureToRampPause, int maxFailureToRampDown, boolean isPercentageScaleForMaxFailure) {
      this.maxFailureToPause = maxFailureToRampPause;
      this.maxFailureToRampDown = maxFailureToRampDown;
      this.isPercentageScaleForMaxFailure = isPercentageScaleForMaxFailure;
    }

    public static Metadata createInstance(int maxFailureToRampPause,
        int maxFailureToRampDown, boolean isPercentageScaleForMaxFailure) {
      return new Metadata(maxFailureToRampPause, maxFailureToRampDown, isPercentageScaleForMaxFailure);
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
      return new Metadata(this.maxFailureToPause, this.maxFailureToRampDown, this.isPercentageScaleForMaxFailure);
    }

    @Override
    public int elementCount() {
      return 1;
    }
  }

  private volatile String id;
  private volatile String policy;
  private volatile Metadata metadata;
  private volatile State state;

  private ExecutableRamp() {

  }

  private ExecutableRamp(@NotNull final String id, @NotNull final String policy,
      @NotNull ExecutableRamp.Metadata metadata, @NotNull ExecutableRamp.State state) {
    this.id = id;
    this.policy = policy;
    this.metadata = metadata;
    this.state = state;
  }

  public static ExecutableRamp createInstance(@NotNull final String id, @NotNull final String policy,
      int maxFailureToRampPause, int maxFailureToRampDown, boolean isPercentageScaleForMaxFailure,
      long startTime, long endTime, long lastUpdatedTime,
      int numTrail, int numSuccess, int numFailure, int numIgnored,
      boolean isPaused, int rampStage, boolean isActive) {
    return new ExecutableRamp(id, policy,
        ExecutableRamp.Metadata.createInstance(
            maxFailureToRampPause, maxFailureToRampDown, isPercentageScaleForMaxFailure),
        ExecutableRamp.State.createInstance(
            startTime, endTime, lastUpdatedTime,
            numTrail, numSuccess, numFailure, numIgnored,
            isPaused, rampStage, isActive)
        );
  }

  public String getId() {
    return id;
  }

  public String getPolicy() {
    return policy;
  }

  public boolean isActive() {
    long diff = this.state.startTime - System.currentTimeMillis();
    boolean isActive = this.state.isActive && (!this.state.isPaused) && (diff <= 0);
    if (!isActive) {
      LOGGER.info("[Ramp Is Isolated] (isActive = {}, isPause = {}, timeDiff = {}",
          this.state.isActive, this.state.isPaused, diff);
    }
    return isActive;
  }

  public boolean isChanged() {
    return !this.state.isSynchronized;
  }

  public boolean isPaused() {
    return this.state.isPaused;
  }

  public boolean isNotTestable() {
    return (!this.state.isActive || this.state.isPaused || (this.state.rampStage <= 0));
  }

  public int getStage() {
    return this.state.rampStage;
  }

  public long getStartTime() {
    return this.state.startTime;
  }

  public long getEndTime() {
    return this.state.endTime;
  }

  public long getLastUpdatedTime() {
    return this.state.lastUpdatedTime;
  }

  public int getMaxFailureToRampDown() {
    return this.metadata.maxFailureToRampDown;
  }

  public int getMaxFailureToPause() {
    return this.metadata.maxFailureToPause;
  }

  public boolean isPercentageScaleForMaxFailure() {
    return this.metadata.isPercentageScaleForMaxFailure;
  }

  public int getCount(@NotNull CountType countType) {
    return getCount(countType, false);
  }

  public int getCachedCount(@NotNull CountType countType) {
    return getCount(countType, true);
  }

  private int getCount(@NotNull CountType countType, boolean isCached) {
    int value = 0;
    switch (countType) {
      case TRAIL:
        value = isCached ? this.state.cachedNumOfTrail : this.state.numOfTrail;
        break;
      case SUCCESS:
        value = isCached ? this.state.cachedNumOfSuccess : this.state.numOfSuccess;
        break;
      case FAILURE:
        value = isCached ? this.state.cachedNumOfFailure : this.state.numOfFailure;
        break;
      default:
        value = isCached ? this.state.cachedNumOfIgnored : this.state.numOfIgnored;
        break;
    }
    return value;
  }

  synchronized public void rampUp(final @NotNull int maxStage) {
    lock.lock();
    try {
      int currentRampStage = this.state.rampStage;
      this.state.rampStage = currentRampStage + 1;
      this.state.lastUpdatedTime = System.currentTimeMillis();
      this.state.lastRampDownTime = 0;
      if (this.state.rampStage >= maxStage) {
        this.state.endTime = this.state.lastUpdatedTime;
      }
      this.state.isSynchronized = false;
    } finally {
      lock.unlock();
    }
  }

  synchronized public void rampDown() {
    lock.lock();
    try {
      int currentStage = this.state.rampStage;
      this.state.rampStage = currentStage - 1;
      this.state.lastRampDownTime = System.currentTimeMillis();
      this.state.isSynchronized = false;
    } finally {
      lock.unlock();
    }
  }

  synchronized public void cacheResult(Action action) {
    lock.lock();
    try {
      this.state.cachedNumOfTrail++;
      switch (action) {
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
      int trails = this.state.numOfTrail + this.state.cachedNumOfTrail;
      int fails = this.state.numOfFailure + this.state.cachedNumOfFailure;
      int failure =
          this.metadata.isPercentageScaleForMaxFailure ? (trails == 0) ? 100 : (int) ((fails * 100.0) / (trails * 1.0))
              : fails;

      LOGGER.info(
          "[Ramp Cached Result] (id = {}, action: {}, {} failure: {}, numOfTrail ({}, {}), numOfSuccess: ({}, {}), numOfFailure: ({}, {}), numOfIgnore: ({}, {}))",
          this.id, action.name(), this.metadata.isPercentageScaleForMaxFailure ? "Percentage" : " ", failure, this.state.numOfTrail, this.state.cachedNumOfTrail, this.state.numOfSuccess, this.state.cachedNumOfSuccess,
          this.state.numOfFailure, this.state.cachedNumOfFailure, this.state.numOfIgnored, this.state.cachedNumOfIgnored);

      if (this.metadata.maxFailureToRampDown != 0) {
        if (failure > this.metadata.maxFailureToRampDown) {
          if (this.state.rampStage > 0) {
            if (TimeUtils.timeEscapedOver(this.state.lastRampDownTime, ONE_DAY)) {
              int currentStage = this.state.rampStage;
              this.rampDown();
              LOGGER.warn("[RAMP DOWN] (rampId = {}, failure = {}, threshold = {}, from stage {} to stage {}.)", this.getId(), failure, this.metadata.maxFailureToRampDown, currentStage, this.state.rampStage);
            }
          }
        }
      }

      if (this.metadata.maxFailureToPause != 0) {
        if (failure > this.metadata.maxFailureToPause) {
          this.state.isPaused = true;
          LOGGER.info("[RAMP STOP] (rampId = {}, failure = {}, threshold = {}, timestamp = {})", this.getId(), failure,
              this.metadata.maxFailureToPause, System.currentTimeMillis());
        }
      }

      this.state.isSynchronized = false;
    } finally {
      lock.unlock();
    }
  }

  synchronized public void cacheSaved() {
    lock.lock();
    try {
      int ttlTrail = this.state.numOfTrail + this.state.cachedNumOfTrail;
      int ttlSuccess = this.state.numOfSuccess + this.state.cachedNumOfSuccess;
      int ttlFailure = this.state.numOfFailure + this.state.cachedNumOfFailure;
      int ttlIgnored = this.state.numOfIgnored + this.state.cachedNumOfIgnored;
      this.state.numOfTrail = ttlTrail;
      this.state.numOfSuccess = ttlSuccess;
      this.state.numOfFailure = ttlFailure;
      this.state.numOfIgnored = ttlIgnored;
      this.state.cachedNumOfTrail = 0;
      this.state.cachedNumOfSuccess = 0;
      this.state.cachedNumOfFailure = 0;
      this.state.cachedNumOfIgnored = 0;
      this.state.isSynchronized = true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public ExecutableRamp refresh(ExecutableRamp source) {
    lock.lock();
    try {
      if (source.getId().equalsIgnoreCase(this.id)) {
        this.policy = source.policy;
        this.state.refresh(source.state);
        this.metadata.refresh(source.metadata);
      }
    } finally {
      lock.unlock();
    }
    return this;
  }

  @Override
  public ExecutableRamp clone() {
    return new ExecutableRamp(this.id, this.policy, this.metadata.clone(), this.state.clone());
  }

  @Override
  public int elementCount() {
    return 1;
  }
}
