package azkaban.reportal.util.tableau;

import java.util.concurrent.TimeUnit;

/**
 * Countdown is a class used by Tableau Job to
 * keep track of time as the Tableau extractions are
 * being refreshed.
 */
public class Countdown {

  private long _timeRemaining;

  public Countdown(final long startingTime) {
    this._timeRemaining = startingTime;
  }

  public void waitForOneMinute() throws InterruptedException {
    TimeUnit.MINUTES.sleep(1);
    this._timeRemaining--;
  }

  public boolean moreTimeRemaining() {
    return (this._timeRemaining > 0);
  }
}
