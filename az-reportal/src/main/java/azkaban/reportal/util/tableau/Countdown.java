package azkaban.reportal.util.tableau;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Countdown is a class used by Tableau Job to
 * keep track of time as the Tableau extractions are
 * being refreshed.
 */
public class Countdown {

  private Duration duration;

  public Countdown(final Duration duration) {
    this.duration = duration;
  }

  public void waitForOneMinute() throws InterruptedException {
    TimeUnit.MINUTES.sleep(1);
    this.duration = this.duration.minusMinutes(1);
  }

  public boolean moreTimeRemaining() {
    return this.duration.toMillis() > 0;
  }
}
